package sgilson.locker

import java.lang.Thread.sleep
import java.util.LinkedList
import java.util.concurrent.CompletableFuture.allOf
import java.util.concurrent.CompletableFuture.runAsync
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.absoluteValue
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.Watcher
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.ZooKeeper
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.junit.rules.Timeout
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.testcontainers.containers.GenericContainer

internal class DistributedLockTest {
    companion object {
        @get:ClassRule
        @JvmStatic
        val zookeeper =
            with(GenericContainer<Nothing>("zookeeper:3.7.0")) {
                portBindings = listOf("2181:2181")
                this
            }
    }

    @get:Rule
    val zk = ZookeeperRule()

    @get:Rule
    val timeout = Timeout(30, TimeUnit.SECONDS)

    @Test
    fun `if lock is used, no exceptions are thrown`() {
        val lock = zk.testLock()
        lock.lock()
        lock.unlock()
    }

    @Test
    fun `if lock is locked, other lock is denied`() {
        val (lock1, lock2) = zk.testLocks()
        lock1.lock()
        assertFailsWith<TimeoutException> {
            Executors.newSingleThreadExecutor().submit { lock2.lock() }.get(1, TimeUnit.SECONDS)
        }
    }

    @Test
    fun `when lock is released, waiting lock can continue`() {
        val (lock1, lock2) = zk.testLocks()
        lock1.lock()
        val latch = CountDownLatch(1)
        val future = runAsync {
            latch.countDown()
            lock2.lock()
        }
        latch.await()
        lock1.unlock()
        future.get(1, TimeUnit.SECONDS)
    }

    @Test
    fun `when many threads request lock, all eventually complete`() {
        val i = AtomicInteger()
        val futures =
            (0 until 50)
                .map {
                    runAsync {
                        val lock = zk.testLock()
                        lock.lock()
                        sleep(5)
                        lock.unlock()
                        i.incrementAndGet()
                    }
                }
                .toTypedArray()
        allOf(*futures)
            .whenComplete { _, _ -> assertEquals(50, i.get(), "All 50 threads completed") }
            .get()
    }

    @Test
    fun `lock can guarantee safe access to a variable`() {
        var i = 0
        val pool = Executors.newFixedThreadPool(10)
        // An unprotected int can expect to lose a few % of writes
        val futures =
            (0 until 1000)
                .map {
                    runAsync(
                        {
                            val lock = zk.testLock()
                            lock.lock()
                            i++
                            lock.unlock()
                        },
                        pool
                    )
                }
                .toTypedArray()
        allOf(*futures)
            .whenComplete { _, _ ->
                assertEquals(
                    1000,
                    i,
                    "Integer was protected from concurrency"
                )
            }
            .get()
    }

    @Test
    fun `if error occurs during acquire, other locks continue`() {
        val (lock1, lock2, lock3) = zk.testLocks()
        lock1.lock()

        val latch1 = CountDownLatch(1)
        val thread1 = Thread {
            latch1.countDown()
            lock2.lock()
        }
        thread1.start()
        sleep(1000)
        latch1.await() // ensure thread1 is running and that lock2 is probably pending

        thread1.interrupt()
        lock1.unlock()
        lock3.lock() // lock2 should delete itself and allow 3 to continue
    }

    @Test
    fun `is lock is already acquired, lock() raises`() {
        assertFails("Lock is already acquired") {
            val lock = zk.testLock()
            lock.lock()
            lock.lock()
        }
    }
}

class ZookeeperRule : TestRule {
    lateinit var zookeeper: ZooKeeper
        private set

    lateinit var nodePath: String
        private set

    override fun apply(base: Statement, description: Description): Statement {
        val latch = CountDownLatch(1)
        zookeeper =
            ZooKeeper("localhost", 30_000) {
                if (it.state == Watcher.Event.KeeperState.SyncConnected) latch.countDown()
                else error("Failed to connect: ${it.state}")
            }
        latch.await()

        nodePath =
            zookeeper.create(
                "/" + Random.nextLong().absoluteValue.toString(16),
                null,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT
            )
        return statement {
            try {
                base.evaluate()
            } finally {
                deleteFolder(nodePath)
                zookeeper.close()
            }
        }
    }

    fun deleteFolder(path: String) {
        val remaining = LinkedList<String>()
        val toDelete = LinkedList<String>()

        remaining.add(path)
        while (remaining.isNotEmpty()) {
            val node = remaining.removeFirst()
            toDelete.add(node)
            try {
                remaining.addAll(zookeeper.getChildren(node, false).map { "$node/$it" })
            } catch (nne: KeeperException.NoNodeException) {
            }
        }

        while (toDelete.isNotEmpty()) {
            val node = toDelete.removeLast()
            kotlin.runCatching { zookeeper.delete(node, -1) }
        }
    }

    fun testLock(): DistributedLock {
        return DistributedLock(zookeeper, nodePath, "lock")
    }

    fun testLocks() =
        object : HasComponents<DistributedLock> {
            override operator fun component1() = testLock()
        }
}

interface HasComponents<T> {
    operator fun component1(): T
    operator fun component2() = component1()
    operator fun component3() = component1()
    operator fun component4() = component1()
    operator fun component5() = component1()
}

inline fun statement(crossinline block: () -> Unit) =
    object : Statement() {
        override fun evaluate() {
            block.invoke()
        }
    }

fun DistributedLock.awaitLocked() {
    while (true) {
        if (this.isLocked()) return
        else Thread.yield()
    }
}
