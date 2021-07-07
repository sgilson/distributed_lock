package sgilson.locker

import java.util.concurrent.CountDownLatch
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.ZooKeeper
import org.slf4j.LoggerFactory

internal val log = LoggerFactory.getLogger(DistributedLock::class.java)

class DistributedLock(
    private val zookeeper: ZooKeeper,
    private val basePath: String,
    private val lockName: String
) {
    private var state: State = Unlocked
        set(value) = synchronized(this) {
            log.debug("State transition: {} -> {}", state, value)
            field = value
        }
        get() = synchronized(this) { field }

    fun lock() {
        if (state !is Unlocked)
            throw IllegalStateException("Lock is not unlocked. State is: $state")

        val mode = CreateMode.EPHEMERAL_SEQUENTIAL
        val acl = ZooDefs.Ids.OPEN_ACL_UNSAFE
        val lockPath = zookeeper.create("$basePath/$lockName", null, acl, mode)
        state = Waiting(lockPath)
        try {
            acquire(state as Waiting)
        } catch (e: Exception) {
            state = ErrorRecovery(lockPath, e)
            try {
                if (zookeeper.state.isConnected) {
                    zookeeper.delete(lockPath, -1)
                }
            } finally {
                state = Unlocked
            }
            throw e
        }
    }

    private fun acquire(waiting: Waiting) {
        val path = waiting.pendingLockPath
        while (true) {
            val latch = CountDownLatch(1)
            val nodes = zookeeper.getChildren(basePath) { latch.countDown() }.sorted()
            if (path.substringAfterLast('/') == nodes.first()) {
                state = Locked(path)
                return
            } else {
                latch.await()
            }
        }
    }

    fun unlock() = synchronized(this) {
        if (state is Locked) {
            val path = (state as Locked).lockPath
            zookeeper.delete(path, -1)
            state = Unlocked
        } else {
            throw IllegalStateException("Lock was not acquired. State is $state")
        }
    }

    fun isLocked() = state is Locked
}

sealed class State

object Unlocked : State() {
    override fun toString() = "Unlocked"
}

data class Locked(val lockPath: String) : State()

data class Waiting(val pendingLockPath: String) : State()

data class ErrorRecovery(val lockPath: String, val throwable: Throwable) : State()
