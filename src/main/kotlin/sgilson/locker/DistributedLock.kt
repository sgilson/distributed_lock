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
    var state: State = Unlocked
        private set(value) = synchronized(this) {
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
        try {
            state = Waiting(lockPath)
            handleState(state as Waiting)
        } catch (e: Exception) {
            state = ErrorRecovery(lockPath, e)
            handleState(state as ErrorRecovery)
            throw e
        }
    }

    private fun handleState(waiting: Waiting) {
        val path = waiting.pendingLockPath
        val name = path.substringAfterLast('/')
        while (true) {
            val latch = CountDownLatch(1)
            val nodes = zookeeper.getChildren(basePath) { latch.countDown() }.sorted()
            when (name) {
                nodes.first() -> {
                    state = Locked(path)
                    return
                }
                !in nodes -> {
                    state = Unlocked
                    throw IllegalStateException("Node was deleted during acquire: $path")
                }
                else -> latch.await()
            }
        }
    }

    private fun handleState(errorRecovery: ErrorRecovery) {
        try {
            if (zookeeper.state.isConnected &&
                zookeeper.exists(errorRecovery.lockPath, false) != null
            )
                zookeeper.delete(errorRecovery.lockPath, -1)
        } catch (e: Exception) {
            log.error(
                "Failed to delete node for lock during error recovery: {}",
                errorRecovery.lockPath,
                e
            )
        } finally {
            state = Unlocked
        }
    }

    fun unlock() = synchronized(this) {
        if (state is Locked) {
            val path = (state as Locked).lockPath
            try {
                zookeeper.delete(path, -1)
            } catch (e: Exception) {
                log.error("Failed to delete node for lock: {}", path, e)
                throw e
            } finally {
                state = Unlocked
            }
        } else {
            throw IllegalStateException("Lock was not acquired. State is $state")
        }
    }
}

sealed class State

object Unlocked : State() {
    override fun toString() = "Unlocked"
}

data class Locked(val lockPath: String) : State()

data class Waiting(val pendingLockPath: String) : State()

data class ErrorRecovery(val lockPath: String, val throwable: Throwable) : State()
