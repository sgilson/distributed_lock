package sgilson.locker

import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.Watcher
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.ZooKeeper
import org.apache.zookeeper.data.Stat
import java.time.Duration
import java.util.concurrent.CountDownLatch

class DistributedLock(
    private val zookeeper: ZooKeeper,
    private val basePath: String,
    private val lockName: String,
    private val config: LockConfig = EPHEMERAL
) {
    private var acquiredLockPath: String? = null

    fun lock() {
        val (mode, ttl) = when (config) {
            PERSISTED -> Pair(CreateMode.PERSISTENT_SEQUENTIAL, -1L)
            EPHEMERAL -> Pair(CreateMode.EPHEMERAL_SEQUENTIAL, -1L)
            is TtlConfig -> Pair(CreateMode.PERSISTENT_SEQUENTIAL_WITH_TTL, config.duration.toMillis())
        }
        val lockPath =
            zookeeper.create(
                "$basePath/$lockName",
                null,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                mode, null as Stat?, ttl
            )
        while (true) {
            val latch = CountDownLatch(1)
            val nodes = zookeeper.getChildren(basePath) { latch.countDown() }.sorted()
            if (lockPath.substringAfterLast('/') == nodes.first()) {
                acquiredLockPath = lockPath
                return
            } else { latch.await() }
        }
    }

    fun unlock() {
        require(acquiredLockPath != null) { "Lock was never acquired" }

        zookeeper.delete(acquiredLockPath, -1)
        acquiredLockPath = null
    }
}

sealed class LockConfig
object PERSISTED : LockConfig()
object EPHEMERAL : LockConfig()
data class TtlConfig(val duration: Duration) : LockConfig()