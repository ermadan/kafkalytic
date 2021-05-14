package org.kafkalytic.plugin

import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.WatchedEvent
import org.apache.zookeeper.Watcher
import org.apache.zookeeper.ZooKeeper
import java.io.IOException
import java.io.PrintWriter
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

object ZkUtils {
    //    private val LOG = Logger.getInstance("zoolytic")
    private val zookeepers = HashMap<String, ZooKeeper>()

    @Throws(IOException::class, InterruptedException::class)
    fun getZk(source: String): ZooKeeper {
        val latch = CountDownLatch(1)
        var zk: ZooKeeper? = zookeepers[source]
        LOG.info("Found zk:" + zk)
        if (zk != null && zk.state != ZooKeeper.States.CONNECTED) {
            zk = null
        }
        if (zk == null) {
            LOG.info("Establishing connection....")
            zk = ZooKeeper(source, 1000) { event: WatchedEvent ->
                LOG.info("event:$event")
                if (event.state == Watcher.Event.KeeperState.SyncConnected) {
                    latch.countDown()
                }
            }
            if (latch.await(10, TimeUnit.SECONDS)) {
                zookeepers[source] = zk
                LOG.info("Connection established")
            } else {
                LOG.info("Connection timed out")
                throw IOException("Couldnt establish connection: " + source)
            }
        }
        return zk
    }

    fun getData(source: String, path: String) =
            try {
                getZk(source)?.getData(path, false, null)
            } catch (e: KeeperException) {
                LOG.info("KeeperException $e, retry")
                disconnect(source)
                getZk(source)?.getData(path, false, null)
            }

    fun disconnect(source: String) {
        getZk(source).close()
    }

    fun format(size: Int) =
            " (" + if (size > 1000) {
                if (size > 1000_000) {
                    (size / 1000_000).toString() + "M"
                } else {
                    (size / 1000).toString() + "K"
                }
            } else {
                size
            } + ")"

    fun count(zk: ZooKeeper, path: String, file: PrintWriter): Int {
        try {
            val data = zk.getData(path, false, zk.exists(path, false));
            file.println(path + format((data?.size ?: 0)))
            val kids = zk.getChildren(path, false).fold(0) { a, c -> a + count(zk, path + (if (path.endsWith("/")) "" else "/") + c, file) }
            val total = (data?.size ?: 0) + kids;
            if (kids > 0) {
                file.println(path + " total:" + format(total))
            }
            file.flush()
            return total;
        } catch (e: Exception) {
            e.printStackTrace();
            System.out.println(e);
        }
        return 0;
    }

}

