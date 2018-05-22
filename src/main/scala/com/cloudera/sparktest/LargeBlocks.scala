package com.cloudera.sparktests

import java.util.concurrent.Executors

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.util.Random

import com.quantifind.sumac.{ArgMain, FieldArgs}

import org.apache.spark.SparkContext
import org.apache.spark.executor.ShuffleReadMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListenerBlockUpdated, SparkListener, SparkListenerTaskEnd}
import org.apache.spark.storage.{RDDBlockId, BlockUpdatedInfo, BlockId, StorageLevel}


/**
 * Simple Spark Application to test handling of large blocks, including:
 *
 * * Shuffle Blocks (remote and local)
 * * Broadcast Blocks
 * * Cached RDDs
 *     * Stored in-memory or on-disk
 *     * Network transfers for replication (push) and remote reads (pull)
 *
 * Sometimes the network transfers can fail, but the app succeeds anyway
 * because the data can be regenerated locally thanks to RDD lineage.  So this app includes extra
 * checks to make sure that spark was doing the right thing internally.  Sometimes this will really
 * require external verification by examining executor logs.
 */
object LargeBlocks extends ArgMain[LargeBlocksArgs] {

  override def main(args: LargeBlocksArgs): Unit = {
    val sc = new SparkContext()
    val appId = sc.applicationId
    try {
      if (args.doShuffle) {
        largeShuffleTest(sc, args)
      }
      if (args.doCache) {
        cacheTest(sc, args)
      }
      if (args.doBroadcast) {
        broadcastTest(sc, args)
      }
      // NOT included
      // * large task results
      // * large WAL blocks
      // * large records (> 2GB)
    } finally {
      println(s"AppId = $appId")
    }
  }

  def largeShuffleTest(sc: SparkContext, args: LargeBlocksArgs): Unit = {
    // Create shuffle blocks that are over 2GB, and test they can be read both with local reads
    // and remote reads.  Application confs should be set so that we only have one core per
    // executor, to ensure you're getting a non-local shuffle fetch.
    val shuffleMetrics = ArrayBuffer[ShuffleReadMetrics]()
    val listener = new SparkListener {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        if (taskEnd.taskMetrics != null) {
          shuffleMetrics += taskEnd.taskMetrics.shuffleReadMetrics
        }
      }
    }
    sc.addSparkListener(listener)
    val nKeys = 2
    sc.parallelize(1 to args.parts, args.parts).flatMap { x =>
      // we're *not* testing records that are over 2GB, just total block size.  So make a bunch of
      // modest-sized records, with the right total.  Random bytes to ensure that they aren't small
      // after compression
      val rng = new Random()
      val numRecords = 1e4.toInt
      val bytesPerRecord = (args.targetBlockSizeGb * nKeys * 1e9/ numRecords).toInt
      (0 to numRecords).iterator.map { idx =>
        val bytes = new Array[Byte](bytesPerRecord)
        rng.nextBytes(bytes)
        (idx % nKeys) -> bytes
      }
    }.groupByKey(args.parts).map { case (key, _) =>
      // Just to make sure we don't run both tasks on one executor, make sure the shuffle-fetch
      // tasks aren't too fast.
      Thread.sleep(args.taskSleepMillis)
      key
    }.collect()
    sc.stop()

    println("Local shuffle read sizes:")
    shuffleMetrics.foreach { metric =>
      println(s"${metric.localBytesRead} Bytes; ${metric.localBlocksFetched} blocks")
    }

    println("Remote shuffle read sizes:")
    shuffleMetrics.foreach { metric =>
      println(s"${metric.remoteBytesRead} Bytes; ${metric.remoteBlocksFetched} blocks")
    }
    assert(shuffleMetrics.exists { metric =>
      val avgRemoteReadBytes = metric.remoteBytesRead / metric.remoteBlocksFetched.toDouble
      avgRemoteReadBytes > args.targetBlockSizeGb * 0.9
    }, "No task had large avg remote shuffle fetch!")
  }

  def cacheTest(sc: SparkContext, args: LargeBlocksArgs): Unit = {
    // make an RDD with large partitions, but *not* large records and make sure:
    // * cache data is readable
    // * replication works
    // * remote reads work
    val blockListener = new BlockListener
    sc.addSparkListener(blockListener)
    val numRecords = 1e4.toInt
    val bytesPerRecord = (args.targetBlockSizeGb * 1e9/ numRecords).toInt
    val origData = sc.parallelize(1 to args.parts, args.parts)
      .mapPartitionsWithIndex { case (part, itr) =>
      val rng = new Random()
      rng.setSeed(part)
      (0 to numRecords).iterator.map { idx =>
        val bytes = new Array[Byte](bytesPerRecord)
        rng.nextBytes(bytes)
        bytes
      }
    }
    val storage = StorageLevel(
      useDisk = args.cacheOnDisk,
      useMemory = args.cacheInMem,
      useOffHeap = false,
      deserialized = false,
      replication = args.replicas)
    origData.persist(storage)

    origData.count()

    val partZeroBlocks = blockListener.blockIdToInfos(RDDBlockId(1,0))
    partZeroBlocks.map{ _.blockManagerId.executorId}.toSet
    assert(partZeroBlocks.size == args.replicas,
      s"expected ${args.replicas} replicas, got ${partZeroBlocks.size}")
    if (args.assertBlockSize) {
      partZeroBlocks.foreach { blockUpdate =>
        val minBlockSize = args.targetBlockSizeGb * 0.9 * 1e9
        if (args.cacheOnDisk) {
          assert(blockUpdate.diskSize > minBlockSize,
            s"Disk Size ${blockUpdate.diskSize} was smaller than expected")
        }
        if (args.cacheInMem) {
          assert(blockUpdate.memSize> minBlockSize,
            s"Disk Size ${blockUpdate.memSize} was smaller than expected")
        }
      }
    }

    // Verify the data is correct

    // By submitting concurrent read jobs, we ensure the data can't all be read locally.  That will
    // exercise remote fetches of the cached blocks.
    // unfortunately, even if remote reads fail, job might succeed anyway, by regenerating the data
    // or running the task locally after the task fails.  We have some weak checks, but need to
    // verify by looking at the logs.

    // To run this test w/ dynamic allocation enabled, you'd want to run some simple jobs here
    // with a lot of tasks to force a bunch of executors to spin up.

    val pool = Executors.newFixedThreadPool(args.concurrentReadJobs)
    val remoteReadListener = new RemoteReadListener
    sc.addSparkListener(remoteReadListener)
    val concurrentJobs = (0 until args.concurrentReadJobs).map { _ =>
      pool.submit(new Runnable() {
        override def run(): Unit = {
          Verify.verify(origData, bytesPerRecord, args.taskSleepMillis)
        }
      })
    }

    concurrentJobs.foreach { _.get()}

    // Check that we executed a task on an executor where data was not cached
    val execsWithCache = partZeroBlocks.map{ _.blockManagerId.executorId}.toSet
    val remoteReadLocations = remoteReadListener.executors(0).diff(execsWithCache)
    println(s"rdd cached on : ${execsWithCache}")
    println(s"tasks executed on: ${remoteReadListener.executors(0)}")
    println(s"remote reads on : ${remoteReadLocations}")
    val execsAndDriver = sc.getExecutorMemoryStatus.keySet
    if (args.replicas < args.concurrentReadJobs &&
        execsAndDriver.size > args.replicas + 1) {
      assert(remoteReadLocations.nonEmpty, "No partition was read remotely!")
    } else {
      println(s"WARNING: test is not configured to ensure remote read of cache data")
    }

    // externally, check executor logs to make sure:
    // 1) we *do* see: DEBUG storage.BlockManager: Getting remote block rdd_1_0
    // 2) we do *not* see: WARN storage.BlockManager: Failed to fetch remote block rdd_1_0
  }

  def broadcastTest(sc: SparkContext, args: LargeBlocksArgs): Unit = {
    // note that you need 2x the memory for this, as you need to store the broadcast var in
    // deserialized and serialized form (even though its just bytes!)
    val rng = new Random()
    rng.setSeed(42)
    val numChunks = 1e4.toInt
    val bytesPerChunk = (args.targetBlockSizeGb * 1e9/ numChunks).toInt
    val chunks = new Array[Array[Byte]](numChunks)
    (0 until numChunks).foreach { idx =>
      chunks(idx) = new Array[Byte](bytesPerChunk)
      rng.nextBytes(chunks(idx))
    }
    val bc = sc.broadcast(chunks)
    sc.parallelize(1 to args.parts).mapPartitions { _ =>
      val rng = new Random()
      rng.setSeed(42)
      val readBc = bc.value
      assert(readBc.length == numChunks)
      val exp = new Array[Byte](bytesPerChunk)
      (0 until numChunks).foreach { chunkIdx =>
        rng.nextBytes(exp)
        (0 until bytesPerChunk).foreach { idx =>
          assert(exp(idx) == readBc(chunkIdx)(idx))
        }
      }
      Iterator()
    }.count()
    bc.destroy()
  }
}

class BlockListener extends SparkListener {

  val blockIdToInfos =  HashMap[BlockId, ArrayBuffer[BlockUpdatedInfo]]()

  override def onBlockUpdated(blockUpdate: SparkListenerBlockUpdated): Unit = {
    val info = blockUpdate.blockUpdatedInfo
    val updates = blockIdToInfos.getOrElseUpdate(info.blockId, ArrayBuffer())
    updates += info
  }

}


class RemoteReadListener extends SparkListener {

  val hosts = HashMap[Int, HashSet[String]]()
  val executors = HashMap[Int, HashSet[String]]()

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val idx = taskEnd.taskInfo.index
    hosts.getOrElseUpdate(idx, HashSet[String]()) += taskEnd.taskInfo.host
    executors.getOrElseUpdate(idx, HashSet[String]()) += taskEnd.taskInfo.executorId
  }
}

class LargeBlocksArgs extends FieldArgs with Serializable {
  var baseLogDir = "app_logs"

  var doShuffle = false
  var doCache = false
  var doBroadcast = false

  var cacheOnDisk = false
  var cacheInMem = false
  var replicas = 1

  var concurrentReadJobs = 1

  var targetBlockSizeGb = 3.0
  var parts = 2

  var taskSleepMillis = 1000L

  var assertBlockSize = true

  addValidation {
    assert(doShuffle || doCache || doBroadcast,
      "Must specify at least one of doShuffle, doCache, or doBroadcast")
    if (doCache) {
      assert(cacheInMem || cacheOnDisk, "Must specify at least one of cacheInMem or cacheOnDisk")
    }
  }
}

object Verify {
  def verify(rdd: RDD[Array[Byte]], bytesPerRecord: Int, sleepTime: Long): Unit = {
    rdd.mapPartitionsWithIndex { case (part, itr) =>
      Thread.sleep(sleepTime)
      val rng = new Random()
      rng.setSeed(part)
      val exp = new Array[Byte](bytesPerRecord)
      itr.foreach { data =>
        rng.nextBytes(exp)
        (0 until bytesPerRecord).foreach { idx =>
          assert(exp(idx) == data(idx))
        }
      }
      Iterator()
    }.count()
  }
}
