package org.apache.kafka.streams.state.internals

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.processor._
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl
import org.apache.kafka.streams.state.WindowStore
import org.apache.logging.log4j.scala.Logging
import snapshot.Snapshoter
import snapshot.tools.UploadS3ClientForStore
import software.amazon.awssdk.regions.Region
import utils.EitherOps.EitherOps

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.util.Try

object CoralogixStore extends Logging {

  object SnapshotStoreListeners {
    case class TppStore(topicPartition: TopicPartition, storeName: String)

    case object SnapshotStoreListener extends StateRestoreListener with StandbyUpdateListener {

      private var isEnabled = false;

      def enable(): Unit = {
        isEnabled = true
      }

      override def onUpdateStart(topicPartition: TopicPartition, storeName: String, startingOffset: Long): Unit = {
        standby.put(TppStore(topicPartition, storeName), true)
        onRestoreStart(topicPartition, storeName, startingOffset, 0)
      }

      override def onBatchLoaded(topicPartition: TopicPartition, storeName: String, taskId: TaskId, batchEndOffset: Long, batchSize: Long, currentEndOffset: Long): Unit = {
        onBatchRestored(topicPartition, storeName, batchEndOffset, batchSize)
      }

      override def onUpdateSuspended(topicPartition: TopicPartition, storeName: String, storeOffset: Long, currentEndOffset: Long, reason: StandbyUpdateListener.SuspendReason): Unit = {
        standby.put(TppStore(topicPartition, storeName), false)
        onRestoreSuspended(topicPartition, storeName, currentEndOffset)
      }

      val taskStore: ConcurrentHashMap[TppStore, Boolean] = new ConcurrentHashMap[TppStore, Boolean]()
      val standby: ConcurrentHashMap[TppStore, Boolean] = new ConcurrentHashMap[TppStore, Boolean]()

      val workingFlush: ConcurrentHashMap[TppStore, Boolean] = new ConcurrentHashMap[TppStore, Boolean]()

      override def onRestoreStart(topicPartition: TopicPartition, storeName: String, startingOffset: Long, endingOffset: Long): Unit = {
        println(Thread.currentThread() + "before restore topic" + topicPartition + " store " + storeName)
        taskStore.put(TppStore(topicPartition, storeName), true)
      }

      override def onBatchRestored(topicPartition: TopicPartition, storeName: String, batchEndOffset: Long, numRestored: Long): Unit = {

        println(Thread.currentThread() + "on ")
      }

      override def onRestoreEnd(topicPartition: TopicPartition, storeName: String, totalRestored: Long): Unit = {
        taskStore.put(TppStore(topicPartition, storeName), false)

        println(Thread.currentThread() + "on end")

      }

      override def onRestoreSuspended(topicPartition: TopicPartition, storeName: String, totalRestored: Long): Unit = {
        if (totalRestored <= 0)
          taskStore.put(TppStore(topicPartition, storeName), true)
        else {
          taskStore.put(TppStore(topicPartition, storeName), false)
        }
        super.onRestoreSuspended(topicPartition, storeName, totalRestored)
      }


    }
  }

  class CoralogixWindowStore(bytesStore: SegmentedBytesStore, retainDuplicates: Boolean, windowSize: Long) extends RocksDBWindowStore(bytesStore, retainDuplicates, windowSize) {

    var context: StateStoreContext = _
    var root: StateStore = _
    var snapshoter: Snapshoter[KeyValueSegment,RocksDBSegmentedBytesStore] = _
    var underlyingStore: RocksDBSegmentedBytesStore = _
    val snapshotStoreListener: SnapshotStoreListeners.SnapshotStoreListener.type = SnapshotStoreListeners.SnapshotStoreListener

    override def init(context: StateStoreContext, root: StateStore): Unit = {
      this.context = context
      this.root = root
      val s3ClientWrapper = UploadS3ClientForStore("cx-snapshot-test", "", Region.EU_NORTH_1, s"${context.applicationId()}/${context.taskId()}/${name()}")
      underlyingStore = this.wrapped.asInstanceOf[RocksDBSegmentedBytesStore]

      this.snapshoter = Snapshoter(
        snapshotStoreListener = snapshotStoreListener,
        s3ClientForStore = s3ClientWrapper,
        context = context.asInstanceOf[ProcessorContextImpl],
        storeName = name(),
        underlyingStore = underlyingStore,
        segmentFetcher = { store: RocksDBSegmentedBytesStore => store.getSegments.asScala.map(_.db).toList }
      )
      snapshoter.initFromSnapshot()
      Try {
        super.init(context, root)
      }.toEither.tapError { e =>
        logger.error(s"Error while initializing store: ${e.getMessage}")
      }

    }

    override def flush(): Unit = {
      super.flush()
      snapshoter.flushSnapshot()

    }

    override def close(): Unit = {
      super.close()
    }

  }

  class CoralogixTimestampedWindowStore(bytesStore: SegmentedBytesStore, retainDuplicates: Boolean, windowSize: Long) extends RocksDBTimestampedWindowStore(bytesStore, retainDuplicates, windowSize) {

    var context: StateStoreContext = _
    var root: StateStore = _
    var snapshoter: Snapshoter[TimestampedSegment,RocksDBTimestampedSegmentedBytesStore] = _
    var underlyingStore: RocksDBTimestampedSegmentedBytesStore = _
    val snapshotStoreListener: SnapshotStoreListeners.SnapshotStoreListener.type = SnapshotStoreListeners.SnapshotStoreListener

    override def init(context: StateStoreContext, root: StateStore): Unit = {
      this.context = context
      this.root = root
      val s3ClientWrapper = UploadS3ClientForStore("cx-snapshot-test", "", Region.EU_NORTH_1, s"${context.applicationId()}/${context.taskId()}/${name()}")
      underlyingStore = this.wrapped.asInstanceOf[RocksDBTimestampedSegmentedBytesStore]

      this.snapshoter = Snapshoter(
        snapshotStoreListener = snapshotStoreListener,
        s3ClientForStore = s3ClientWrapper,
        context = context.asInstanceOf[ProcessorContextImpl],
        storeName = name(),
        underlyingStore = underlyingStore,
        segmentFetcher = { store: RocksDBTimestampedSegmentedBytesStore => store.getSegments.asScala.map(_.db).toList }
      )
      snapshoter.initFromSnapshot()
      Try {
        super.init(context, root)
      }.toEither.tapError { e =>
        logger.error(s"Error while initializing store: ${e.getMessage}")
      }

    }

    override def flush(): Unit = {
      super.flush()
      snapshoter.flushSnapshot()

    }

    override def close(): Unit = {
      super.close()
    }

  }

  class WindowedSnapshotSupplier(name: String,
                                 retentionPeriod: Long,
                                 segmentInterval: Long,
                                 windowSize: Long,
                                 retainDuplicates: Boolean,
                                 returnTimestampedStore: Boolean,
                                ) extends RocksDbWindowBytesStoreSupplier(name, retentionPeriod, segmentInterval, windowSize, retainDuplicates, returnTimestampedStore) {


    private val windowStoreType = if (returnTimestampedStore) RocksDbWindowBytesStoreSupplier.WindowStoreTypes.TIMESTAMPED_WINDOW_STORE else RocksDbWindowBytesStoreSupplier.WindowStoreTypes.DEFAULT_WINDOW_STORE

    override def get(): WindowStore[Bytes, Array[Byte]] = {
      windowStoreType match {
        case RocksDbWindowBytesStoreSupplier.WindowStoreTypes.DEFAULT_WINDOW_STORE =>
          new CoralogixWindowStore(new RocksDBSegmentedBytesStore(name, metricsScope, retentionPeriod, segmentInterval, new WindowKeySchema), retainDuplicates, windowSize)
        case RocksDbWindowBytesStoreSupplier.WindowStoreTypes.TIMESTAMPED_WINDOW_STORE =>
            new CoralogixTimestampedWindowStore(new RocksDBTimestampedSegmentedBytesStore(name, metricsScope, retentionPeriod, segmentInterval, new WindowKeySchema), retainDuplicates, windowSize)

      }
    }
  }
}