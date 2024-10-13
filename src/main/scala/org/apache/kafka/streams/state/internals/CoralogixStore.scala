package org.apache.kafka.streams.state.internals

import io.confluent.examples.streams.SnapshotStoreListener.SnapshotStoreListener
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl
import org.apache.kafka.streams.processor.{StateStore, StateStoreContext}
import org.apache.kafka.streams.state.WindowStore
import org.apache.logging.log4j.scala.Logging
import snapshot.{S3ClientWrapper, Snapshoter}
import software.amazon.awssdk.regions.Region
import tools.UploadS3ClientForStore

object CoralogixStore extends Logging {




  class CoralogixStore(bytesStore: SegmentedBytesStore, retainDuplicates: Boolean, windowSize: Long, snapshotStoreListener: SnapshotStoreListener) extends RocksDBWindowStore(bytesStore, retainDuplicates, windowSize) {

    var context: StateStoreContext = _
    var root: StateStore = _
    var snapshoter: Snapshoter = _
    var s3ClientWrapper: UploadS3ClientForStore = _

    override def init(context: StateStoreContext, root: StateStore): Unit = {
      this.context = context
      this.root = root
      val coreLogicS3Client = new S3ClientWrapper("cx-snapshot-test")
      val s3ClientWrapper = UploadS3ClientForStore("cx-snapshot-test", "", Region.EU_NORTH_1, s"${context.applicationId()}/${context.taskId()}/${name()}")
      this.snapshoter = Snapshoter(
        s3ClientWrapper = coreLogicS3Client,
        snapshotStoreListener = snapshotStoreListener,
        s3ClientForStore = s3ClientWrapper,
        context = context.asInstanceOf[ProcessorContextImpl],
        storeName = name())
//      snapshoter.initFromSnapshot()
      super.init(context, root)
    }

    override def flush(): Unit = {
      super.flush()
      snapshoter.flushSnapshot()
    }


    override def close(): Unit = {
      super.close()
    }


  }

  class WindowsCoralogixSupplier(name: String,
                                 retentionPeriod: Long,
                                 segmentInterval: Long,
                                 windowSize: Long,
                                 retainDuplicates: Boolean,
                                 returnTimestampedStore: Boolean,
                                 snapshotStoreListener: SnapshotStoreListener
                                ) extends RocksDbWindowBytesStoreSupplier(name, retentionPeriod, segmentInterval, windowSize, retainDuplicates, returnTimestampedStore) {


    private val windowStoreType = if (returnTimestampedStore) RocksDbWindowBytesStoreSupplier.WindowStoreTypes.TIMESTAMPED_WINDOW_STORE else RocksDbWindowBytesStoreSupplier.WindowStoreTypes.DEFAULT_WINDOW_STORE

    override def get(): WindowStore[Bytes, Array[Byte]] = {
      windowStoreType match {
        case RocksDbWindowBytesStoreSupplier.WindowStoreTypes.DEFAULT_WINDOW_STORE =>
          new CoralogixStore(new RocksDBSegmentedBytesStore(name, metricsScope, retentionPeriod, segmentInterval, new WindowKeySchema), retainDuplicates, windowSize, snapshotStoreListener)

        case _ =>
          throw new IllegalArgumentException("invalid window store type: " + windowStoreType)
      }
    }
  }


}