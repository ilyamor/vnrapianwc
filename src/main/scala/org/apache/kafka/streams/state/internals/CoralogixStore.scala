package org.apache.kafka.streams.state.internals

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.processor.internals.ProcessorContextImpl
import org.apache.kafka.streams.processor.{StateStore, StateStoreContext}
import org.apache.kafka.streams.state.WindowStore
import org.apache.logging.log4j.scala.Logging
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, GetObjectResponse}
import tools.{Archiver, CheckPointCreator, S3ClientForStore}

import java.io.File
import java.nio.file.{Files, Path, StandardOpenOption}
import scala.util.Try

object CoralogixStore extends Logging {

  class S3ClientWrapper(bucketName: String) {

    val region = Region.EU_NORTH_1
    val s3: S3Client = S3Client.builder
      .region(region)
      .build


    val CHECKPOINT = ".checkpoint"
    val state = "state.tar.gz"

    def upload(): Unit = {
      println("uploading to s3")
    }

    def getCheckpointFile(partition: String, applicationId: String): Either[Throwable, String] = {

      //      val rootPath = s"$applicationId/$partition"
      //      val version = getLatestVersion(partition, applicationId)
      //      val checkpointPath = s"$rootPath/$version/$CHECKPOINT"

      val rootPath = s"$applicationId/$partition"
      val checkpointPath = s"$rootPath/$CHECKPOINT"


      Try {
        s3.getObject(GetObjectRequest.builder().bucket(bucketName).key(checkpointPath).build())
        //check if exists and read
      }.toEither.map(response => new String(response.readAllBytes()))

    }

    def getStateStores(partition: String, applicationId: String): Either[Throwable, ResponseInputStream[GetObjectResponse]] = {
      val rootPath = s"$applicationId/$partition"
      //      val version = getLatestVersion(partition, applicationId)
      val stateFileCompressed = s"$rootPath/$state"

      Try {
        s3.getObject(GetObjectRequest.builder().bucket(bucketName).key(stateFileCompressed).build())
        //check if exists and read
      }.toEither

    }

    private def getLatestVersion(partition: String, applicationId: String): String = {
      ""
    }
  }

  class CoralogixStore(bytesStore: SegmentedBytesStore, retainDuplicates: Boolean, windowSize: Long) extends RocksDBWindowStore(bytesStore, retainDuplicates, windowSize) {

    var context: StateStoreContext = _
    var root: StateStore =_

    val coreLogicS3Client = new S3ClientWrapper("cx-snapshot-test")



    override def flush(): Unit = {
      super.flush()
      val newContext = context.asInstanceOf[ ProcessorContextImpl]

      val stateDir = newContext.stateDir()
      val storeName = name();
      val topic = newContext.changelogFor(storeName)
      val partition = newContext.taskId.partition()
      val tp = new TopicPartition(topic, partition)
      val offset = newContext.recordCollector().offsets().get(tp)

      if (offset != null) {
        val storePath = s"${stateDir.getAbsolutePath}/$storeName"
        val tempDir = Files.createTempDirectory(s"$partition-$storeName")

        val files = for {
          a <- Archiver(tempDir.toFile, offset: Long, new File(storePath)).archive()
          c <- CheckPointCreator(tempDir.toFile, offset).create()
          u <- S3ClientForStore("", Region.EU_NORTH_1, storeName, partition, offset).uploadStateStore(a, c)
        } yield (a, c)

        println()
      }
    }

    override def init(context: StateStoreContext, root: StateStore): Unit = {
      this.context = context
      this.root = root
      val checkpoint = ".checkpoint"
      val stateDir = context.stateDir()
      val taskId = context.taskId()
      val checkpointFile = stateDir.toString + "/" + checkpoint
      val finalFile = new File(checkpointFile)
      if (finalFile.exists()) {
        val source = scala.io.Source.fromFile(checkpointFile)
        val localCheckPoint = try source.mkString finally source.close()
        //read file content
        coreLogicS3Client.getCheckpointFile(taskId.toString, context.applicationId())
          .map(remoteCheckpoint =>
            shouldFetchRemoteDb(localCheckPoint, remoteCheckpoint)).getOrElse(true)
        //handle errors
      }

//      coreLogicS3Client.getStateStores(taskId.toString, context.applicationId())
//        .foreach((response: ResponseInputStream[GetObjectResponse]) => {
//
//          val destDir = context.stateDir()
//          try {
//            val gzipInputStream = new GzipCompressorInputStream(response)
//            val tarInputStream = new TarArchiveInputStream(gzipInputStream)
//            try {
//              var entry: ArchiveEntry = null
//              do {
//                entry = tarInputStream.getNextEntry
//                if (entry != null) {
//                  val destPath = new File(destDir, entry.getName)
//                  if (entry.isDirectory) destPath.mkdirs // Create directories
//                  else {
//                    // Ensure parent directories exist
//                    destPath.getParentFile.mkdirs
//                    // Write file content
//                    try {
//                      val outputStream = new FileOutputStream(destPath)
//                      try {
//                        val buffer = new Array[Byte](1024)
//                        var bytesRead = 0
//                        do {
//                          bytesRead = tarInputStream.read(buffer)
//                          if (bytesRead != -1)
//                            outputStream.write(buffer, 0, bytesRead)
//                        } while (bytesRead != -1)
//
//                      } finally if (outputStream != null) outputStream.close()
//                    }
//                  }
//                }
//              }
//              while ((entry != null))
//            } finally {
//              if (response != null) response.close()
//              if (gzipInputStream != null) gzipInputStream.close()
//              if (tarInputStream != null) tarInputStream.close()
//            }
//          }
//
//        })


      super.init(context, root)


    }

    private def shouldFetchRemoteDb(localCheckPoint: String, remoteCheckpoint: String): Boolean = {
      true

    }


  }


  class WindowsCoralogixSupplier(name: String,
                                 retentionPeriod: Long,
                                 segmentInterval: Long,
                                 windowSize: Long,
                                 retainDuplicates: Boolean,
                                 returnTimestampedStore: Boolean) extends RocksDbWindowBytesStoreSupplier(name, retentionPeriod, segmentInterval, windowSize, retainDuplicates, returnTimestampedStore) {


    private val windowStoreType = if (returnTimestampedStore) RocksDbWindowBytesStoreSupplier.WindowStoreTypes.TIMESTAMPED_WINDOW_STORE else RocksDbWindowBytesStoreSupplier.WindowStoreTypes.DEFAULT_WINDOW_STORE

    override def get(): WindowStore[Bytes, Array[Byte]] = {
      windowStoreType match {
        case RocksDbWindowBytesStoreSupplier.WindowStoreTypes.DEFAULT_WINDOW_STORE =>
          new CoralogixStore(new RocksDBSegmentedBytesStore(name, metricsScope, retentionPeriod, segmentInterval, new WindowKeySchema), retainDuplicates, windowSize)

        case _ =>
          throw new IllegalArgumentException("invalid window store type: " + windowStoreType)
      }
    }
  }





}