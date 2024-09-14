import com.github.plokhotnyuk.jsoniter_scala.core.readFromString
import model.span.Span
import model.span.bulk.SpanBulk
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.OpenContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.{CheckpointingOptions, Configuration}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.streams.StreamsConfig

import java.time.Duration
import java.util.Properties

object Main {


  //
  //  def stream(): StreamsBuilder = {
  //    //    val builder = new StreamsBuilder();
  //    //
  //    ////    val stream: KStream[String, Seq[Span]] = builder.stream[String, SpanBulk]("traces-gateway.spans")
  //    ////      .flatMap((_, bulk) => {
  //    ////        readFromString[Seq[Span]](bulk.spanData.spans.spansRaw)
  //    ////          .groupBy(_.traceID)
  //    ////          .toSeq
  //    ////      })
  //    ////
  //    ////    stream.groupByKey
  //    ////      .windowedBy(
  //    ////        SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(5))
  //    ////      ).inner.emitStrategy(EmitStrategy.onWindowClose()).aggregate(
  //    ////        Seq.empty[Span])((_, spans: Seq[Span], acc: Seq[Span]) => acc ++ spans, (_, span, acc) => acc ++ span)
  //    ////
  //    ////      .toStream.mapValues((k,v)=> {
  //    ////      val size = writeToArray(v).size
  //    ////      if (size > 5000000) {
  //    ////        println("traceId: " + k + " spans: " + v)
  //    ////        v.slice(0,100)
  //    ////      }
  //    ////      else {
  //    ////        v
  //    ////      }
  //    ////      }).map { case (key, value) => (key.key(), value) }.to("tracing-tco.aggregated-spans")
  //    ////    builder
  //    ////
  //    ////    builder.stream[String,Seq[Span]]("tracing-tco.aggregated-spans").groupByKey
  //    ////      .count().toStream.foreach((k: String, v: Long) => {
  //    ////      println("traceId: " + k + " size: " + v)
  //    ////        ()
  //    ////      })
  //
  //    //    builder
  //  }


  def main(args: Array[String]): Unit = {

    val config = new Configuration()
    config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem")
    config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///Users/ilya.m/IdeaProjects/trageAggregation/savepoints")
    val env = StreamExecutionEnvironment.createLocalEnvironment(1, config)

    val backend = new EmbeddedRocksDBStateBackend()
    backend.setDbStoragePath("/Users/ilya.m/IdeaProjects/trageAggregation/data")
    env.setStateBackend(backend)
    env.setDefaultSavepointDirectory("file:///Users/ilya.m/IdeaProjects/trageAggregation/savepoints")
    val source: KafkaSource[String] = KafkaSource.builder[String]().setBootstrapServers("localhost:9092")
      .setTopics("traces-gateway.spans")
      .setGroupId("aggregation-flink")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build();

    val processorS: KeyedProcessFunction[String, (String, Seq[Span]), (String, Int)] = new SpanAppender()
    val watermarkStrategy: WatermarkStrategy[(String, Seq[Span])] = WatermarkStrategy
      .forBoundedOutOfOrderness[(String, Seq[Span])](Duration.ofSeconds(20))
      .withTimestampAssigner(new SerializableTimestampAssigner[(String, Seq[Span])] {
        override def extractTimestamp(element: (String, Seq[Span]), recordTimestamp: Long): Long = {
          println("timestamp: " + recordTimestamp)
          recordTimestamp
        }
      })
    // get input data
    env.enableCheckpointing(1000)
    source
    //idle detection

    env.fromSource(source, WatermarkStrategy.forMonotonousTimestamps().withIdleness(Duration.ofSeconds(5)), "kafka")
      .flatMap { bulk =>
        val x = readFromString[SpanBulk](bulk)
        val spans = readFromString[Seq[Span]](x.spanData.spans.spansRaw)
        spans.groupBy(_.traceID).toSeq
      }
      .keyBy(span => span._1)
      .process(new SpanAppender())
      .setParallelism(10)
      .print()


    env.execute("grouper")

  }

  def prop() = {
    val properties = new Properties()

    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("application.id", "traceAggregation")
    properties.put("auto.offset.reset", "earliest")
    properties.put("max.poll.interval.ms", "4000")
    properties.put("session.timeout", "5000")
    properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "133554432")
    properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1")
    properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "100000000")
    properties
  }


}


class SpanAppender extends KeyedProcessFunction[String, (String, Seq[Span]), (String, Int)] {
  var spanState: ValueState[Int] = _
  var timerState: ValueState[Long] = _
  val timeout = 10000

  override def open(openContext: OpenContext): Unit = {
    val spanStateDescriptor = new ValueStateDescriptor[Int]("spanState", classOf[Int])
    spanState = getRuntimeContext.getState(spanStateDescriptor)
    val timerStateDescriptor = new ValueStateDescriptor[Long]("timerState", classOf[Long])
    timerState = getRuntimeContext.getState(timerStateDescriptor)
    super.open(openContext)
  }

  override def processElement(value: (String, Seq[Span]), ctx: KeyedProcessFunction[String, (String, Seq[Span]), (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {
    val currsize = spanState.value()
    spanState.update(currsize + value._2.size)
    val old = timerState.value()
    val coalescedTime = ((ctx.timestamp + timeout) / 1000) * 1000
    if (old == 0) {
      timerState.update(coalescedTime)
      ctx.timerService()
        .registerEventTimeTimer(coalescedTime)
    } else {
      if (old < coalescedTime) {
        println("thread" + Thread.currentThread() + "curr" + ctx.getCurrentKey + " " + currsize + " old time was" + old + "new time is" + coalescedTime + "watermark is" + ctx.timerService().currentWatermark())
        ctx.timerService().deleteEventTimeTimer(old)
        timerState.update(coalescedTime)
        ctx.timerService()
          .registerEventTimeTimer(coalescedTime)
      }
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Seq[Span]), (String, Int)]#OnTimerContext, out: Collector[(String, Int)]): Unit = {
    val spans = spanState.value();
    spanState.clear();
    timerState.clear();
    out.collect((ctx.getCurrentKey, spans))

  }


}




