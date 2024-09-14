package model.span

import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
import io.circe.syntax._
import model.span.Span.SpanWithFlatTags
import org.apache.kafka.common.serialization.Serde
import utils.JsonSerdeFromCodec

case class Span(
  traceID: String,
  spanID: String,
  operationName: String,
  startTime: Long,
  duration: Long,
  tags: List[KeyValue],
  process: Process,
  metadata: SpanMetadata
) {
  lazy val tagsMap: Map[String, String] = tags.map { case KeyValue(key, value) =>
    (key, value)
  }.toMap

  private lazy val processTagsMap: Map[String, String] = process.tags.map {
    case KeyValue(key, value) =>
      (key, value)
  }.toMap


  def getTag(tagName: String): Option[String] = tagsMap.get(tagName)
  def getProcessTag(tagName: String): Option[String] = processTagsMap.get(tagName)




  private def withFlatTags: SpanWithFlatTags =
    SpanWithFlatTags(
      this.traceID,
      this.spanID,
      this.operationName,
      this.process.serviceName,
      this.metadata.applicationName,
      this.metadata.subsystemName,
      this.startTime,
      this.duration,
      this.tagsMap,
      this.process.withFlatTags()
    )
}

object Span {
  implicit val codec: JsonValueCodec[Span] = JsonCodecMaker.make
  implicit val seqCodec: JsonValueCodec[Seq[Span]] = JsonCodecMaker.make
  implicit val circeEncoder: Encoder[Span] = deriveEncoder[Span]
  implicit val serde: Serde[Span] = JsonSerdeFromCodec(codec);
  implicit val serdes: Serde[Seq[Span]] = JsonSerdeFromCodec(seqCodec);

  // Wierd names like applicationname to match autocomplete values from schema store - so the lucene query will match
  // correctly
  case class SpanWithFlatTags(
    traceID: String,
    spanID: String,
    operationname: String,
    servicename: String,
    applicationname: String,
    subsystemname: String,
    startTime: Long,
    duration: Long,
    tags: Map[String, String],
    process: ProcessWithFlatTags
  )
  private object SpanWithFlatTags {
    implicit val codec: Encoder[SpanWithFlatTags] = deriveEncoder
  }
}
