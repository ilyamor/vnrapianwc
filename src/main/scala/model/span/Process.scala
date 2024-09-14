package model.span

import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

case class Process(
  serviceName: String,
  tags: List[KeyValue] = List.empty
) {
  def withFlatTags(): ProcessWithFlatTags = ProcessWithFlatTags(
    this.serviceName,
    this.tags.map { case KeyValue(key, value) =>
      key -> value
    }.toMap
  )
}
object Process {
  implicit val codec: JsonValueCodec[Process] = JsonCodecMaker.make
  implicit val circeEncoder: Encoder[Process] = deriveEncoder
}

case class ProcessWithFlatTags(serviceName: String, tags: Map[String, String])

private object ProcessWithFlatTags {
  implicit val codec: Encoder[ProcessWithFlatTags] = deriveEncoder
}
