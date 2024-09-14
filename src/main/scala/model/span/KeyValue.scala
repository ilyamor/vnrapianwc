package model.span

import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

case class KeyValue(key: String, value: String)
object KeyValue {
  implicit val codec: JsonValueCodec[KeyValue] = JsonCodecMaker.make
  implicit val circeEncoder: Encoder[KeyValue] = deriveEncoder
}
