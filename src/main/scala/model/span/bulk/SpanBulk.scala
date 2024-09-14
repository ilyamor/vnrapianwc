package model.span.bulk

import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import utils.JsonSerdeFromCodec

case class SpanBulk(spanData: SpanData, spanMetadata: SpanBulkMetadata)
object SpanBulk {
  implicit val codec: JsonValueCodec[SpanBulk] = JsonCodecMaker.make
  implicit val serde = JsonSerdeFromCodec(codec)
}
