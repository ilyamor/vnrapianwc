package model.span.bulk

import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker

/*
  actually is Array[Span] but we dont want to parse the spans
  before parsing the bulk
 */
case class SpanData(spans: SpansWrapper)
object SpanData {
  implicit val codec: JsonValueCodec[SpanData] = JsonCodecMaker.make
}
