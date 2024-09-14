package model.span.bulk

import com.github.plokhotnyuk.jsoniter_scala.core.{JsonReader, JsonValueCodec, JsonWriter}

case class SpansWrapper(spansRaw: String)
object SpansWrapper {
  implicit val codec: JsonValueCodec[SpansWrapper] = new JsonValueCodec[SpansWrapper] {
    override def decodeValue(in: JsonReader, default: SpansWrapper): SpansWrapper =
      SpansWrapper(new String(in.readRawValAsBytes()))

    override def encodeValue(x: SpansWrapper, out: JsonWriter): Unit =
      out.writeRawVal(x.spansRaw.getBytes)

    override def nullValue: SpansWrapper = null
  }
}
