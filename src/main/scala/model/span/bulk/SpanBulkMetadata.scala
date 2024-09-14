package model.span.bulk

import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker

case class CompanyId(id: Int)
object CompanyId {
  implicit val codec: JsonValueCodec[CompanyId] = JsonCodecMaker.make
}

case class SpanBulkMetadata(applicationName: String, subsystemName: String, companyId: CompanyId)
object SpanBulkMetadata {
  implicit val codec: JsonValueCodec[SpanBulkMetadata] = JsonCodecMaker.make
}
