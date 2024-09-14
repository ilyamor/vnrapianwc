package model.span

import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

case class SpanMetadata(applicationName: String, subsystemName: String, companyId: Int)

object SpanMetadata {
  implicit val circeEncoder: Encoder[SpanMetadata] = deriveEncoder
}
