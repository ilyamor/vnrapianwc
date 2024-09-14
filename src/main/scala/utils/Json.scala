import cats.data.{ Validated, ValidatedNel }
import cats.implicits._
import com.github.plokhotnyuk.jsoniter_scala.core.{ readFromArray, writeToArray, JsonValueCodec }
import io.circe.Encoder.encodeString
import io.circe.parser._
import io.circe.syntax.KeyOps
import io.circe.{ ACursor, Decoder, DecodingFailure, Encoder, HCursor, _ }
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream.internals.TimeWindow
import org.apache.kafka.streams.kstream.{ TimeWindowedDeserializer, TimeWindowedSerializer, Windowed }
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes.fromFn

import java.nio.charset.StandardCharsets

package object utils {
  implicit class ACursorOps(cursor: ACursor) {
    def asA[T: Decoder]: Decoder.AccumulatingResult[T] =
      Decoder[T].tryDecodeAccumulating(cursor)

    def getA[T: Decoder](field: String): Decoder.AccumulatingResult[T] =
      cursor.downField(field).asA[T]

    def getNested[T: Decoder](fields: String): Decoder.AccumulatingResult[T] =
      fields
        .split(":")
        .foldLeft(cursor)((cur, field) => cur.downField(field))
        .asA[T]
  }

  implicit class DecoderOps(d: Decoder.type) {
    def accumulating[T](decodeMethod: HCursor => ValidatedNel[DecodingFailure, T]): Decoder[T] =
      new Decoder[T] {
        def apply(cursor: HCursor): Either[DecodingFailure, T] =
          decodeMethod(cursor).toEither.leftMap(_.head)
        override def tryDecodeAccumulating(c: ACursor): ValidatedNel[DecodingFailure, T] = c match {
          case hc: HCursor => decodeMethod(hc)
          case _ =>
            Validated.invalidNel(
              DecodingFailure("Attempt to decode value on failed cursor", c.history)
            )
        }
      }
  }

  def decodeA[T: Decoder](data: String): ValidatedNel[Error, T] =
    parse(data)
      .leftWiden[Error]
      .toValidatedNel
      .andThen(json => Decoder[T].tryDecodeAccumulating(json.hcursor))

  class JsonSerializer[T](implicit circeEncoder: Encoder[T]) extends Serializer[T] {
    override def serialize(topic: String, data: T): Array[Byte] = {
      val json = circeEncoder(data)
      if (json.isNull) null
      else json.noSpaces.getBytes(StandardCharsets.UTF_8)
    }
  }

  class JsonDeserializer[T](implicit circeDecoder: Decoder[T]) extends Deserializer[T] {
    override def deserialize(topic: String, data: Array[Byte]): T =
      decode[T](new String(data, StandardCharsets.UTF_8)) match {
        case Left(err)    => throw err
        case Right(value) => value
      }
  }

  def JsonSerde[T >: Null](implicit circeEncoder: Encoder[T], circeDecoder: Decoder[T]): Serde[T] =
    Serdes.fromFn(
      data => {
        val json = circeEncoder(data)

        if (json.isNull) null
        else json.noSpaces.getBytes(StandardCharsets.UTF_8)
      },
      data =>
        if (data == null)
          None
        else
          decode[T](new String(data, StandardCharsets.UTF_8)) match {
            case Left(err)    => throw err
            case Right(value) => Some(value)
          }
    )

  def JsonSerdeOrString[T >: Null](
                                    implicit circeEncoder: Encoder[T],
                                    circeDecoder: Decoder[T]
                                  ): Serde[T] =
    Serdes.fromFn(
      data => {
        val json = circeEncoder(data)

        if (json.isNull) null
        else json.noSpaces.getBytes(StandardCharsets.UTF_8)
      },
      data =>
        if (data == null)
          None
        else {
          val json: String = (new String(data, StandardCharsets.UTF_8))
          decode[T](json).orElse(decode[T](Json.fromString(json).noSpaces)) match {
            case Left(err)    => throw err
            case Right(value) => Some(value)
          }
        }
    )

  def JsonSerdeFlat[T >: Null](
                                implicit circeEncoder: Encoder[T],
                                circeDecoder: Decoder[T]
                              ): Serde[T] =
    Serdes.fromFn(
      data => {
        val json = circeEncoder(data)

        if (json.isNull) null
        else json.asString.get.getBytes(StandardCharsets.UTF_8)
      },
      data =>
        if (data == null)
          None
        else {
          val json: String = (new String(data, StandardCharsets.UTF_8))
          decode[T](json).orElse(decode[T](Json.fromString(json).noSpaces)) match {
            case Left(err)    => throw err
            case Right(value) => Some(value)
          }
        }
    )

  val windowSerializer: TimeWindowedSerializer[String] =
    new TimeWindowedSerializer[String](new StringSerializer)
  val windowDeserializer: TimeWindowedDeserializer[String] =
    new TimeWindowedDeserializer[String](new StringDeserializer)

  implicit def windowedCodec: Codec[Windowed[String]] =
    Codec.from(
      Decoder.instance { cursor =>
        (cursor.get[Long]("start"), cursor.get[Long]("end"), cursor.get[String]("key")).mapN {
          case (start, end, key) =>
            new Windowed(key, new TimeWindow(start, end))
        }
      },
      Encoder.instance { windowedT =>
        Json.obj(
          "start" := windowedT.window.start(),
          "end"   := windowedT.window.end(),
          "key"   := windowedT.key
        )
      }
    )

  implicit val windowedSerde: Serde[Windowed[String]] = JsonSerde[Windowed[String]]
  implicit val setSerde: Serde[Set[String]] = JsonSerde[Set[String]]
  implicit val mapSerde: Serde[Map[String, Long]] = JsonSerde[Map[String, Long]]
  implicit val nullSerde: Serde[Null] = fromFn(_ => null, _ => None)

  def JsonSerdeFromCodec[T >: Null](implicit codec: JsonValueCodec[T]): Serde[T] =
    Serdes.fromFn(
      data => {
        val json = writeToArray(data)
        if (json.isEmpty) null
        else {
          json
        }
      },
      data =>
        if (data == null)
          None
        else {
          Option(readFromArray(data))
        }
    )
}
