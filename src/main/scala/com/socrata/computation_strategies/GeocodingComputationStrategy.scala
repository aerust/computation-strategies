package com.socrata.computation_strategies

import com.rojoma.json.v3.ast.JObject
import com.rojoma.json.v3.codec.{Path, DecodeError, JsonDecode, JsonEncode}
import com.rojoma.json.v3.util._
import com.socrata.soql.types.{SoQLNumber, SoQLText, SoQLPoint, SoQLType}

@JsonKeyStrategy(Strategy.Underscore)
case class GeocodingSources[T](address: Option[T],
                               locality: Option[T],
                               subregion: Option[T],
                               region: Option[T],
                               postalCode: Option[T],
                               country: Option[T])

object GeocodingSources {
  implicit def encoder[T : JsonEncode] = AutomaticJsonEncodeBuilder[GeocodingSources[T]]
  implicit def decoder[T : JsonDecode] = AutomaticJsonDecodeBuilder[GeocodingSources[T]]


  def toSet[T](sources: GeocodingSources[T]): Set[T] =
    Set(
      sources.address,
      sources.locality,
      sources.subregion,
      sources.region,
      sources.postalCode,
      sources.country).filter(_.isDefined).map(_.get)

  def empty[T] = GeocodingSources[T](None, None, None, None, None, None)
}

@JsonKeyStrategy(Strategy.Underscore)
case class GeocodingDefaults(address: Option[String],
                    locality: Option[String],
                    subregion: Option[String],
                    region: Option[String],
                    postalCode: Option[String],
                    country: String)

object GeocodingDefaults {
  implicit val codec = AutomaticJsonCodecBuilder[GeocodingDefaults]
}

@JsonKeyStrategy(Strategy.Underscore)
case class FlexibleGeocodingDefaults(address: Option[String],
                                     locality: Option[String],
                                     subregion: Option[String],
                                     region: Option[String],
                                     postalCode: Option[String],
                                     country: Option[String])

object FlexibleGeocodingDefaults {
  implicit val codec = AutomaticJsonCodecBuilder[FlexibleGeocodingDefaults]

  def empty = FlexibleGeocodingDefaults(None, None, None, None, None, None)
}

case class GeocodingParameterSchema[T](sources: GeocodingSources[T], defaults: GeocodingDefaults, version: String)

object GeocodingParameterSchema extends ParameterSchema {
  implicit def encoder[T : JsonEncode] = AutomaticJsonEncodeBuilder[GeocodingParameterSchema[T]]
  implicit def decoder[T : JsonDecode] = AutomaticJsonDecodeBuilder[GeocodingParameterSchema[T]]

  def requiredFields = Seq("defaults")
}

case class FlexibleGeocodingParameterSchema[T](sources: Option[GeocodingSources[T]],
                                               defaults: Option[FlexibleGeocodingDefaults],
                                               version: Option[String])

object FlexibleGeocodingParameterSchema {
  implicit def encoder[T : JsonEncode] = AutomaticJsonEncodeBuilder[FlexibleGeocodingParameterSchema[T]]
  implicit def decoder[T : JsonDecode] = AutomaticJsonDecodeBuilder[FlexibleGeocodingParameterSchema[T]]

  def empty[T] = FlexibleGeocodingParameterSchema[T](None, None, None)
}

/**
 * GeocodingComputationStrategy geocodes an address or centroid defined by its source columns.
 *   - Its strategy type is "geocoding"
 *   - Its target column should be of type "point"
 *   - Its source columns should be of type "text"
 *   - Its source columns and parameter "sources" should match
 *
 * For example:
 * { "type": "geocoding",
 *   "source_columns": ["street_address", "city", "zip_code"],
 *   "parameters": {
 *       "sources": {
 *           "address": "street_address",
 *           "locality": "city",
 *           "postal_code": "zip_code" },
 *       "defaults": {
 *           "region": "WA",
 *           "country": "US" },
 *       "version": "v1" }}
 */
object GeocodingComputationStrategy extends ComputationStrategy with Augment[FlexibleGeocodingDefaults] {

  val apiVersion = "v1"
  val apiVersions = Set(apiVersion)

  val defaultUS =  "US"

  override def strategyType: StrategyType = StrategyType.Geocoding

  override def acceptsStrategyType(typ: StrategyType) = typ == StrategyType.Geocoding

  override def targetColumnType: SoQLType = SoQLPoint

  case class GeocodingSourcesDoNotMatchSourceColumns[ColumnName](sourceColumns: Seq[ColumnName],
                                                                 sources: Option[GeocodingSources[ColumnName]])
    extends ValidationError(s"""Computation strategy "parameters.sources" do not match "source_columns"; """""" +
      s"source_columns: $sourceColumns, " +
      s"parameters.sources: $sources")

  case class UnknownGeocodingApiVersion(version: String)
    extends ValidationError(s"Unknown geocoding api version: $version.")

  override def augment[ColumnName : JsonDecode : JsonEncode](definition: StrategyDefinition[ColumnName],
                                                             info: FlexibleGeocodingDefaults):
  Either[ValidationError, StrategyDefinition[ColumnName]] = {
    JsonDecode.fromJValue[FlexibleGeocodingParameterSchema[ColumnName]](definition.parameters.getOrElse(JObject.canonicalEmpty)) match {
      case Right(schema) =>
        val augmented = augment(schema, info)
        JsonEncode.toJValue[GeocodingParameterSchema[ColumnName]](augmented) match {
          case obj: JObject => Right(definition.copy(parameters = Some(obj)))
          case other => throw new InternalError(s"GeocodingParameterSchema encoded to $other and not JObject?") // this shouldn't happen
        }
      case Left(error) => Left(InvalidStrategyParameters(error))
    }
  }

  // should augment a valid FlexibleGeocodingParameterSchema to be a valid GeocodingParameterSchema
  private def augment[ColumnName](flexibleStrategy: FlexibleGeocodingParameterSchema[ColumnName],
                                  envDefaults: FlexibleGeocodingDefaults): GeocodingParameterSchema[ColumnName] = {
    val FlexibleGeocodingParameterSchema(sources, defaults, version) = flexibleStrategy
    GeocodingParameterSchema(
      sources.getOrElse(GeocodingSources.empty[ColumnName]),
      defaults match {
        case Some(FlexibleGeocodingDefaults(address, locality, subregion, region, postalCode, country)) =>
          GeocodingDefaults(
            address.orElse(envDefaults.address),
            locality.orElse(envDefaults.locality),
            subregion.orElse(envDefaults.subregion),
            region.orElse(envDefaults.region),
            postalCode.orElse(envDefaults.postalCode),
            country.getOrElse(envDefaults.country.getOrElse(defaultUS)))
        case None =>
          GeocodingDefaults(
            envDefaults.address,
            envDefaults.locality,
            envDefaults.subregion,
            envDefaults.region,
            envDefaults.postalCode,
            envDefaults.country.getOrElse(defaultUS))
      },
      version.getOrElse(apiVersion)
    )
  }

  override protected def validate[ColumnName : JsonDecode](definition: StrategyDefinition[ColumnName],
                                                           columns: Option[Map[ColumnName, SoQLType]]):
    Option[ValidationError] = definition match {
      case StrategyDefinition(StrategyType.Geocoding, optSourceColumns, Some(obj)) =>
        val parameters = JsonDecode.fromJValue[GeocodingParameterSchema[ColumnName]](obj) match {
          case Right(res) => res
          case Left(DecodeError.MissingField(field, Path.empty)) => return Some(MissingParameter(field))
          case Left(error) => return Some(InvalidStrategyParameters(error))
        }

        // source columns and parameters.sources should match
        val sourceColumns = optSourceColumns.getOrElse(Seq.empty)
        val paramSources = GeocodingSources.toSet[ColumnName](parameters.sources)
        if (!sourceColumns.toSet.equals(paramSources))
          return Some(GeocodingSourcesDoNotMatchSourceColumns(sourceColumns, Some(parameters.sources)))

        // source columns should have the correct soql type
        columns.foreach { cols =>
          val sources = parameters.sources
          val sourceColumnTypeErrors = Seq(
            validateSoQLType(sources.address, cols),
            validateSoQLType(sources.locality, cols),
            validateSoQLType(sources.subregion, cols),
            validateSoQLType(sources.region, cols),
            validateSoQLType(sources.postalCode, cols, numberOkay = true),
            validateSoQLType(sources.country, cols)
          )
          if (sourceColumnTypeErrors.exists(_.isDefined)) return sourceColumnTypeErrors.filter(_.isDefined).head
        }

        // country default is insured by successfully decoding

        // the version should be valid
        if (!apiVersions.contains(parameters.version)) return Some(UnknownGeocodingApiVersion(parameters.version))

        None
      case StrategyDefinition(StrategyType.Geocoding, _, None) => Some(MissingParameters(GeocodingParameterSchema))
      case StrategyDefinition(other, _, _) => Some(WrongStrategyType(received = other, expected = strategyType))
    }

  private def validateSoQLType[ColumnName](optName: Option[ColumnName],
                                           columns: Map[ColumnName, SoQLType],
                                           numberOkay: Boolean = false): Option[ValidationError] = optName match {
    case Some(name) =>
      columns.get(name) match {
        case Some(SoQLText) => None // success
        case Some(SoQLNumber) => if (numberOkay) None else Some(WrongSourceColumnType(name, SoQLNumber, SoQLText))
        case Some(other) => Some(WrongSourceColumnType(name, other, SoQLText))
        case None => Some(UnknownSourceColumn(name))
      }
    case None => None
  }

  case class UnknownSourceColumnException[CN](cn: CN) extends Exception
  override protected def transform[CN: JsonDecode, CI : JsonEncode](parameters: JObject, columns: Map[CN, CI]):
    Either[ValidationError, JObject] = {
      def map(cn: Option[CN]): Option[CI] = cn.map { cn =>
        columns.getOrElse(cn, throw UnknownSourceColumnException(cn))
      }

      JsonDecode.fromJValue[FlexibleGeocodingParameterSchema[CN]](parameters) match {
        case Right(FlexibleGeocodingParameterSchema(sources, defaults, version)) =>
          try {
            val transformed = sources.map { srcs: GeocodingSources[CN] =>
              GeocodingSources[CI](
                address = map(srcs.address),
                locality = map(srcs.locality),
                subregion = map(srcs.subregion),
                region = map(srcs.region),
                postalCode = map(srcs.postalCode),
                country = map(srcs.country)
              )
            }

            // should always be encoded to a JObject
            Right(JsonEncode.toJValue(FlexibleGeocodingParameterSchema(transformed, defaults, version)).asInstanceOf[JObject])
          } catch {
            case UnknownSourceColumnException(cn) => Left(UnknownSourceColumn(cn))
          }
        case Left(DecodeError.MissingField(field, Path.empty)) => Left(MissingParameter(field))
        case Left(error) => Left(InvalidStrategyParameters(error))
      }
    }
}
