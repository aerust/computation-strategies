package com.socrata.computation_strategies

import com.rojoma.json.v3.ast.JObject
import com.rojoma.json.v3.codec.{JsonDecode, JsonEncode}
import com.rojoma.json.v3.util._
import com.socrata.soql.types.{SoQLPoint, SoQLType}

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

case class GeocodingParameterSchema[T](sources: GeocodingSources[T], defaults: GeocodingDefaults, version: String) extends ParameterSchema

object GeocodingParameterSchema {
  implicit def encoder[T : JsonEncode] = AutomaticJsonEncodeBuilder[GeocodingParameterSchema[T]]
  implicit def decoder[T : JsonDecode] = AutomaticJsonDecodeBuilder[GeocodingParameterSchema[T]]
}

case class FlexibleGeocodingParameterSchema[T](sources: Option[GeocodingSources[T]],
                                               defaults: Option[FlexibleGeocodingDefaults],
                                               version: Option[String])

object FlexibleGeocodingParameterSchema {
  implicit def encoder[T : JsonEncode] = AutomaticJsonEncodeBuilder[FlexibleGeocodingParameterSchema[T]]
  implicit def decoder[T : JsonDecode] = AutomaticJsonDecodeBuilder[FlexibleGeocodingParameterSchema[T]]

  def empty[T] = FlexibleGeocodingParameterSchema[T](None, None, None)
}

object GeocodingComputationStrategy extends ComputationStrategy with Augment[FlexibleGeocodingDefaults] {

  val apiVersion = "v1"
  val apiVersions = Set(apiVersion)

  val defaultUS =  "US"

  override def strategyType: StrategyType.Value = StrategyType.Geocoding

  override def acceptsStrategyType(typ: StrategyType.Value) = typ == StrategyType.Geocoding

  override def targetColumnType: SoQLType = SoQLPoint

  case class GeocodingSourcesDoNotMatchSourceColumns[ColumnName](sourceColumns: Seq[ColumnName],
                                                                 sources: Option[GeocodingSources[ColumnName]])
    extends ValidationError(s"Computation strategy 'parameters.sources' do not match 'source_columns'." +
      s"\n\tsource_columns: $sourceColumns" +
      s"\n\tparameters.sources: $sources")

  case class UnknownGeocodingApiVersion(version: String)
    extends ValidationError(s"Unknown geocoding api version: $version.")

  def coreValidate[ColumnName](definition: StrategyDefinition[ColumnName])
                              (decode: JsonDecode[ColumnName]): Option[ValidationError] = {
    val StrategyDefinition(typ, optSourceColumns, optParameters) = definition
    typ match {
      case StrategyType.Geocoding =>
        // source columns and parameters should match
        // since we are being "flexible" we aren't concerned about defaults
        val sourceColumns = optSourceColumns.getOrElse(Seq.empty)
        val parameters = optParameters match {
          case Some(obj) => FlexibleGeocodingParameterSchema.decoder[ColumnName](decode).decode(obj) match {
            case Right(res) => res
            case Left(error) => return Some(InvalidStrategyParameters(error))
          }
          case None => FlexibleGeocodingParameterSchema.empty[ColumnName]
        }
        val paramSources = parameters.sources.map { GeocodingSources.toSet[ColumnName] }.getOrElse(Set.empty)
        if (!sourceColumns.toSet.equals(paramSources))
          return Some(GeocodingSourcesDoNotMatchSourceColumns(sourceColumns, parameters.sources))

        // the version should be valid (or not given)
        parameters.version match {
          case Some(version) =>
            if (!apiVersions.contains(version)) Some(UnknownGeocodingApiVersion(version))
            else None
          case None => None
        }
      case other => Some(WrongStrategyType(received = other, expected = strategyType))
    }
  }

  override def augment[ColumnName](definition: StrategyDefinition[ColumnName],
                                   info: FlexibleGeocodingDefaults,
                                   decode: JsonDecode[ColumnName],
                                   encode: JsonEncode[ColumnName]): Either[ValidationError, StrategyDefinition[ColumnName]] = {
    FlexibleGeocodingParameterSchema.decoder(decode).decode(definition.parameters.getOrElse(JObject.canonicalEmpty)) match {
      case Right(schema) =>
        val augmented = augment(schema, info)
        GeocodingParameterSchema.encoder(encode).encode(augmented) match {
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

  override def sodaValidate[ColumnName](definition: StrategyDefinition[ColumnName])
                                       (decode: JsonDecode[ColumnName]): Option[ValidationError] = {
    val StrategyDefinition(typ, optSourceColumns, optParameters) = definition
    typ match {
      case StrategyType.Geocoding =>
        val parameters = optParameters match {
          case Some(obj) => GeocodingParameterSchema.decoder[ColumnName](decode).decode(obj) match {
            case Right(res) => res
            case Left(error) => return Some(InvalidStrategyParameters(error))
          }
          case None => return Some(ParametersNotFoundWhenRequired(strategyType))
        }

        // source columns and parameters.sources should match
        val sourceColumns = optSourceColumns.getOrElse(Seq.empty)
        val paramSources = GeocodingSources.toSet[ColumnName](parameters.sources)
        if (!sourceColumns.toSet.equals(paramSources))
          return Some(GeocodingSourcesDoNotMatchSourceColumns(sourceColumns, Some(parameters.sources)))

        // country default is insured by successfully decoding

        // the version should be valid
        if (!apiVersions.contains(parameters.version))
          return Some(UnknownGeocodingApiVersion(parameters.version))

        None
      case other => Some(WrongStrategyType(received = other, expected = strategyType))
    }
  }
}
