package com.socrata.computation_strategies

import com.rojoma.json.v3.ast.JObject
import com.rojoma.json.v3.codec.{DecodeError, JsonDecode, JsonEncode}
import com.rojoma.json.v3.util._
import com.socrata.soql.types.SoQLType

abstract class ValidationError(val message: String)

case class WrongStrategyType(received: StrategyType, expected: StrategyType)
  extends ValidationError(s"Wrong strategy type; received: $received, expected: $expected")

case class WrongTargetColumnType(received: SoQLType, expected: SoQLType)
  extends ValidationError(s"Wrong target column data type; received: $received, expected: $expected")

case class WrongSourceColumnType[ColumnName](name: ColumnName, received: SoQLType, expected: SoQLType)
  extends ValidationError(s"Wrong data type for source column $name; received: $received, expected: $expected")

case class WrongNumberOfSourceColumns(received: Int, expected: Int)
  extends ValidationError(s"Wrong number of source columns; received: $received, expected: $expected")

case class UnknownSourceColumn[ColumnName](name: ColumnName)
  extends ValidationError(s"Unknown source column: $name")

case class MissingSourceColumns(typ: StrategyType)
  extends ValidationError(s"Source columns not found when required for strategy type: $typ")

case class MissingParameters(schema: ParameterSchema)
  extends ValidationError(s"Missing required computation strategy parameters: ${schema.requiredFields}")

case class MissingParameter(field: String)
  extends ValidationError(s"""Missing computation strategy parameters field "$field"""")

case class InvalidStrategyParameters(error: DecodeError)
  extends ValidationError(s"Unable to decode computation strategy parameters: ${error.english}")

case class StrategyDefinition[ColumnName](@JsonKey("type") typ: StrategyType,
                                          @JsonKey("source_columns") sourceColumns: Option[Seq[ColumnName]],
                                          @JsonKey("parameters") parameters: Option[JObject])

object StrategyDefinition {
  implicit def encoder[ColumnName : JsonEncode] = AutomaticJsonEncodeBuilder[StrategyDefinition[ColumnName]]
  implicit def decoder[ColumnName : JsonDecode] = AutomaticJsonDecodeBuilder[StrategyDefinition[ColumnName]]
}

@JsonKeyStrategy(Strategy.Underscore)
case class InternalStrategyDefinition[ColumnId](strategyType: StrategyType,
                                                sourceColumnIds: Seq[ColumnId],
                                                parameters: JObject)

trait ComputationStrategy {

  def strategyType: StrategyType

  def acceptsStrategyType(typ: StrategyType): Boolean

  def targetColumnType: SoQLType

  def validate[ColumnName : JsonDecode](definition: StrategyDefinition[ColumnName]): Option[ValidationError] =
    validate[ColumnName](definition, None)

  def validate[ColumnName : JsonDecode](definition: StrategyDefinition[ColumnName],
                                        columns: Map[ColumnName, SoQLType]): Option[ValidationError] =
    validate[ColumnName](definition, Some(columns))

  protected def validate[ColumnName : JsonDecode](definition: StrategyDefinition[ColumnName],
                                                  columns: Option[Map[ColumnName, SoQLType]]): Option[ValidationError]
}

object ComputationStrategy {

  val strategies = Map[StrategyType, ComputationStrategy](
    StrategyType.GeoRegionMatchOnPoint -> GeoRegionMatchOnPointComputationStrategy,
    StrategyType.GeoRegion -> GeoRegionMatchOnPointComputationStrategy,
    StrategyType.GeoRegionMatchOnString -> GeoRegionMatchOnStringComputationStrategy,
    StrategyType.Geocoding -> GeocodingComputationStrategy,
    StrategyType.Test -> TestComputationStrategy
  )

  def validate[ColumnName : JsonDecode](definition: StrategyDefinition[ColumnName]): Option[ValidationError] =
    strategies(definition.typ).validate(definition)

  def validate[ColumnName : JsonDecode](definition: StrategyDefinition[ColumnName],
                                        columns: Map[ColumnName, SoQLType]): Option[ValidationError] =
    strategies(definition.typ).validate(definition, columns)
}

trait ParameterSchema {
  def requiredFields: Seq[String]
}

trait Augment[T] {

  def augment[ColumnName : JsonDecode : JsonEncode](definition: StrategyDefinition[ColumnName],
                                                   info: T): Either[ValidationError, StrategyDefinition[ColumnName]]
}
