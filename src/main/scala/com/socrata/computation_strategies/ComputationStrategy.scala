package com.socrata.computation_strategies

import com.rojoma.json.v3.ast.JObject
import com.rojoma.json.v3.codec.{DecodeError, JsonDecode, JsonEncode}
import com.rojoma.json.v3.util._
import com.socrata.soql.types.SoQLType

abstract class ValidationError(val message: String)

case class WrongStrategyType(received: StrategyType.Value, expected: StrategyType.Value)
  extends ValidationError(s"Wrong target column data type; received: $received, expected: $expected")
case class WrongTargetColumnType(received: SoQLType, expected: SoQLType)
  extends ValidationError(s"Wrong target column data type; received: $received, expected: $expected")
case class InvalidStrategyParameters(error: DecodeError)
  extends ValidationError(s"Unable to decode computation strategy parameters: ${error.english}")
case class ParametersNotFoundWhenRequired(typ: StrategyType.Value)
  extends ValidationError(s"Computation strategy parameters not found when required for strategy type: $typ")

case class StrategyDefinition[ColumnName](@JsonKey("type") typ: StrategyType.Value,
                              @JsonKey("source_columns") sourceColumns: Option[Seq[ColumnName]],
                              @JsonKey("parameters") parameters: Option[JObject])

object StrategyDefinition {
  implicit def encoder[ColumnName: JsonEncode] = AutomaticJsonEncodeBuilder[StrategyDefinition[ColumnName]]
  implicit def decoder[ColumnName: JsonDecode] = AutomaticJsonDecodeBuilder[StrategyDefinition[ColumnName]]
}

@JsonKeyStrategy(Strategy.Underscore)
case class InternalStrategyDefinition[ColumnId](strategyType: StrategyType.Value,
                                      sourceColumnIds: Seq[ColumnId],
                                      parameters: JObject)

trait ComputationStrategy {

  def strategyType: StrategyType.Value

  def acceptsStrategyType(typ: StrategyType.Value): Boolean

  def targetColumnType: SoQLType

  def coreValidate[ColumnName](definition: StrategyDefinition[ColumnName])
                              (decode: JsonDecode[ColumnName]): Option[ValidationError]

  def sodaValidate[ColumnName](definition: StrategyDefinition[ColumnName])
                              (decode: JsonDecode[ColumnName]): Option[ValidationError]
}

trait ParameterSchema

trait Augment[T] {

  def augment[ColumnName](definition: StrategyDefinition[ColumnName],
                          info: T,
                          decode: JsonDecode[ColumnName],
                          encode: JsonEncode[ColumnName]): Either[ValidationError, StrategyDefinition[ColumnName]]
}
