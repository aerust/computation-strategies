package com.socrata.computation_strategies

import com.rojoma.json.v3.codec.{Path, DecodeError, JsonDecode}
import com.rojoma.json.v3.util.{AutomaticJsonCodecBuilder, Strategy, JsonKeyStrategy}
import com.socrata.soql.types.{SoQLText, SoQLType}

@JsonKeyStrategy(Strategy.Underscore)
case class TestParameterSchema(concatText: String)

object TestParameterSchema extends ParameterSchema {
  implicit val codec = AutomaticJsonCodecBuilder[TestParameterSchema]

  override def requiredFields = Seq("concat_text")
}

/**
 * TestComputationStrategy is a simple computation strategy for testing;
 * it concatenates a value to the value found in its source column.
 *   - Its strategy type is "test"
 *   - Its target column should be of type "text"
 *   - It should have a _single_ source column of type "text"
 *   - It has the required parameter "concat_text"
 *
 * For example:
 * { "type": "test", "source_columns": ["my_source"], "parameters": { "concat_text" : "foo" } }
 */
object TestComputationStrategy extends ComputationStrategy {
  override def strategyType: StrategyType = StrategyType.Test

  override def targetColumnType: SoQLType = SoQLText

  override def acceptsStrategyType(typ: StrategyType): Boolean = typ.equals(strategyType)

  override protected def validate[ColumnName : JsonDecode](definition: StrategyDefinition[ColumnName],
                                                           columns: Option[Map[ColumnName, SoQLType]]):
    Option[ValidationError] = definition match {
      case StrategyDefinition(StrategyType.Test, Some(Seq(name)), Some(obj)) =>
        // validate type of source column optionally
        columns.foreach { cols =>
          cols.get(name) match {
            case Some(SoQLText) => {}
            case Some(other) => return Some(WrongSourceColumnType(name, other, SoQLText))
            case None => return Some(UnknownSourceColumn(name))
          }
        }

        JsonDecode.fromJValue[TestParameterSchema](obj) match {
          case Right(parameters) => None
          case Left(DecodeError.MissingField(field, Path.empty)) => Some(MissingParameter(field))
          case Left(error) => Some(InvalidStrategyParameters(error))
        }
      case StrategyDefinition(StrategyType.Test, Some(cols), Some(_)) => Some(WrongNumberOfSourceColumns(cols.size, 1))
      case StrategyDefinition(StrategyType.Test, None, _) => Some(MissingSourceColumns(strategyType))
      case StrategyDefinition(StrategyType.Test, _, None) => Some(MissingParameters(TestParameterSchema))
      case StrategyDefinition(other, _, _) => Some(WrongStrategyType(received = other, expected = strategyType))
    }
}
