package com.socrata.computation_strategies

import com.rojoma.json.v3.codec.JsonDecode
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
    Option[ValidationError] = {
      val StrategyDefinition(typ, optSourceColumns, optParameters) = definition
      typ match {
        case StrategyType.Test =>
          optSourceColumns match {
            case Some(sourceColumns) =>
              sourceColumns.length match {
                case 1 =>
                  // validate type of source column optionally
                  if (columns.isDefined) {
                    val name = sourceColumns.head
                    val error = columns.get.get(name) match {
                      case Some(SoQLText) => None
                      case Some(other) => Some(WrongSourceColumnType(name, other, SoQLText))
                      case None => Some(UnknownSourceColumn(name))
                    }
                    if (error.isDefined) return error
                  }
                  optParameters match {
                    case Some(obj) => JsonDecode.fromJValue[TestParameterSchema](obj) match {
                      case Right(parameters) => None
                      case Left(error) => Some(InvalidStrategyParameters(error))
                    }
                    case None => Some(MissingParameters(TestParameterSchema))
                  }
                case other => Some(WrongNumberOfSourceColumns(received = other, expected = 1))
              }
            case None => Some(MissingSourceColumns(strategyType))
          }
        case other => Some(WrongStrategyType(received = other, expected = strategyType))
      }
    }
}
