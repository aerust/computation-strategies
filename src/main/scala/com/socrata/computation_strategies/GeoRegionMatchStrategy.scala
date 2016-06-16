package com.socrata.computation_strategies

import com.rojoma.json.v3.codec.{Path, DecodeError, JsonDecode}
import com.socrata.soql.types.{SoQLPoint, SoQLType}

object GeoRegionMatchStrategy {


  def validate[ColumnName : JsonDecode, Schema : JsonDecode](strategyType: StrategyType,
                                                             sourceColumnType: SoQLType,
                                                             parameterSchema: ParameterSchema,
                                                             definition: StrategyDefinition[ColumnName],
                                                             columns: Option[Map[ColumnName, SoQLType]]):
    Option[ValidationError] = definition match {
      case StrategyDefinition(`strategyType`, Some(Seq(name)), Some(obj)) =>
        columns.foreach { cols =>
          cols.get(name) match {
            case Some(`sourceColumnType`) => {} // good case, no error
            case Some(other) => return Some(WrongSourceColumnType(name, other, SoQLPoint))
            case None => return Some(UnknownSourceColumn(name))
          }
        }

        JsonDecode.fromJValue[Schema](obj) match {
          case Right(_) => None // success
          case Left(DecodeError.MissingField(field, Path.empty)) => Some(MissingParameter(field))
          case Left(error) => Some(InvalidStrategyParameters(error))
        }
      case StrategyDefinition(`strategyType`, Some(cols), Some(_)) => Some(WrongNumberOfSourceColumns(cols.size, 1))
      case StrategyDefinition(`strategyType`, None, _) => Some(MissingSourceColumns(strategyType))
      case StrategyDefinition(`strategyType`, _, None) =>  Some(MissingParameters(parameterSchema))
      case StrategyDefinition(other, _, _) => Some(WrongStrategyType(received = other, expected = strategyType))
    }
}
