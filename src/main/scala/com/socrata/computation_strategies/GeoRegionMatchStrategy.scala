package com.socrata.computation_strategies

import com.rojoma.json.v3.codec.{Path, DecodeError, JsonDecode}
import com.socrata.soql.types.{SoQLPoint, SoQLType}

object GeoRegionMatchStrategy {


  def validate[ColumnName : JsonDecode, Schema: JsonDecode](strategyType: StrategyType,
                                                            sourceColumnType: SoQLType,
                                                            parameterSchema: ParameterSchema,
                                                            definition: StrategyDefinition[ColumnName],
                                                            columns: Option[Map[ColumnName, SoQLType]]):
    Option[ValidationError] = {
      val StrategyDefinition(typ, optSourceColumns, optParameters) = definition
      typ match {
        case StrategyType.GeoRegionMatchOnPoint | StrategyType.GeoRegion =>

          // validate the source column
          optSourceColumns match {
            case Some(Seq(name)) =>
              columns.foreach { cols =>
                cols.get(name) match {
                  case Some(`sourceColumnType`) => {} // good case, no error
                  case Some(other) => return Some(WrongSourceColumnType(name, other, SoQLPoint))
                  case None => return Some(UnknownSourceColumn(name))
                }
              }
            case Some(sourceColumns) => return Some(WrongNumberOfSourceColumns(
              received = sourceColumns.size,
              expected = 1))
            case None => return Some(MissingSourceColumns(strategyType))
          }

          // validate the parameters
          optParameters match {
            case Some(obj) => JsonDecode.fromJValue[Schema](obj) match {
              case Right(_) => None // success
              case Left(error) => error match {
                case DecodeError.MissingField(field, Path.empty) => Some(MissingParameter(field))
                case _ => Some(InvalidStrategyParameters(error))
              }
            }
            case None => Some(MissingParameters(parameterSchema))
          }
        case other => Some(WrongStrategyType(received = other, expected = strategyType))
      }
    }
}
