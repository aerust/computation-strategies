package com.socrata.computation_strategies

import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode}
import com.rojoma.json.v3.util.{AutomaticJsonDecodeBuilder, AutomaticJsonEncodeBuilder}
import com.socrata.soql.types.{SoQLText, SoQLNumber, SoQLType}

case class GeoRegionMatchOnStringParameterSchema[T,U](region: T, column: U, primaryKey: Option[U])

object GeoRegionMatchOnStringParameterSchema  extends ParameterSchema {
  implicit def encoder[T : JsonEncode, U : JsonEncode] = AutomaticJsonEncodeBuilder[GeoRegionMatchOnStringParameterSchema[T,U]]
  implicit def decoder[T : JsonDecode, U : JsonDecode] = AutomaticJsonDecodeBuilder[GeoRegionMatchOnStringParameterSchema[T,U]]

  override def requiredFields = Seq("region", "column")
}

/**
 * GeoRegionMatchOnStringComputationStrategy region codes strings from its source column
 *   - Its strategy type is "georegion_match_on_string"
 *   - Its target column should be of type "number"
 *   - Its source column should be of type "text"
 *   - It has a required field "parameters.region" which is the resource name of the curated region
 *   - It has a required field "parameters.column"
 *   - It has an optional field "parameters.primary_key" which is the primary key of the curated region // TODO ???
 *
 * For example:
 * { "type": "georegion_match_on_point",
 *   "source_columns": ["location_point"],
 *   "parameters": {
 *     "region": "_nmuc-gpu5",
 *     "column": "column_1",
 *     "primary_key": "_feature_id"
 *   }
 */
object GeoRegionMatchOnStringComputationStrategy extends ComputationStrategy {
  override def strategyType: StrategyType = StrategyType.GeoRegionMatchOnString

  override def acceptsStrategyType(typ: StrategyType): Boolean = typ.equals(strategyType)

  override def targetColumnType: SoQLType = SoQLNumber

  override protected def validate[ColumnName : JsonDecode](definition: StrategyDefinition[ColumnName],
                                                           columns: Option[Map[ColumnName, SoQLType]]):
    Option[ValidationError] = {
      // TODO: do we want to do something better for the resource name type parameter?
      GeoRegionMatchStrategy.validate[ColumnName, GeoRegionMatchOnStringParameterSchema[String, ColumnName]](
        strategyType,
        SoQLText,
        GeoRegionMatchOnStringParameterSchema,
        definition,
        columns)
    }
}
