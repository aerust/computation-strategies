package com.socrata.computation_strategies

import com.rojoma.json.v3.codec.{JsonEncode, JsonDecode}
import com.rojoma.json.v3.util.{Strategy, JsonKeyStrategy, AutomaticJsonDecodeBuilder, AutomaticJsonEncodeBuilder}
import com.socrata.soql.types.{SoQLPoint, SoQLNumber, SoQLType}

@JsonKeyStrategy(Strategy.Underscore)
case class GeoRegionMatchOnPointParameterSchema[T,U](region: T, primaryKey: Option[U])

object GeoRegionMatchOnPointParameterSchema extends ParameterSchema {
  implicit def encoder[T : JsonEncode, U : JsonEncode] = AutomaticJsonEncodeBuilder[GeoRegionMatchOnPointParameterSchema[T,U]]
  implicit def decoder[T : JsonDecode, U : JsonDecode] = AutomaticJsonDecodeBuilder[GeoRegionMatchOnPointParameterSchema[T,U]]

  override def requiredFields = Seq("region")
}

/**
 * GeoRegionMatchOnPointComputationStrategy region codes points from its source column
 *   - Its strategy type is "georegion_match_on_point" (or legacy "georegion")
 *   - Its target column should be of type "number"
 *   - Its source column should be of type "point"
 *   - It has a required field "parameters.region" which is the resource name of the curated region
 *   - It has an optional field "parameters.primary_key" which is the primary key of the curated region //TODO ???
 *
 * For example:
 * { "type": "georegion_match_on_point",
 *   "source_columns": ["location_point"],
 *   "parameters": {
 *     "region": "_nmuc-gpu5",
 *     "primary_key": "_feature_id"
 *   }
 */
object GeoRegionMatchOnPointComputationStrategy extends ComputationStrategy {
  override def strategyType: StrategyType = StrategyType.GeoRegionMatchOnPoint

  override def acceptsStrategyType(typ: StrategyType): Boolean =
    Set[StrategyType](StrategyType.GeoRegionMatchOnPoint, StrategyType.GeoRegion).contains(typ)

  override def targetColumnType: SoQLType = SoQLNumber

  override protected def validate[ColumnName: JsonDecode](definition: StrategyDefinition[ColumnName],
                                                          columns: Option[Map[ColumnName, SoQLType]]):
    Option[ValidationError] = {
      // TODO: do we want to do something better for the resource name type parameter?
      GeoRegionMatchStrategy.validate[ColumnName, GeoRegionMatchOnPointParameterSchema[String, ColumnName]](
        strategyType,
        SoQLPoint,
        GeoRegionMatchOnPointParameterSchema,
        definition,
        columns)
    }
}
