package com.socrata.computation_strategies

import com.rojoma.json.v3.codec.JsonDecode
import com.socrata.soql.types.{SoQLNumber, SoQLType}

object GeoRegionMatchOnStringComputationStrategy extends ComputationStrategy {
  override def strategyType: StrategyType = StrategyType.GeoRegionMatchOnString

  override def acceptsStrategyType(typ: StrategyType): Boolean = typ.equals(strategyType)

  override def targetColumnType: SoQLType = SoQLNumber

  override protected def validate[ColumnName : JsonDecode](definition: StrategyDefinition[ColumnName],
                                                           columns: Option[Map[ColumnName, SoQLType]]):
  Option[ValidationError] = {
    // TODO: implement
    None
  }
}
