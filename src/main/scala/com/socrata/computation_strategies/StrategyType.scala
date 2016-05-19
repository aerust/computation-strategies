package com.socrata.computation_strategies

import com.rojoma.json.v3.ast.{JValue, JString}
import com.rojoma.json.v3.codec.{DecodeError, JsonDecode, JsonEncode}

sealed abstract class StrategyType(val name: String)

object StrategyType {

  /******************************************************************
   * IMPORTANT: when adding a strategy, add it to allStrategyTypes! *
   ******************************************************************/
  case object GeoRegionMatchOnPoint extends StrategyType("georegion_match_on_point")
  case object GeoRegionMatchOnString extends StrategyType("georegion_match_on_string")
  case object Geocoding extends StrategyType("geocoding")
  case object Test extends StrategyType("test")

  // For backwards compatibility. Superceded by georegion_match_on_point
  case object GeoRegion extends StrategyType("georegion")

  val allStrategyTypes = Set[StrategyType](
    GeoRegionMatchOnPoint,
    GeoRegionMatchOnString,
    Geocoding,
    Test,
    GeoRegion
  )

  implicit object codec extends JsonEncode[StrategyType] with JsonDecode[StrategyType] {
    val codecMap: Map[StrategyType, String] = allStrategyTypes.map { st =>
      (st, st.name)
    } (scala.collection.breakOut)
    val invCodecMap = codecMap.map(_.swap).toMap

    def encode(s: StrategyType) = JString(codecMap(s))
    def decode(x: JValue) = x match {
      case JString(s) =>
        invCodecMap.get(s) match {
          case Some(v) => Right(v)
          case None => Left(DecodeError.InvalidValue(x))
        }
      case _ =>
        Left(DecodeError.InvalidType(expected = JString, got = x.jsonType))
    }
  }

  def fromCuratedRegions(typ: StrategyType) = fromCuratedRegionsSet.contains(typ)

  private val fromCuratedRegionsSet = Set[StrategyType](
    GeoRegionMatchOnPoint,
    GeoRegionMatchOnString,
    GeoRegion
  )

  def userColumnAllowed(typ: StrategyType) = userColumnAllowedSet.contains(typ)

  private val userColumnAllowedSet = Set[StrategyType](
    Geocoding
  )

  // true if strategy type has a ComputationHandler in soda-fountain
  def computeSynchronously(typ: StrategyType) = computeSynchronouslySet.contains(typ)

  private val computeSynchronouslySet = Set[StrategyType](
    GeoRegionMatchOnPoint,
    GeoRegionMatchOnString,
    Test,
    GeoRegion
  )
}
