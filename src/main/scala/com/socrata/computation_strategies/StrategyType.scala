package com.socrata.computation_strategies

import com.rojoma.json.v3.ast.{JValue, JString}
import com.rojoma.json.v3.codec.DecodeError.{InvalidType, InvalidValue}
import com.rojoma.json.v3.codec.{JsonDecode, JsonEncode}

import scala.util.Try

object StrategyType extends Enumeration {
  implicit val JsonCodec = StrategyTypeCodec

  val GeoRegionMatchOnPoint = Value("georegion_match_on_point")
  val GeoRegionMatchOnString = Value("georegion_match_on_string")
  val Geocoding = Value("geocoding")
  val Test      = Value("test")

  // For backwards compatibility. Superceded by georegion_match_on_point
  val GeoRegion = Value("georegion")

  def fromCuratedRegions(v: Value) = fromCuratedRegionsSet.contains(v)

  private val fromCuratedRegionsSet = Set(
    GeoRegionMatchOnPoint,
    GeoRegionMatchOnString,
    GeoRegion
  )

  def userColumnAllowed(v: Value) = userColumnAllowedSet.contains(v)

  private val userColumnAllowedSet = Set(
    Geocoding
  )

  // true if strategy type has a ComputationHandler in soda-fountain
  def computeSynchronously(v: Value) = computeSynchronouslySet.contains(v)

  private val computeSynchronouslySet = Set(
    GeoRegionMatchOnPoint,
    GeoRegionMatchOnString,
    Test
  )
}

object StrategyTypeCodec extends JsonEncode[StrategyType.Value] with JsonDecode[StrategyType.Value] {
  def encode(v: StrategyType.Value) = JString(v.toString)
  def decode(x: JValue) = x match {
    case JString(s) =>
      Try(StrategyType.withName(s)).toOption.map(Right(_)).getOrElse(Left(InvalidValue(x)))
    case u => Left(InvalidType(JString, u.jsonType))
  }
}
