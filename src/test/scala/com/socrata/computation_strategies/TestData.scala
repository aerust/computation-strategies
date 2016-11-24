package com.socrata.computation_strategies

import com.rojoma.json.v3.ast.JObject
import com.rojoma.json.v3.interpolation._
import com.socrata.computation_strategies.StrategyType.{GeoRegion, GeoRegionMatchOnString, GeoRegionMatchOnPoint, Geocoding}
import com.socrata.soql.types.{SoQLPoint, SoQLText}

abstract class StrategyData(typ: StrategyType) {
  protected val strategy = typ

  def fullSources: Seq[String]
  def fullSourcesTransformed: Seq[Int]

  def fullParameters: JObject
  def fullParametersTransformed: JObject = fullParameters

  def fullDefinition = definition(Some(fullSources), Some(fullParameters))
  def fullDefinitionTransformed = Right(definition(Some(fullSourcesTransformed), Some(fullParametersTransformed)))

  def noParamsDefinition = definition(Some(fullSources), None)
  def noSourcesDefinition: StrategyDefinition[String] = definition(None, Some(fullParameters))

  def definition[T](sourceColumns: Option[Seq[T]], parameters: Option[JObject]) =
    StrategyDefinition(strategy, sourceColumns, parameters)
}

object TestData {

  // dataset columns

  // for geocoding
  val address = "address"
  val city = "city"
  val county = "county"
  val state = "state"
  val zip = "zip"
  val country = "country"

  // for region coding
  val locationPoint = "location_point"
  val locationString = "location_string"

  val columnTypes = Map(
    address -> SoQLText,
    city -> SoQLText,
    county -> SoQLText,
    state -> SoQLText,
    zip -> SoQLText,
    country -> SoQLText,
    locationPoint -> SoQLPoint,
    locationString -> SoQLText
  )

  val columnIds = Map(
    address -> 1,
    city -> 2,
    county -> 3,
    state -> 4,
    zip -> 5,
    country -> 6,
    locationPoint -> 7,
    locationString -> 8
  )

  object GeocodingData extends StrategyData(Geocoding) {

    override val fullSources = Seq(address, city, county, state, zip, country)
    override val fullParameters =
      json"""{ sources:
               { address: 'address'
               , locality: 'city'
               , subregion: 'county'
               , region: 'state'
               , postal_code: 'zip'
               , country: 'country'
               }
             , defaults:
               { address: '705 5th Ave S #600'
               , locality: 'Seattle'
               , subregion: 'King'
               , region: 'WA'
               , postal_code: '98104'
               , country: 'US'
               }
             , version: 'v1'
             }""".asInstanceOf[JObject]

    override val fullSourcesTransformed = Seq(1, 2, 3, 4, 5, 6)
    override val fullParametersTransformed =
      json"""{ sources:
               { address: 1
               , locality: 2
               , subregion: 3
               , region: 4
               , postal_code: 5
               , country: 6
               }
             , defaults:
               { address: '705 5th Ave S #600'
               , locality: 'Seattle'
               , subregion: 'King'
               , region: 'WA'
               , postal_code: '98104'
               , country: 'US'
               }
             , version: 'v1'
             }""".asInstanceOf[JObject]
  }

  class GeoRegionMatchOnPointData extends StrategyData(GeoRegionMatchOnPoint) {
    override val fullSources = Seq(locationPoint)
    override val fullParameters = json"""{ region: '_nmuc-gpu5', primary_key: '_feature_id' }""".asInstanceOf[JObject]

    override val fullSourcesTransformed = Seq(7)
  }

  val GeoRegionMatchOnPointData = new GeoRegionMatchOnPointData()

  object GeoRegionData extends GeoRegionMatchOnPointData {
    override protected val strategy = GeoRegion
  }

  object GeoRegionMatchOnStringData extends StrategyData(GeoRegionMatchOnString) {
    override val fullSources = Seq(locationString)
    override val fullParameters = json"""{ region: '_nmuc-gpu5', column: 'column', primary_key: '_feature_id' }""".asInstanceOf[JObject]

    override val fullSourcesTransformed = Seq(8)
  }

  val data: Map[StrategyType, StrategyData] = Map(
    Geocoding -> GeocodingData,
    GeoRegionMatchOnPoint -> GeoRegionMatchOnPointData,
    GeoRegion -> GeoRegionData,
    GeoRegionMatchOnString -> GeoRegionMatchOnStringData
  )

}
