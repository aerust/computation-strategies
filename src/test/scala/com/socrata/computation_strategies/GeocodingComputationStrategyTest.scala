package com.socrata.computation_strategies

import com.socrata.computation_strategies.GeocodingComputationStrategy.GeocodingSourcesDoNotMatchSourceColumns
import org.scalatest.{ShouldMatchers, FunSuite}

class GeocodingComputationStrategyTest extends FunSuite with ShouldMatchers {
  import TestData._
  import TestData.GeocodingData._

  def testValidate(definition: StrategyDefinition[String], expected: Option[ValidationError] = None): Unit = {
    GeocodingComputationStrategy.validate(definition) should be (expected)
  }

  def testTransform(definition: StrategyDefinition[String],
                    columns: Map[String, Int],
                    expected: Either[ValidationError, StrategyDefinition[Int]]): Unit = {
    GeocodingComputationStrategy.transform(definition, columns) should be (expected)
  }

  test("Definition with full sources and parameters should be invalid") {
    testValidate(fullDefinition)
  }

  test("Definition with no sources should be invalid") {
    val expected = Some(GeocodingSourcesDoNotMatchSourceColumns(List(), Some(GeocodingSources(Some(address),Some(city),
      Some(county),Some(state),Some(zip),Some(country)))))
    testValidate(noSourcesDefinition, expected)
  }

  test("Definition with no parameters should be invalid") {
    testValidate(noParamsDefinition, Some(MissingParameters(GeocodingParameterSchema)))
  }

  test("Definition with an unknown source column should fail to transform") {
    testTransform(fullDefinition, columnIds - address, Left(UnknownSourceColumn(address)))
  }

}
