package com.socrata.computation_strategies

import com.rojoma.json.v3.interpolation._
import com.socrata.computation_strategies
import com.socrata.computation_strategies.StrategyType._
import org.scalatest.{Matchers, FunSuite}

class ComputationStrategyTest extends FunSuite with Matchers {
  import TestData._

  val strategies = StrategyType.allStrategyTypes - Test // Don't care to test Test strategy

  def forStrategyData(test: StrategyData => Unit): Unit = {
    strategies.foreach { typ =>
      test(data(typ))
    }
  }

  test("Should map strategy types to the correct computation strategy") {
    val strategies = ComputationStrategy.strategies

    var count = 0
    def testStrategies(typ: StrategyType, strategy: ComputationStrategy): Unit = {
      strategies.get(typ) should be (Some(strategy))
      count += 1
    }

    testStrategies(GeoRegionMatchOnPoint, GeoRegionMatchOnPointComputationStrategy)
    testStrategies(GeoRegion, GeoRegionMatchOnPointComputationStrategy)
    testStrategies(GeoRegionMatchOnString, GeoRegionMatchOnStringComputationStrategy)
    testStrategies(Geocoding, GeocodingComputationStrategy)
    testStrategies(Test, TestComputationStrategy)

    strategies.size should be (count) // force people to update this test if they add new strategies
    strategies.size should be (StrategyType.allStrategyTypes.size)
  }

  test("Should be able to validate all strategy types") {
    forStrategyData { data =>
      ComputationStrategy.validate(data.fullDefinition) should be (None)
    }
  }

  test("Should be able to validate all strategy types with column types") {
    forStrategyData { data =>
      ComputationStrategy.validate(data.fullDefinition, columnTypes) should be (None)
    }
  }

  test("Should be able to transform all strategy types") {
    forStrategyData { data =>
      ComputationStrategy.transform(data.fullDefinition, columnIds) should be (data.fullDefinitionTransformed)
    }
  }

  test("Can decode region columns with 'strategy_type' keys") {
    val computationStrat = json"""
       {
        "parameters": {
          "primary_key": "_feature_id",
          "region": "_4svm-3hip"
        },
        "source_columns": ["location1"],
        "strategy_type": "georegion_match_on_point"
       }
    """
    StrategyDefinition.decoder[String].decode(computationStrat) match {
      case Right(stratDef) =>
        stratDef.typ should be (GeoRegionMatchOnPoint)
      case Left(err) =>
        fail(err.english)
    }
  }

  test("Can decode region columns with 'type' keys") {
    val computationStrat = json"""
       {
        "parameters": {
          "primary_key": "_feature_id",
          "region": "_4svm-3hip"
        },
        "source_columns": ["location1"],
        "type": "georegion_match_on_point"
       }
    """
    StrategyDefinition.decoder[String].decode(computationStrat) match {
      case Right(stratDef) =>
        stratDef.typ should be (GeoRegionMatchOnPoint)
      case Left(err) =>
        fail(err.english)
    }
  }
}
