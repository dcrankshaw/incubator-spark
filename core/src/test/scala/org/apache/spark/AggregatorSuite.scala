
package org.apache.spark

import org.scalatest.FunSuite
import org.apache.spark.Aggregator
import org.apache.spark.util.AppendOnlyMap
import org.apache.spark.util.collection._
import org.apache.spark.SparkContext._

class AggregatorSuite extends FunSuite with LocalSparkContext {
  test("select correct hashmap") {

    val createCombiner = (a: Int) => a
    val mergeValue = (a: Int, b: Int) => a

    val intKeyAggregator = new Aggregator[Int, Int, Int](createCombiner,
      mergeValue, mergeValue)
    val intMap = intKeyAggregator.createHashMap
    assert(intMap.isInstanceOf[PrimitiveKeyOpenHashMap[Int, Int]])

    val longKeyAggregator = new Aggregator[Long, Int, Int](createCombiner,
      mergeValue, mergeValue)
    val longMap = longKeyAggregator.createHashMap
    assert(longMap.isInstanceOf[PrimitiveKeyOpenHashMap[Long, Int]])

    val nullableKeyAggregator = new Aggregator[String, Int, Int](createCombiner,
      mergeValue, mergeValue)
    val nullableMap = nullableKeyAggregator.createHashMap
    assert(nullableMap.isInstanceOf[OpenHashMap[String, Int]])

    val doubleKeyAggregator = new Aggregator[Double, Int, Int](createCombiner,
      mergeValue, mergeValue)
    val doubleMap = doubleKeyAggregator.createHashMap
    assert(doubleMap.isInstanceOf[AppendOnlyMap[Double, Int]])
  }
}
