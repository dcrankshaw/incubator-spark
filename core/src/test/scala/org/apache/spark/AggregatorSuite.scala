/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import org.scalatest.FunSuite
import org.apache.spark.Aggregator
import org.apache.spark.util.AppendOnlyMap
import org.apache.spark.util.collection._
import org.apache.spark.SparkContext._

class AggregatorSuite extends FunSuite with LocalSparkContext {

  // Tests that Aggregator chooses the correct HashMap implementation
  // based on the type of the key
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
