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

<<<<<<< HEAD
import org.apache.spark.util.AppendOnlyMap
import org.apache.spark.util.collection.{HashMap, OpenHashMap,PrimitiveKeyOpenHashMap}
=======
//import org.apache.spark.util.AppendOnlyMap
import org.apache.spark.util.hash.{OpenHashMap,PrimitiveKeyOpenHashMap}
>>>>>>> Initial work on integrating in new hashset

/**
 * A set of functions used to aggregate data.
 *
 * @param createCombiner function to create the initial value of the aggregation.
 * @param mergeValue function to merge a new value into the aggregation result.
 * @param mergeCombiners function to merge outputs from multiple mergeValue function.
 */


// TODO(crankshaw) I think what I have to do here is decide which hashmap
// to use based on whether K is nullable or primitive?
case class Aggregator[K: ClassManifest, V, C: ClassManifest] (
    createCombiner: V => C,
    mergeValue: (C, V) => C,
    mergeCombiners: (C, C) => C) {

  def combineValuesByKey(iter: Iterator[_ <: Product2[K, V]]) : Iterator[(K, C)] = {


    val combiners: HashMap[K, C] = {
      val mk = classManifest[K]
      if (mk >:> classManifest[Null]) {
        (new OpenHashMap[AnyRef, C]).asInstanceOf[HashMap[K, C]]
      } else if (mk == classManifest[Long]) {
        (new PrimitiveKeyOpenHashMap[Long, C]).asInstanceOf[HashMap[K, C]]
      } else if (mk == classManifest[Int]) {
        (new PrimitiveKeyOpenHashMap[Int, C]).asInstanceOf[HashMap[K, C]]
      } else {
        (new AppendOnlyMap[K, C]).asInstanceOf[HashMap[K, C]]
      }
    }

    //val combiners: HashMap[K, C] = new AppendOnlyMap[K, C]
    var kv: Product2[K, V] = null
    val update = (oldValue: C) => {
      mergeValue(oldValue, kv._2)
    }
    while (iter.hasNext) {
      kv = iter.next()
      combiners.changeValue(kv._1, createCombiner(kv._2), update)
    }
    combiners.iterator
  }

  def combineCombinersByKey(iter: Iterator[(K, C)]) : Iterator[(K, C)] = {
    //val combiners: HashMap[K, C] = new AppendOnlyMap[K, C]
    val combiners: HashMap[K, C] = {
      val mk = classManifest[K]
      if (mk >:> classManifest[Null]) {
        (new OpenHashMap[AnyRef, C]).asInstanceOf[HashMap[K, C]]
      } else if (mk == classManifest[Long]) {
        (new PrimitiveKeyOpenHashMap[Long, C]).asInstanceOf[HashMap[K, C]]
      } else if (mk == classManifest[Int]) {
        (new PrimitiveKeyOpenHashMap[Int, C]).asInstanceOf[HashMap[K, C]]
      } else {
        (new AppendOnlyMap[K, C]).asInstanceOf[HashMap[K, C]]
      }
    }

    var kc: (K, C) = null
    val update = (oldValue: C) => {
      mergeCombiners(oldValue, kc._2)
    }
    while (iter.hasNext) {
      kc = iter.next()
      combiners.changeValue(kc._1, kc._2, update)
    }
    combiners.iterator
  }
}

