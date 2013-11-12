
package org.apache.spark.util.hash

import scala.collection.mutable.HashSet
import org.scalatest.FunSuite

class HashMapSuite extends FunSuite {

  test("initialization") {
    val goodMap1 = new OpenHashMap[String, Int](1)
    assert(goodMap1.size === 0)
    val goodMap2 = new OpenHashMap[String, Int](255)
    assert(goodMap2.size === 0)
    val goodMap3 = new OpenHashMap[String, String](256)
    assert(goodMap3.size === 0)
    intercept[IllegalArgumentException] {
      new OpenHashMap[String, Int](1 << 30) // Invalid map size: bigger than 2^29
    }
    intercept[IllegalArgumentException] {
      new OpenHashMap[String, Int](-1)
    }
    intercept[IllegalArgumentException] {
      new OpenHashMap[String, String](0)
    }
  }
}
