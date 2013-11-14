package org.apache.spark.util.collection


private[spark]
trait HashMap[K, V] extends Iterable[(K, V)] with Serializable{
  def update(k: K, v: V)
  def changeValue(k: K, defaultValue: => V, mergeValue: (V) => V): V
}


