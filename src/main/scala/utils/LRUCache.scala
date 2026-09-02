package utils

import java.util.Map

/**
 * A cache with least-recently-used policy
 * @param cacheSize the size of the cache in terms of number of entries
 * @tparam K Key type
 * @tparam V Value type
 */
class LRUCache[K,V](cacheSize: Int = 16, evictNotifier: (K, V) => Unit = null) extends collection.mutable.Map[K,V] {

  val internalHashMap = new java.util.LinkedHashMap[K,V](cacheSize * 4 / 3, 0.75f, true) {
    override def removeEldestEntry(eldest: Map.Entry[K, V]): Boolean = {
      if (size > cacheSize) {
        if (evictNotifier != null)
          evictNotifier.apply(eldest.getKey, eldest.getValue)
        return true
      }
      return false
    }
  }

  override def clear(): Unit = internalHashMap.clear()

  override def size: Int = internalHashMap.size()

  override def +=(kv: (K, V)): LRUCache.this.type = {
    internalHashMap.put(kv._1, kv._2)
    this
  }

  override def -=(key: K): LRUCache.this.type = {
    internalHashMap.remove(key)
    this
  }

  override def get(key: K): Option[V] = {
    if (internalHashMap.containsKey(key)) Some(internalHashMap.get(key)) else None
  }

  override def iterator: Iterator[(K, V)] = {
    import collection.JavaConverters._
    internalHashMap.entrySet().asScala.map(entry => (entry.getKey, entry.getValue)).iterator
  }
}
