package cn.edu.whu.spatialJoin.algortithms

import cn.edu.whu.spatialJoin.geom.IFeature
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD

trait SpatialDataTypesMixin {

  /**A type alias for spatial RDDs*/
  type SpatialRDD = RDD[IFeature]


  /**A type alias for Java spatial RDDs*/
  type JavaSpatialRDD = JavaRDD[IFeature]

}

/**
 * Create an object out of the mixin to enable import of the data types
 */
object SpatialDataTypes extends SpatialDataTypesMixin
