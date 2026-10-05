/*
 * Copyright 2020 University of California, Riverside
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.edu.whu.spatialJoin.common

import cn.edu.whu.spatialJoin.geom.IFeature
import cn.edu.whu.spatialJoin.geom.util.GeometryHelper
import org.apache.spark.util.AccumulatorV2

class SummaryAccumulator(sizeFunction: IFeature => Int = _.getHeapEstimateBytes) extends AccumulatorV2[IFeature, Summary] {
  /**The internal summary value*/
  val summary = new Summary

  override def isZero: Boolean = summary.isEmpty

  override def copy(): AccumulatorV2[IFeature, Summary] = {
    val other = new SummaryAccumulator(sizeFunction)
    other.summary.setCoordinateDimension(this.summary.getCoordinateDimension)
    other.summary.expandToSummary(this.summary)
    other
  }

  override def reset(): Unit = summary.setEmpty()

  override def add(f: IFeature): Unit = {
    if (summary.getCoordinateDimension == 0)
      summary.setCoordinateDimension(GeometryHelper.getCoordinateDimension(f.getGeometry))
    val size: Int = sizeFunction(f)
    summary.expandToGeometryWithSize(f.getGeometry, size)
  }

  override def merge(other: AccumulatorV2[IFeature, Summary]): Unit = {
    val otherAcc = other.asInstanceOf[SummaryAccumulator]
    if (this.summary.isEmpty)
      this.summary.setCoordinateDimension(otherAcc.summary.getCoordinateDimension)
    this.summary.expandToSummary(otherAcc.summary)
  }

  override def value: Summary = {
    summary
  }
}
