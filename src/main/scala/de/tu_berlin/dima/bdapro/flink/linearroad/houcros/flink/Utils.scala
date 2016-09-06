package de.tu_berlin.dima.bdapro.flink.linearroad.houcros.flink

/**
  * Created by ilucero on 9/6/16.
  */
object Utils {

  final val TYPE_TOLL_NOTIFICATION = 0
  final val TYPE_ACCIDENT_NOTIFICATION = 1
  final val TYPE_HQ1 = 2
  final val tYPE_HQ2 = 3

  def getCurrentRelativeTime(absoluteTime: Long): Int = {
    return ((absoluteTime - LinearRoad.startTime)/1000).toInt
  }

}
