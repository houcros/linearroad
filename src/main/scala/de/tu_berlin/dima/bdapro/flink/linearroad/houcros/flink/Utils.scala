package de.tu_berlin.dima.bdapro.flink.linearroad.houcros.flink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  * Created by ilucero on 8/15/16.
  */
object Utils {

  /**
    * Calculates the number of vehicles per minute in an expressway, direction and segment.
    * @param reports The stream of reports and queries emitted by the vehicles.
    * @return A new stream with expressway, direction, segment and number of vehicles (in that order). Will emit every minute.
    */
  def numberOfVehicles(reports: DataStream[Tuple15[Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int]]): DataStream[(Int, Int, Int, Int)] ={

    reports.filter(_._1 == 0) // only position reports
      .map( rep => (rep._3, rep._5, rep._7, rep._8)) // project on vid, xway, direction, segment
      .keyBy(1, 2, 3) // key by location exc. line (xway, direction, segment)
      .timeWindow(Time.seconds(2*30)) // in one minute we can have either 0+ reports from each car
      .apply( (tuple, tw, iterable, collector: Collector[(Int, Int, Int, Int)]) => {

      val nov = iterable.groupBy(_._1).size // group by vid and count different vehicles
      val spl = iterable.head
      collector.collect(spl._2, spl._3, spl._4, nov)

    })(TypeInformation.of(classOf[(Int, Int, Int, Int)]))

  }

  /**
    * Calculates the latest average velocity in an expressway, direction and segment over the previous five minutes. Takes into account
    * that a car might emit more than one position report per segment.
    * @param reports The stream of reports and queries emitted by the vehicles.
    * @return A new stream with a tuple for each expressway, direction and segment, with the latest average velocity over the last
    *         five minutes. Will emit every minute.
    */
  def latestAverageVelocity(reports: DataStream[Tuple15[Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int]]): DataStream[(Int, Int, Int, Float)] ={

    reports.filter(_._1 == 0) // only position reports
      // REVIEW: is there a project method?
      .map( x => (x._3, x._4, x._5, x._7, x._8)) // project to vid, speed, xway, direction and segment
      .keyBy(2,3,4) // key by xway, direction, segment
      .timeWindow(Time.minutes(5), Time.minutes(1))
      .apply( (tup, timeWindow, iterable, collector: Collector[(Int, Int, Int, Float)]) => {

        var grandAvg: Float = 0 // the avg that we want to calculate, for all vehicles in a location
        var counter: Int = 0 // how many different cars, for the grand average
        val partialAvgs = mutable.Map[Int, (Float, Int)]() // per car, the accumulated average and the count, to calculate avg online

        iterable.foreach( reportAtPos => {
          val k = reportAtPos._1 // vid is the key
          if (partialAvgs contains k) {
            val aux = partialAvgs(k)
            // remove previously calculated avg speed corresponding to this car
            grandAvg -= aux._1
            // calculate new avg per car in an *online* fashion
            val carAvg = (aux._1 * aux._2) + reportAtPos._2 / (aux._2 + 1)
            val carReportsCount = aux._2 + 1
            partialAvgs(k) = (carAvg, carReportsCount)
            // add newly calculated avg speed corresponding to this car
            grandAvg += carAvg
          }
          else{
            counter += 1 // for the grand average we count every car just once (on first report)
            partialAvgs(k) = (reportAtPos._2, 1) // partial average equal to speed for first report from location, count of reports is 1
            grandAvg += reportAtPos._2 // save the tentative car average, until we find that it emits more reports from same location
          }
        })

        grandAvg /= counter // as we accumulated all the avg speeds for each car, just need to divide per number of cars
        val spl = iterable.head
        collector.collect(spl._3, spl._4, spl._5, grandAvg) // xway, direction, segment, avg velocity at this location

      })(TypeInformation.of(classOf[(Int, Int, Int, Float)]))

  }

}
