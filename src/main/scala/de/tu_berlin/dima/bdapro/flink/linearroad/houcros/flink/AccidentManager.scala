package de.tu_berlin.dima.bdapro.flink.linearroad.houcros.flink

import java.util.Calendar

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  * Created by ilucero on 8/8/16.
  */
object AccidentManager {

  // Assuming that reports make sense: we cannot have two reports in a row from a car stopped (speed = 0) from different
  // locations (xway, lane, direction, position). This simplifies the calculation, as we only need to check if there are
  // four consecutive reports with speed = 0 to consider a car as stopped, but don't actually have to check that the four
  // reports are from the same position (four coordinates).
  def accidentDetection(reports: DataStream[Tuple15[Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int]]): DataStream[(Int, Int, Int)] ={

    reports.filter(_._1 == 0) // only position reports
      .filter(_._4 == 0) // only stopped cars (speed == 0)
      .timeWindowAll(Time.seconds(4*30), Time.seconds(1*30)) // reports are every 30 seconds
      .apply( (timeWindow: TimeWindow, iterable, collector: Collector[(Int, Int, Int)]) => {

      // (vid, count): number of stopped reports per car in the window interval
      val stoppedCounts = mutable.Map[Int, Int]()
      // (vid, (xway, direction, segment)): if a vehicle is stop at least once in the window interval, save the first
      // (xway, direction, segment) where it stopped; we don't need the lane, as all the segments in the lane are affected
      val stoppedPositions = mutable.Map[Int, (Int, Int, Int)]()

      iterable foreach { report => {
        val vid = report._3
        if (!(stoppedCounts contains vid)){ // first report stopped so
          stoppedCounts(vid) = 1            // put it in stoppedCars
          stoppedPositions(vid) = (report._5, report._7, report._8) // and save location (exc. lane): xway, direction, segment
        }
        else stoppedCounts(vid) += 1
      }}
      // Keep only POSITIONS with *actually* stopped cars (4 stop reports in a row)
      stoppedCounts foreach { kv => { if (kv._2 < 4) stoppedPositions remove kv._1 } }
      //println("stoppedPositions: " + stoppedPositions)
      // Count how many cars per position, accident only if 2+ cars in that position
      stoppedPositions groupBy(_._2) foreach { stopPosCount => {
        // Size of the list of cars in position = stopped cars in position
        if(stopPosCount._2.size > 1) collector.collect(stopPosCount._1)
      }}

    })(TypeInformation.of(classOf[(Int, Int, Int)]))
      // Trick: return also the 4 previous segments as accidents, so we can just join with the segments of the reports
      .flatMap( (accLoc, collector: Collector[(Int, Int, Int)]) => {
      // If negatives, join just won't happen
      collector.collect(accLoc)
      collector.collect(accLoc._1, accLoc._2, accLoc._3-1)
      collector.collect(accLoc._1, accLoc._2, accLoc._3-2)
      collector.collect(accLoc._1, accLoc._2, accLoc._3-3)
      collector.collect(accLoc._1, accLoc._2, accLoc._3-4)
    })
  }

  def accidentNotifications(reports: DataStream[Tuple15[Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int]],
                            accidentStream: DataStream[(Int, Int, Int)]): DataStream[(Int, Int, Int, Int)] ={
    reports
      .join(accidentStream)
      .where(rep => (rep._5, rep._6, rep._8))
      .equalTo(acc => (acc._1, acc._2, acc._3)) // REVIEW: WTH do I need to do this hack? Why it doesn't just work with (_)
      .window(TumblingEventTimeWindows.of(Time.seconds(30)))
      .apply( (report, accLoc, collector: Collector[(Int, Int, Int, Int)]) => {
        // ASK: how to get the current event minute, instead of the processor minute?
        val currentSecond = Calendar.getInstance().get(Calendar.SECOND)
        // type, receive time, emit time, segment [, car]
        collector.collect((Utils.TYPE_ACCIDENT_NOTIFICATION, report._2, currentSecond, accLoc._3))
      })
  }

}
