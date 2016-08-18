package de.tu_berlin.dima.bdapro.flink.linearroad.houcros.flink

import java.util.Calendar

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  * Created by ilucero on 8/15/16.
  */
object TollManager {

  def tollCalculation(novStream: DataStream[(Int, Int, Int, Int)],
                      lavStream: DataStream[(Int, Int, Int, Float)],
                      accidentStream: DataStream[(Int, Int, Int)]): DataStream[(Int, Int, Int, Int, Float)] ={

    novStream
      .join(lavStream) // join on xway, direction and segment
      .where(lf => (lf._1, lf._2, lf._3))
      .equalTo(rg => (rg._1, rg._2, rg._3))
      .window(TumblingEventTimeWindows.of(Time.seconds(1*30)))
      .apply((novTuple, lavTuple) => {
        if (novTuple._4 > 50 && lavTuple._4 < 40) // nov > 50 and lav < 40
          (novTuple._1, novTuple._2, novTuple._3, 2 * (novTuple._4 - 50)^2, lavTuple._4) // (x, d, s, toll, lav)
        else
          (novTuple._1, novTuple._2, novTuple._3, 0, lavTuple._4) // (x, d, s, toll, lav)
      })
      .coGroup(accidentStream) // co-group (to achieve outer join) on xwy, direction and segment
      .where(lf => (lf._1, lf._2, lf._3))
      .equalTo(rg => (rg._1, rg._2, rg._3))
      .window(TumblingEventTimeWindows.of(Time.seconds(1*30)))
      // the co-group will be 1-1 or 1-0 (for each unique tentative toll, there is 0 or 1 accident there)
      .apply((tentativeTolls, accidentLocations, collector: Collector[(Int, Int, Int, Int, Float)]) => {
      // First element of tentativeTolls (tentativeTolls.toList.head) should always exist! (cause it's a super set of accidentLocations)
      // FIXME: for some reason tentativeTolls is empty! It shouldn't: we should be calculating tolls for all locations where there are cars, and if there is an accident in a location then there are cars there!
      if (tentativeTolls.nonEmpty) {
        val tentativeToll = tentativeTolls.toList.head
        if (accidentLocations.isEmpty) collector.collect(tentativeToll) // no accident at this location, so tentative is definitive
        // Else, accident at this location, so toll should be 0
        collector.collect(tentativeToll.copy(_4 = 0))
      }
    })

  }

  def tollNotificationsAndAssessments(reports: DataStream[Tuple15[Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int]],
                                      tollCalculation: DataStream[(Int, Int, Int, Int, Float)],
                                      carCurrentState: mutable.Map[Int, (Int, Float)],
                                      tolls: mutable.Map[Int, Float]): DataStream[(Int, Int, Int, Int, Float, Int)] ={

    reports
      .map(rep => (rep._3, rep._5, rep._6, rep._7, rep._8, rep._2)) // project on vid, xway, lane, direction, segment, timestamp
      .keyBy(0) // key by vid
      .join(tollCalculation) // join on xway, direction and segment
      .where(lf => (lf._2, lf._4, lf._5))
      .equalTo(rg => (rg._1, rg._2, rg._3))
      .window(TumblingEventTimeWindows.of(Time.seconds(30)))
      .apply( (tollNot, tollCalc, collector: Collector[(Int, Int, Int, Int, Float, Int)]) => { // the join will be 1 to 1, as there is one (or none) report per car in 30 sec, and one toll calculation per location
      // TODO: also send toll notification! (write)
      // FIXME: this is not the right second (should be event time)
        val retProv = (0, tollNot._1, tollNot._6, -1, tollCalc._5, tollCalc._4) // provisional return tuple, to be updated with emit time
        val vid = tollNot._1
        // If not in carCurrentState, save segment and segment charge for this car
        if (!(carCurrentState contains vid)) {
          carCurrentState(vid) = (tollCalc._3, tollCalc._4) // (newSegment, newToll)
          if(!(tolls contains vid)) tolls(vid) = 0 // if it's the first time we see this car, we create an entry in the toll history
          // Emit toll notification
          val ret = (retProv._1, retProv._2, retProv._3,
              ((Calendar.getInstance.getTimeInMillis - LinearRoad.startTime)/1000).toInt, retProv._5, retProv._6)
          collector.collect(ret) // (type, vid, receive time, emit time, lav, toll)
        }
        else{
          val oldCharge = carCurrentState(vid)._2
          if (carCurrentState(vid)._1 != tollCalc._3){ // if it changed segment, we need to recalculate the new charge for the new segment
            carCurrentState(vid) = (tollCalc._3, tollCalc._4) // (newSegment, newToll)
            // Also, charge toll for the segment that just left, as it changed (if not in exit lane!)
            if (tollNot._3 != 4) tolls(vid) += oldCharge
            // FIXME: then don't need tollAssessment!!!
            // Emit toll notification
            val ret = (retProv._1, retProv._2, retProv._3,
              ((Calendar.getInstance.getTimeInMillis - LinearRoad.startTime)/1000).toInt, retProv._5, retProv._6)
            collector.collect(ret) // (type, vid, receive time, emit time, lav, toll)
          }
        }
      })

  }

}
