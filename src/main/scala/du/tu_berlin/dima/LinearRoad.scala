package du.tu_berlin.dima.bdapro.flink.linearroad.houcros

import java.util.Calendar

import es.houcros.linearroad.datasource.CarReportsSource
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import scala.collection.mutable
import org.apache.flink.streaming.api.scala._
import org.apache.flink.core.fs.FileSystem

/**
  * Created by houcros on 07/06/16.
  */
object LinearRoad {

  def main(args: Array[String]): Unit = {

    // Set-up
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //val inputFile = "datafile20seconds.dat"
    val inputFile = "datafile3hours.dat"
    val outputFile = "src/main/resources/output3hours.dat"

    // Get custom data source
    val dataStream = new CarReportsSource[Tuple15[Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int]](inputFile)
    val reports = env.addSource(dataStream)(TypeInformation.of(classOf[Tuple15[Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int]]))

    // ** ACCIDENT DETECTION **
    val accidentStream: DataStream[(Int, Int, Int)] = accidentDetection(reports)
    //accidentStream.print()
    // ** ACCIDENT NOTIFICATION **
    // FIXME: currently also sending notifications for cars in the accident location (or 4 before) at the minute of
    // detection of the accident, but the notifications should start from the minute after!
    val accidentNotifications: DataStream[(Int, Int, Int, Int)] =
      reports
        .join(accidentStream)
        .where(rep => (rep._5, rep._6, rep._8))
        .equalTo(acc => (acc._1, acc._2, acc._3)) // REVIEW: WTH do I need to do this hack? Why it doesn't just work with (_)
        .window(TumblingEventTimeWindows.of(Time.seconds(30)))
        .apply( (report, accLoc, collector: Collector[(Int, Int, Int, Int)]) => {
          // ASK: how to get the current event minute, instead of the processor minute?
          val currentSecond = Calendar.getInstance().get(Calendar.SECOND)
          // type, receive time, emit time, segment [, car]
          collector.collect((1, report._2, currentSecond, accLoc._3))
        })
    // ASK: why wouldn't write with writeAsCsv?
    // Write to file
    accidentNotifications.writeAsText(outputFile, FileSystem.WriteMode.OVERWRITE)
    accidentNotifications.print()

    // **SEGMENT STATISTICS**
    // FIXME: currently gives for this minute, and I want for last minute!
    // Number of vehicles during minute prior to current minute
    val novStream = numberOfVehicles(reports) // (x, d, s, nov)
    //novStream.print()
    // Average velocity
    // FIXME: I think same problem as above: need to get from previous 5 min, but I'm getting from this min and 4 before
    val lavStream = latestAverageVelocity(reports) // (x, d, s, lav)
    //lavStream.print()
    // toll calculation per xway, direction and segment, and lav: (x, d, s, toll, lav)
    val tollCalculation = novStream
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


    // **TOLLS**
    // TODO: how to save this to disk?
    val tolls = mutable.Map[Int, Float]() // save sum of all the tolls assessed to car: vid, amount
                                          // WARNING: might need more granularity for historical queries, not aggregated
    val carCurrentState = mutable.Map[Int, (Int, Float)]() // vid, (segment, segmCharge)

    // REVIEW: do i need the lane at all?
    val tollNotificationsAndAssessments = reports
      .map(rep => (rep._3, rep._5, rep._6, rep._7, rep._8, rep._2)) // project on vid, xway, lane, direction, segment, timestamp
      .keyBy(0) // key by vid
      .join(tollCalculation) // join on xway, direction and segment
      .where(lf => (lf._2, lf._4, lf._5))
      .equalTo(rg => (rg._1, rg._2, rg._3))
        .window(TumblingEventTimeWindows.of(Time.seconds(30)))
        .apply( (tollNot, tollCalc, collector: Collector[(Int, Int, Int, Int, Float, Int)]) => { // the join will be 1 to 1, as there is one (or none) report per car in 30 sec, and one toll calculation per location
          // TODO: also send toll notification! (write)
          // FIXME: this is not the right second (should be event time)
          val currentSecond = Calendar.getInstance().get(Calendar.SECOND)
          val ret = (0, tollNot._1, tollNot._6, currentSecond, tollCalc._5, tollCalc._4)
          val vid = tollNot._1
          // If not in carCurrentState, save segment and segment charge for this car
          // REVIEW: maybe can compact the if's (but less legible...)
          if (!(carCurrentState contains vid)) {
            carCurrentState(vid) = (tollCalc._3, tollCalc._4) // (newSegment, newToll)
            if(!(tolls contains vid)) tolls(vid) = 0 // if it's the first time we see this car, we create an entry in the toll history
            // Emit toll notification
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
              collector.collect(ret) // (type, vid, receive time, emit time, lav, toll)
            }
          }
        })
    // Write to file
    tollNotificationsAndAssessments.writeAsText(outputFile, FileSystem.WriteMode.OVERWRITE)
    tollNotificationsAndAssessments.print()

    /*
    // ASK: why do i need a window after every join?
    // REVIEW: do i need the lane at all? (not used in the join)
    val tollAssessments = reports
        .filter(_._6 != 4) // if the car is in the exit lane, we won't charge him the toll
        .map(rep => (rep._3, rep._5, rep._6, rep._7, rep._8)) // project on vid, xway, lane, direction, segment
        .keyBy(0) // key by vid
        .timeWindow(Time.seconds(2*30), Time.seconds(30)) // 2 rounds of reports, sliding every 1 round
        .reduce( (rep1, rep2) => { // reducer will be executed just once per car: max two reports from a car in 2 rounds of report (60 sec)
          (rep1._1, rep1._2, rep1._3, rep1._4, rep1._5 - rep2._5) // if the reports are from the same segment, the rest will be 0
        })
        .filter(_._5 != 0) // keep only the ones that changed segment, i.e. the different of segments != 0
        .map(tollAsses => { // charge toll to account, actually return nothing
          val vid = tollAsses._1
          // It needs to be on this table, as the notification must be sent before the assessment (an therefore saved in carCurrentState)
          tolls(vid) += carCurrentState(vid)._2
          return
        })
      */

    // Execute
    env.execute("Linear Road")
  }


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
