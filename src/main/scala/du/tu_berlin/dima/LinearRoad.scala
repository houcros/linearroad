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
    val outoutFile = "src/main/resources/output3hours.dat"

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
      .equalTo(acc => (acc._1, acc._2, acc._3)) //REVIEW: WTH do I need to do this hack? Why it doesn't just work with (_)
      .window(TumblingEventTimeWindows.of(Time.seconds(30)))
      .apply( (report, accLoc, collector: Collector[(Int, Int, Int, Int)]) => {
        // ASK: how to get the current event minute, instead of the processor minute?
        val now = Calendar.getInstance()
        val currentMinute = now.get(Calendar.MINUTE)
        // type, receive time, emit time, segment [, car]
        collector.collect((1, report._2, currentMinute, accLoc._3))
      })
    //accidentNotifications.print()
    // ASK: why wouldn't write with write as csv?
    accidentNotifications.writeAsText(outoutFile, FileSystem.WriteMode.OVERWRITE)



    // **SEGMENT STATISTICS**
    // FIXME: currently gives for this minute, and I want for last minute!
    // Number of vehicles during minute prior to current minute
    val novStream: DataStream[Int] = numberOfVehicles(reports)
    //novStream.print()

    // Average velocity
    val lavStream: DataStream[(Int, Int, Int, Float)] = latestAverageVelocity(reports)
    //lavStream.print()

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
          stoppedPositions(vid) = (report._5, report._7, report._8) // and save location (exc. lane)
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

  def numberOfVehicles(reports: DataStream[Tuple15[Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int]]): DataStream[Int] ={

    reports.filter(_._1 == 0) // only position reports
      .timeWindowAll(Time.minutes(1)) // in one minute we can have either 1 or 2 reports from each car
      .apply( (timeWindow: TimeWindow, iterable, collector: Collector[Int]) => {

      val nov = iterable.groupBy(_._3).size // group by VID and count different vehicles
      collector.collect(nov)

    })(TypeInformation.of(classOf[Int]))

  }

  def latestAverageVelocity(reports: DataStream[Tuple15[Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int]]): DataStream[(Int, Int, Int, Float)] ={

    reports.filter(_._1 == 0) // only position reports
      .map( x => (x._3, x._4, x._5, x._7, x._8)) // keep vid, speed, xway, direction and segment
      .keyBy(2,3,4) // fields of position after mapping
      .timeWindow(Time.minutes(5), Time.minutes(1))
      .apply( (tup, timeWindow, iterable, collector: Collector[(Int, Int, Int, Float)]) => {

        var grandAvg: Float = 0
        var counter: Int = 0
        val partialAvgs = mutable.Map[Int, Float]() // partial avg velocity per car

        iterable.foreach( reportAtPos => {
          counter += 1
          val k = reportAtPos._1 // vid is the key
          if (partialAvgs contains k) {
            val aux = partialAvgs(k)
            grandAvg -= aux // remove what added before and add real avg speed of this car
            grandAvg += (aux + reportAtPos._2) / 2 // at most 2 reports per car per position
            partialAvgs remove k // clean up, since this key won't appear again
          }
          else{
            partialAvgs(k) = reportAtPos._2
            grandAvg += reportAtPos._2
          }
        })

        grandAvg /= counter
        val spl = iterable.head
        collector.collect(spl._3, spl._4, spl._5, grandAvg) // xway, direction, segment, avg velocity at this position

      })(TypeInformation.of(classOf[(Int, Int, Int, Float)]))

  }

}
