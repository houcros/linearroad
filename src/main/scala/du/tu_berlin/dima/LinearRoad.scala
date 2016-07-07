package du.tu_berlin.dima.bdapro.flink.linearroad.houcros

import es.houcros.linearroad.datasource.CarReportsSource
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import scala.collection.mutable
import org.apache.flink.streaming.api.scala._

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

    // Get custom data source
    val dataStream = new CarReportsSource[Tuple15[Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int]](inputFile)
    val reports = env.addSource(dataStream)(TypeInformation.of(classOf[Tuple15[Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int]]))

    // **ACCIDENT DETECTION**
    val accidentStream: DataStream[Int] = accidentDetection(reports)
    //accidentStream.print()

    // **SEGMENT STATISTICS**
    // TODO: fix, currently gives for this minute, and I want for last minute!
    // Number of vehicles during minute prior to current minute
    val novStream: DataStream[Int] = numberOfVehicles(reports)
    //novStream.print()

    // Average velocity
    val lavStream: DataStream[(Int, Int, Int, Float)] = latestAverageVelocity(reports)
    lavStream.print()

    // Execute
    env.execute("Linear Road")
  }

  // project to get rid of useless fields
  // input source: change to tuples
  // key by many fields possible with tuple
  // grouping to reduce vehicles

  def accidentDetection(reports: DataStream[Tuple15[Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int]]): DataStream[Int] ={

    reports.filter(_._1 == 0) // only position reports
      .timeWindowAll(Time.seconds(4*30), Time.seconds(1*30)) // reports are every 30 seconds
      .apply( (timeWindow: TimeWindow, iterable, collector: Collector[Int]) => {

      // (vid, count): number of stopped reports per car in the window interval
      val stoppedCounts = mutable.Map[Int, Int]()
      // (vid, position): if a vehicle is stop at least once in the window interval, save the first position where it stopped
      val stoppedPositions = mutable.Map[Int, Int]()

      iterable filter(_._4 == 0) foreach { report => { // keep only the stopped cars (speed == 0)
      val vid = report._3
        if (!(stoppedCounts contains vid)){ // first report stopped so put it in stoppedCars and save position
          stoppedCounts(vid) = 1
          stoppedPositions(vid) = report._9 // position
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

    })(TypeInformation.of(classOf[Int]))

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
