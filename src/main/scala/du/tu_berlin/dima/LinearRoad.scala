package du.tu_berlin.dima.bdapro.flink.linearroad.houcros

import es.houcros.linearroad.datasource.CarReportsSource
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.aggregation.AggregationFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{AllWindowedStream, StreamExecutionEnvironment, DataStream}
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable

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
    val dataStream = new CarReportsSource[Array[Int]](inputFile)
    val reports: DataStream[Array[Int]] = env.addSource(dataStream)(TypeInformation.of(classOf[Array[Int]]))

    // Data transformations
    /*
    val b: AllWindowedStream[Array[Int], TimeWindow] = reports.windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))
    b.apply( (timeWindow, iterable, collector: Collector[String]) => {
      var out = "Window: " + String.valueOf(timeWindow.getStart) + "\n"
      for (report <- iterable) {
        for (reportItem <- report) {
          out += reportItem + " "
        }
        out += "\n"
      }
      collector.collect(out)
    })(TypeInformation.of(classOf[String]))
      .print()
    */

    // **ACCIDENT DETECTION**
    val accidentStream: DataStream[Int] = reports.timeWindowAll(Time.seconds(4*30), Time.seconds(1*30)) // reports are every 30 seconds
      .apply( (timeWindow: TimeWindow, iterable, collector: Collector[Int]) => {

      // (vid, count): number of stopped reports per car in the window interval
      val stoppedCounts = mutable.Map[Int, Int]()
      // (vid, position): if a vehicle is stop at least once in the window interval, save the first position where it stopped
      val stoppedPositions = mutable.Map[Int, Int]()

      iterable filter(_(3) == 0) foreach { report => { // keep only the stopped cars (speed == 0)
      val vid = report(2)
        if (!(stoppedCounts contains vid)){ // first report stopped so put it in stoppedCars and save position
          stoppedCounts(vid) = 1
          stoppedPositions(vid) = report(8) // position
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
    //accidentStream.print()


    // **SEGMENT STATISTICS**
    // WARNING: fix, currently gives for this minute, and I want for last minute!
    // Number of vehicles during minute prior to current minute
    val numberOfVehicles: DataStream[Int] = reports.timeWindowAll(Time.minutes(1)) // in one minute we can have either 1 or 2 reports from each car
      .apply( (timeWindow: TimeWindow, iterable, collector: Collector[Int]) => {

        val nov = iterable.groupBy(_(2)).size
        collector.collect(nov)

    })(TypeInformation.of(classOf[Int]))
    //numberOfVehicles.print()

    // Average velocity
    // TODO: LAV

    // Execute
    env.execute("Linear Road")
  }
}
