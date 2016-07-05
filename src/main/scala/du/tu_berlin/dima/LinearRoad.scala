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
import scala.collection.mutable.ListBuffer

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

    // **ACCIDENT DETECTION**
    val accidentStream: DataStream[Int] = accidentDetection(reports)
    //accidentStream.print()

    // **SEGMENT STATISTICS**
    // TODO: fix, currently gives for this minute, and I want for last minute!
    // Number of vehicles during minute prior to current minute
    val nov: DataStream[Int] = numberOfVehicles(reports)
    //nov.print()

    // Average velocity
    val lav: DataStream[(Int, Int, Int, Float)] = latestAverageVelocity(reports)
    //lav.print()

    // Execute
    env.execute("Linear Road")
  }

  def accidentDetection(reports: DataStream[Array[Int]]): DataStream[Int] ={

    reports.filter(_(0) == 0) // only position reports
      .timeWindowAll(Time.seconds(4*30), Time.seconds(1*30)) // reports are every 30 seconds
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

  }

  def numberOfVehicles(reports: DataStream[Array[Int]]): DataStream[Int] ={

    reports.filter(_(0) == 0) // only position reports
      .timeWindowAll(Time.minutes(1)) // in one minute we can have either 1 or 2 reports from each car
      .apply( (timeWindow: TimeWindow, iterable, collector: Collector[Int]) => {

      val nov = iterable.groupBy(_(2)).size // group by VID and count different vehicles
      collector.collect(nov)

    })(TypeInformation.of(classOf[Int]))

  }

  def latestAverageVelocity(reports: DataStream[Array[Int]]): DataStream[(Int, Int, Int, Float)] ={

    reports.filter(_(0) == 0) // only position reports
      .timeWindowAll(Time.minutes(5), Time.minutes(1))
      .apply( (timeWindow: TimeWindow, iterable, collector: Collector[(Int, Int, Int, Float)]) => {

        val velsPerPosition = mutable.Map[(Int, Int, Int), ListBuffer[Float]]()
        // Position := expressway, direction and segment
        val positions = iterable.groupBy(rep => (rep(4): Int, rep(6): Int, rep(7): Int, rep(2): Int)) // group by expressway, direction and segment
        positions.foreach( reportsAtPosition => {
          // for each position calculate avg speed

          val avgVelocityPerCar: Float = reportsAtPosition._2.foldLeft(0)(_ + _(3)) / reportsAtPosition._2.size.toFloat
          val k = (reportsAtPosition._1._1, reportsAtPosition._1._2, reportsAtPosition._1._3)
          if (!(velsPerPosition contains k)){
            velsPerPosition(k) = ListBuffer[Float]()
          }
          velsPerPosition((reportsAtPosition._1._1, reportsAtPosition._1._2, reportsAtPosition._1._3)) += avgVelocityPerCar

          /*
          println{"reportsAtPosition: " + reportsAtPosition._1.toString()}
          reportsAtPosition._2 foreach(it => {print("vid: " + it(2) + " - speed: " + it(3) + " | ")})
          println("avgVelocityPerCar: " + avgVelocityPerCar.toString)
          println()
          */
        })

        velsPerPosition.foreach( velsAtPosition => {

          val avgVelocityPerPosition: Float = velsAtPosition._2.sum / velsAtPosition._2.size.toFloat
          collector.collect(velsAtPosition._1._1, velsAtPosition._1._2, velsAtPosition._1._3, avgVelocityPerPosition)
          /*
          println{"velsAtPosition: " + velsAtPosition._1.toString()}
          velsAtPosition._2 foreach(it => {print(it.toString + " | ")})
          println("\navgVelocityPerPosition: " + avgVelocityPerPosition.toString)
          */
        })
      })(TypeInformation.of(classOf[(Int, Int, Int, Float)]))
  }

}
