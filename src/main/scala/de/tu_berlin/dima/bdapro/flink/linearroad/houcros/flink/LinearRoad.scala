package de.tu_berlin.dima.bdapro.flink.linearroad.houcros.flink

import es.houcros.linearroad.datasource.CarReportsSource
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.core.fs.FileSystem
import scala.collection.mutable

/**
  * Created by houcros on 07/06/16.
  */
object LinearRoad {

  def main(args: Array[String]): Unit = {

    // Set-up
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val inputFile = "datasets/datafile20seconds.dat"
    val outputFile = "src/main/resources/output20seconds.dat"
    //val inputFile = "datasets/datafile3hours.dat"
    //val outputFile = "src/main/resources/output3hours.dat"

    // Get custom data source
    val dataStream = new CarReportsSource[Tuple15[Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int]](inputFile)
    val reports = env.addSource(dataStream)(TypeInformation.of(classOf[Tuple15[Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int]]))

    // ** ACCIDENT DETECTION **
    val accidentStream: DataStream[(Int, Int, Int)] = AccidentManager.accidentDetection(reports)
    //accidentStream.print()
    // ** ACCIDENT NOTIFICATION **
    // FIXME: currently also sending notifications for cars in the accident location (or 4 before) at the minute of
    // detection of the accident, but the notifications should start from the minute after!
    val accidentNotifications: DataStream[(Int, Int, Int, Int)] = AccidentManager.accidentNotifications(reports, accidentStream)
    // ASK: why wouldn't write with writeAsCsv?
    // Write to file
    accidentNotifications.writeAsText(outputFile, FileSystem.WriteMode.OVERWRITE)
    accidentNotifications.print()

    // **SEGMENT STATISTICS**
    // FIXME: currently gives for this minute, and I want for last minute!
    // Number of vehicles during minute prior to current minute
    val novStream = SegmentStatistics.numberOfVehicles(reports) // (x, d, s, nov)
    //novStream.print()
    // Average velocity
    // FIXME: I think same problem as above: need to get from previous 5 min, but I'm getting from this min and 4 before
    val lavStream = SegmentStatistics.latestAverageVelocity(reports) // (x, d, s, lav)
    //lavStream.print()
    // toll calculation per xway, direction and segment, and lav: (x, d, s, toll, lav)
    val tollCalculation = TollManager.tollCalculation(novStream, lavStream, accidentStream)


    // **TOLLS**
    // TODO: how to save this to disk?
    val tolls = mutable.Map[Int, Float]() // save sum of all the tolls assessed to car: vid, amount
                                          // WARNING: might need more granularity for historical queries, not aggregated
    val carCurrentState = mutable.Map[Int, (Int, Float)]() // vid, (segment, segmCharge)

    // REVIEW: do i need the lane at all?
    val tollNotificationsAndAssessments = TollManager.tollNotificationsAndAssessments(reports, tollCalculation, carCurrentState, tolls)
    // Write to file
    tollNotificationsAndAssessments.writeAsText(outputFile, FileSystem.WriteMode.OVERWRITE)
    tollNotificationsAndAssessments.print()

    // Execute
    env.execute("Linear Road")
  }

}
