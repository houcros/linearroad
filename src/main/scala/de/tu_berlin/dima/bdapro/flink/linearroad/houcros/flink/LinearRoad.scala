package de.tu_berlin.dima.bdapro.flink.linearroad.houcros.flink

import java.util.Calendar

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.core.util.StatusPrinter
import es.houcros.linearroad.datasource.CarReportsSource
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.core.fs.FileSystem
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Created by houcros on 07/06/16.
  */
object LinearRoad {

  final var startTime: Long = _
  private final val LOG = LoggerFactory.getLogger(LinearRoad.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {

    LOG.info("LINEAR ROAD starting!")
//    StatusPrinter.print(LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext])

    // Set-up
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //val inputFile = "datasets/datafile20seconds.dat"
    //val outputFile = "src/main/resources/output20seconds.dat"
    val inputFile = "datasets/datafile3hours.dat"
    val outputFile = "src/main/resources/output3hours.dat"

    // Get custom data source
    LOG.info("Creating reports data source.")
    val dataStream = new CarReportsSource[Tuple15[Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int]](inputFile)
    val reports = env.addSource(dataStream)(TypeInformation.of(classOf[Tuple15[Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int]]))
    startTime = Calendar.getInstance.getTimeInMillis

    // ** ACCIDENT DETECTION **
    LOG.info("Creating accident detection and notification handlers.")
    val accidentStream: DataStream[(Int, Int, Int)] = AccidentManager.accidentDetection(reports)
    //accidentStream.print()
    // ** ACCIDENT NOTIFICATION **
    // FIXME: currently also sending notifications for cars in the accident location (or 4 before) at the minute of
    // detection of the accident, but the notifications should start from the minute after!
    val accidentNotifications: DataStream[(Int, Int, Int, Int)] = AccidentManager.accidentNotifications(reports, accidentStream)
    // ASK: why wouldn't write with writeAsCsv?
    // Write to file
    accidentNotifications.print()

    // **SEGMENT STATISTICS**
    LOG.info("Creating segment statistics calculators.")
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
    tollNotificationsAndAssessments.writeAsText(outputFile, FileSystem.WriteMode.OVERWRITE).setParallelism(1)
    tollNotificationsAndAssessments.print()

    // **HQ1: ACCOUNT BALANCE**
    LOG.info("Creating HQ1 (account balance) handler.")
    val accountBalanceStream = HistoricalQueries.accountBalance(reports, tolls)
    accountBalanceStream.writeAsText(outputFile, FileSystem.WriteMode.OVERWRITE).setParallelism(1)

    // **HQ2: DAILY EXPENDITURE**
    LOG.info("Creating HQ2 (daily expenditure) handler.")
    val dailyExpenditureStream = HistoricalQueries.dailyExpenditure(reports)
    dailyExpenditureStream.writeAsText(outputFile, FileSystem.WriteMode.OVERWRITE).setParallelism(1)

    // Execute
    LOG.info("Executing the job chain.")
    env.execute("Linear Road")
  }

}
