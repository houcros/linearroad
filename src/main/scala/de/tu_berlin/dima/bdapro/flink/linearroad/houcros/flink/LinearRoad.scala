package de.tu_berlin.dima.bdapro.flink.linearroad.houcros.flink

import java.util.{Calendar, Properties}

import es.houcros.linearroad.datasource.CarReportsSource
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.slf4j.LoggerFactory

import scala.util.Try

/**
  * Created by houcros on 07/06/16.
  *
  * Entry point class for the application. Will set up the environment, instantiate the custom data source, build the needed streams
  * form the other classes and start the job.
  */
object LinearRoad {

  /* We need to save the time when the work actually started to be able to include the emission time of some tuples */
  final var startTime: Long = _

  /* Path to the properties file */
  val SETUP_PROPERTIES_PATH: String = "/setup.properties"
  /* Decide if we want to print the results to stdout. In general we just want to output the results to a file. */
  final var WRITE_TO_STDOUT: Boolean = _
  /* Location of the input file with the reports for the data driver */
  final var INPUT_FILE: String = _
  /* Where to write the results */
  final var OUTPUT_FILE: String = _

  /* The logger */
  private final val LOG = LoggerFactory.getLogger(LinearRoad.getClass.getSimpleName)

  /**
    * Just the main method.
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    LOG.info("LINEAR ROAD starting!")

    // Load setup properties
    LOG.info("Preparing to load setup properties...")
    loadProperties()

    // Set-up environment
    LOG.info("Setting up environment.")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // Create custom data source
    LOG.info("Creating reports data source.")
    val dataStream = new CarReportsSource[Tuple15[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]](INPUT_FILE)
    val reports = env.addSource(dataStream)(TypeInformation.of(classOf[Tuple15[Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int]]))

    /* ---------- ACCIDENT DETECTION ---------- */
    LOG.info("Creating accident detection and notification handlers.")
    /* This stream represents the positions (xway, direction, segment) affected by an accident. That is, either a position where
     * an accident happened or a position up to 4 segments "behind" a position with an accident. As the elements of the stream are
     * positions, they're of the form (xway, direction, segment). */
    val accidentStream: DataStream[(Int, Int, Int)] = AccidentManager.accidentDetection(reports)

    /* ---------- ACCIDENT NOTIFICATION ---------- */
    // FIXME: currently also sending notifications for cars in the accident location (or 4 before) at the minute of detection of the accident, but the notifications should start from the minute after!
    /* This stream represents the notifications emitted to cars in a position affected by an accident. The elements of the stream
     * are of the form (type, receive time, emit time, segment) */
    val accidentNotifications: DataStream[(Int, Int, Int, Int)] = AccidentManager.accidentNotifications(reports, accidentStream)
    val stringedAccidentNotifications: DataStream[String] = accidentNotifications.map(_.toString.replaceAll("[)(]",""))
    if (WRITE_TO_STDOUT) accidentNotifications.print

    /* ---------- SEGMENT STATISTICS ---------- */
    LOG.info("Creating segment statistics calculators.")
    // FIXME: currently gives for this minute, and I want for last minute!
    /* Number of vehicles stream. This stream represents the number of vehicles during the minute prior to current minute.
     * Each element of the stream is of the form (xway, direction, segment, number of vehicles) */
    val novStream = SegmentStatistics.numberOfVehicles(reports)
    // FIXME: I think same problem as above: need to get from previous 5 min, but I'm getting from this min and 4 before
    /* Latest average velocity stream. This stream represents the average velocity of all the cars that went through a given
     * segment during the last 5 minutes. Each element of the stream is of the form (xway, direction, segment, latest average velocity) */
    val lavStream = SegmentStatistics.latestAverageVelocity(reports)

    /* ---------- TOLL NOTIFICATIONS AND ASSESSMENTS ---------- */
    LOG.info("Creating toll notifications and assessments streams.")
    /* This stream represents the toll that costs every position on an expressway, and also give the latest average velocity in that
     * position. Each element of the stream is of the form (xway, direction, toll, latest average velocity) */
    val tollCalculation = TollManager.tollCalculation(novStream, lavStream, accidentStream)
    // REVIEW: do i need the lane at all?
    /* This stream takes care of updating the total amount that each car is to be charged (toll assessment), as well of notifying the
     * cars of the amount that will be charged for the segment that they just entered (toll notification). Each element of the stream
     * is of the form (type=0, vid, receive time, emit time, latest average velocity, toll to be charged) */
    val tollNotificationsAndAssessments = TollManager.tollNotificationsAndAssessments(reports, tollCalculation)
    val stringedTollNotificationsAndAssessments: DataStream[String] = tollNotificationsAndAssessments.map(_.toString.replaceAll("[)(]",""))
    if (WRITE_TO_STDOUT) tollNotificationsAndAssessments.print()

    /* ---------- HQ1: ACCOUNT BALANCE ---------- */
    LOG.info("Creating HQ1 (account balance) handler.")
    /* This stream handles requests for a historical query of type "account balance". That is, a car requests to know how
     * much he ows since the beginning of the simulation. Each element of the stream is of the form
     * (type=2, receive time, emit time, result time, query id, balance) */
    val accountBalanceStream = HistoricalQueries.accountBalance(reports)
    val stringedAccountBalanceStream: DataStream[String] = accountBalanceStream.map(_.toString.replaceAll("[)(]",""))
    if (WRITE_TO_STDOUT) accountBalanceStream.print

    /* ---------- HQ2: DAILY EXPENDITURE ---------- */
    LOG.info("Creating HQ2 (daily expenditure) handler.")
    /* This stream handles requests for a historical query of type "daily expenditure". That is, a car requests to know how much
     * he spent on a given position for a given day. Each element of the stream is of the form
     * (type=3, receive time, emit time, result time, query id, expenditure) */
    val dailyExpenditureStream = HistoricalQueries.dailyExpenditure(reports)
    val stringedDailyExpenditureStream: DataStream[String] = dailyExpenditureStream.map(_.toString.replaceAll("[)(]",""))
    if (WRITE_TO_STDOUT) dailyExpenditureStream.print

    // Join all streams and write to shared file
    val resultStream = stringedAccidentNotifications
      .union(stringedTollNotificationsAndAssessments)
      .union(stringedAccountBalanceStream)
      .union(stringedDailyExpenditureStream)
    resultStream.writeAsText(OUTPUT_FILE, FileSystem.WriteMode.OVERWRITE).setParallelism(1)

    // Execute
    LOG.info("Initializing the startTime")
    startTime = Calendar.getInstance.getTimeInMillis
    LOG.info("Executing the job chain.")
    env.execute("Linear Road")
  }

  private def loadProperties() = {

    LOG.info("Loading setup properties.")

    // Load properties file
    val props = new Properties
    val inputStream = getClass.getResourceAsStream(SETUP_PROPERTIES_PATH)
    props.load(inputStream)

    // Init properties
    INPUT_FILE = props.getProperty("linearroad.file.input", "datasets/datafile20seconds.dat")
    OUTPUT_FILE = props.getProperty("linearroad.file.output", "src/main/resources/output20seconds.dat")
    WRITE_TO_STDOUT = Try(props.getProperty("linearroad.output.stdout", "false").toBoolean).getOrElse(false)

    LOG.info("Setup properties loaded.")

  }

}
