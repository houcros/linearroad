package de.tu_berlin.dima.bdapro.flink.linearroad.houcros.flink

import java.nio.file.Paths
import java.util.Calendar

import es.houcros.linearroad.datasource.CarReportsSource
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by ilucero on 8/23/16.
  */
object HistoricalQueries {

  private final val LOG = LoggerFactory.getLogger(HistoricalQueries.getClass.getSimpleName)

  val tollHistoryFile = "datasets/tollHistory1Xway.dat"
  val tollHistory = loadTollHistory(tollHistoryFile) // key: (VID, Day, XWay), value: List[Tolls]

  private def loadTollHistory(tollHistoryFile: String): mutable.HashMap[(Int,Int,Int),ListBuffer[Int]] = {

    LOG.info("Loading toll history")
    val tollHistory = mutable.HashMap[(Int,Int,Int),ListBuffer[Int]]()

    // Build toll history file resource path
    val uri = classOf[CarReportsSource[String]].getClassLoader.getResource(tollHistoryFile).toURI
    /*    Map<String, String> env = new HashMap<>();
        env.put("create", "true");
        FileSystem zipfs = FileSystems.newFileSystem(uri, env);*/
    val inputPath = Paths.get(uri).toString

    // Parse each line of the file and put it in the map
    for (line <- Source.fromFile(inputPath).getLines()) {
      val splitLine: Array[Int] = line.split(",", 4).map(_.toInt)
      val key = (splitLine(0), splitLine(1), splitLine(2))
      if (!(tollHistory contains key)) tollHistory.put(key, ListBuffer[Int]())
      tollHistory(key) += splitLine(3)
    }

    LOG.info("Toll history loaded")
    tollHistory
  }

  def accountBalance(reports: DataStream[Tuple15[Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int]]): DataStream[(Int,Int,Int,Int,Int,Float)] = {
    val tolls = TollManager.tolls
    reports
      .filter(_._1 == 2) // only account balance queries
      .map(query => {
      val balance = if (tolls contains query._3) tolls(query._3) else 0
      // responseType, receive time, emit time, result time, query id, balance
      (Utils.TYPE_HQ1, query._1, ((Calendar.getInstance.getTimeInMillis - LinearRoad.startTime)/1000).toInt, -1, query._10, balance)
    })
  }

  def dailyExpenditure(reports: DataStream[(Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)]):
  DataStream[(Int,Int,Int,Int,Int)] = {
    reports
      .filter(_._1 == 3) // only daily expenditure queries
      .map(query => {
      // sum of the list at key: (vid, day, xway), or 0 in no such key
      val expenditure = if (tollHistory contains (query._3, query._15, query._5)) tollHistory((query._3, query._15, query._5)).sum else 0
      // responseType, receive time, emit time, query id, expenditure
      (Utils.TYPE_HQ2, query._1, Utils.getCurrentRelativeTime(Calendar.getInstance.getTimeInMillis), query._10, expenditure)
    })
  }
}
