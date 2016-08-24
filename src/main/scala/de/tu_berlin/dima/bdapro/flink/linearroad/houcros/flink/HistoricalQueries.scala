package de.tu_berlin.dima.bdapro.flink.linearroad.houcros.flink

import java.nio.file.Paths
import java.util.Calendar

import es.houcros.linearroad.datasource.CarReportsSource
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by ilucero on 8/23/16.
  */
object HistoricalQueries {

  val tollHistoryFile = "datasets/tollHistory1Xway.dat"
  val tollHistory = loadTollHistory(tollHistoryFile) // key: (VID, Day, XWay), value: List[Tolls]

  private def loadTollHistory(tollHistoryFile: String): mutable.HashMap[(Int,Int,Int),ListBuffer[Int]] = {

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

    tollHistory
  }

  def accountBalance(reports: DataStream[Tuple15[Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int]],
                     tolls: mutable.Map[Int, Float]): DataStream[(Int,Int,Int,Int,Int,Float)] = {
    reports
      .filter(_._1 == 2) // only account balance queries
      .map(query => {
      // responseType, receive time, emit time, result time, query id, balance
      (2, query._1, ((Calendar.getInstance.getTimeInMillis - LinearRoad.startTime)/1000).toInt, -1, query._10, tolls(query._3))
    })
  }

  def dailyExpenditure(reports: DataStream[(Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int, Int)]):
  DataStream[(Int,Int,Int,Int,Int)] = {
    reports
      .filter(_._1 == 3) // only daily expenditure queries
      .map(query => {
      // responseType, receive time, emit time, result time, query id, balance
      (3, query._1, ((Calendar.getInstance.getTimeInMillis - LinearRoad.startTime)/1000).toInt,
        query._10, tollHistory((query._3, query._15, query._5)).sum) // last item is the list at key: (vid, day, xway)
    })
  }
}
