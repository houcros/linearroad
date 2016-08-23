package de.tu_berlin.dima.bdapro.flink.linearroad.houcros.flink

import java.util.Calendar

import org.apache.flink.streaming.api.scala.DataStream

import scala.collection.mutable

/**
  * Created by ilucero on 8/23/16.
  */
object HistoricalQueries {

  def accountBalance(reports: DataStream[Tuple15[Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int]],
                     tolls: mutable.Map[Int, Float]): DataStream[(Int,Int,Int,Int,Int,Float)] = {
    reports
      .filter(_._1 == 2) // only account balance queries
      .map(query => {
      // responseType, receive time, emit time, result time, query id, balance
      (2, query._1, ((Calendar.getInstance.getTimeInMillis - LinearRoad.startTime)/1000).toInt, -1, query._10, tolls(query._3))
    })
  }
}
