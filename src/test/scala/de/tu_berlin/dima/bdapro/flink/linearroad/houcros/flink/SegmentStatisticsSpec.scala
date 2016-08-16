package de.tu_berlin.dima.bdapro.flink.linearroad.houcros.flink

import java.util.{Timer, TimerTask}

import es.houcros.linearroad.datasource.CarReportsSource
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.FlatSpec

import scala.reflect.io.File

/**
  * Created by ilucero on 8/16/16.
  */
class SegmentStatisticsSpec extends FlatSpec {

//  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val env = StreamExecutionEnvironment.createLocalEnvironment(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  class FileMinuteSplitter(path: String) extends TimerTask{
    var minute = 0
    var file = File(path)

    override def run(): Unit = {
//      File(path).appendAll(minute.toString)
//      new File(path)
      file.appendAll(minute.toString)
      minute += 1
    }
  }

  "A number of vehicles calculation" should "find 4 vehicles in the expressway 0, direction 0 and segment 0 during the minute 1" in {

    val inputFile = "testNov1.dat"
    val outputFile = "src/test/resources/outputTest1"
    val dataStream = new CarReportsSource[Tuple15[Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int]](inputFile)
    val reports = env.addSource(dataStream)(TypeInformation.of(classOf[Tuple15[Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int]]))

    val novStream = SegmentStatistics.numberOfVehicles(reports)
    novStream.writeAsText(outputFile, FileSystem.WriteMode.OVERWRITE)

    env.execute("Number of vehicles: test 1")

    val timer: Timer = new Timer()
    timer.schedule(new FileMinuteSplitter(outputFile), 60000, 60000)

    val timer2: Timer = new Timer()
    timer2.schedule(new FileMinuteSplitter("src/test/resources/testingTest.dat"), 0, 10000)
  }

}
