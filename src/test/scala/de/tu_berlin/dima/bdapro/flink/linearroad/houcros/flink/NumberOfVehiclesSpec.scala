package de.tu_berlin.dima.bdapro.flink.linearroad.houcros.flink

import es.houcros.linearroad.datasource.CarReportsSource
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

import scala.reflect.io.File

/**
  * Created by ilucero on 8/16/16.
  */
class NumberOfVehiclesSpec extends FlatSpec with BeforeAndAfterAll {

  var result: Array[String] = _
  val inputFile = "testNov1.dat"
  val outputFile = "src/test/resources/outputTest1"

  /**
    * Common previous set up for all the tests to come
    */
  override protected def beforeAll(): Unit = {

    // Set up environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // Generate reports stream from file
    val dataStream = new CarReportsSource[Tuple15[Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int]](inputFile)
    val reports = env.addSource(dataStream)(TypeInformation.of(classOf[Tuple15[Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int,Int]]))

    // Generate number of vehicles stream (what we want to test) and write the stream to text
    val novStream = SegmentStatistics.numberOfVehicles(reports)
    novStream.writeAsText(outputFile, FileSystem.WriteMode.OVERWRITE).setParallelism(1)
    env.execute("SegmentStatisticsSpec Test")

    // Parse the number of vehicles stream to a List, so we can test it
    result = File(outputFile).slurp().split("\n")
  }

  "A number of vehicles calculation with the given input" should "emit count the number of vehicles 8 times" in {
    assert(result.length.equals(8))
  }
  it should "find 4 vehicles in the expressway 0, direction 0 and segment 0 during the minute 1" in {
    assert(result contains "(0,0,0,4)")
  }
  it should "find 2 vehicles in the expressway 0, direction 0 and segment 10 during the minute 1" in {
    assert(result contains "(0,0,10,2)")
  }
  it should "find 1 vehicle in the expressway 0, direction 0 and segment 20 during the minute 1" in {
    assert(result contains "(0,0,20,1)")
  }
  it should "find 2 vehicles in the expressway 0, direction 0 and segment 20 during the minute 2" in {
    assert(result contains "(0,0,20,2)")
  }
  it should "find 2 vehicles in the expressway 0, direction 0 and segment 30 during the minute 2" in {
    assert(result contains "(0,0,30,2)")
  }
  it should "find 1 vehicle in the expressway 0, direction 0 and segment 40 during the minute 2" in {
    assert(result contains "(0,0,40,1)")
  }
  it should "find 1 vehicle in the expressway 0, direction 0 and segment 60 during the minute 2" in {
    assert(result contains "(0,0,20,1)")
  }
  it should "find 1 vehicle in the expressway 0, direction 0 and segment 10 during the minute 2" in {
    assert(result contains "(0,0,10,1)")
  }
}