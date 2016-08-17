package de.tu_berlin.dima.bdapro.flink.linearroad.houcros.flink

import es.houcros.linearroad.datasource.CarReportsSource
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

import scala.reflect.io.File

/**
  * Created by ilucero on 8/17/16.
  */
class LatestAverageVelocitySpec extends FlatSpec with BeforeAndAfterAll {

  var result: Array[String] = _
  val inputFile = "testLav1.dat"
  val outputFile = "src/test/resources/outputTest2"

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
    val lavStream = SegmentStatistics.latestAverageVelocity(reports)
    lavStream.writeAsText(outputFile, FileSystem.WriteMode.OVERWRITE).setParallelism(1)
    env.execute("LatestAverageVelocitySpec Test")

    // Parse the number of vehicles stream to a List, so we can test it
    result = File(outputFile).slurp().split("\n")
  }

  "A latest average velocity with the given input" should "contain results for 2 expressway-direction-segment combinations" in {
    assert(result.length.equals(2))
  }
  it should "calculate an average velocity of 50 mph in the expressway 0, direction 0 and segment 10" in {
    assert(result contains "(0,0,0,50.0)")
  }
  it should "calculate an average velocity of 100 mph in the expressway 0, direction 0 and segment 10" in {
    assert(result contains "(0,0,0,100.0)")
  }
}
