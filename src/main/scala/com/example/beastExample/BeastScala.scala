package com.example.beastExample

import edu.ucr.cs.bdlab.beast.cg.SpatialJoinAlgorithms.{ESJDistributedAlgorithm, ESJPredicate}
import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.locationtech.jts.geom.{Envelope, GeometryFactory}
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import java.util.TimeZone
import java.util.Locale

/** Scala examples for Beast
  */
object BeastScala {
  def main(args: Array[String]): Unit = {
    // Initialize Spark context

    val conf = new SparkConf().setAppName("Beast Example")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)

    val sparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext

    try {
      // Import Beast features
      import edu.ucr.cs.bdlab.beast._

      val chicagoCommArea = sparkContext.shapefile("Boundaries_Community_Areas.zip")
      // Init dataset
      val crimes = sparkContext.readWKTFile("Chicago_Crimes_data_index.csv", 0, delimiter = '\t', skipHeader = true)

      // display the top 20 most frequent type of crime
      val typeCrimes = crimes.map(f => (f.getAs[String](6), 1))
      val topTypeCrimes = typeCrimes.countByKey()
      topTypeCrimes.toArray.sortBy(-_._2).take(20).foreach(item => { println(s"Type: ${item._1}, Count: ${item._2}") })

      // sort the commit time of theft in Chicago
      val dateParser = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a", Locale.US)
      // dateParser.setTimeZone(TimeZone.getTimeZone("GMT-5"))
      val burglary = crimes.filter(_.getAs[String](6).contains("BURGLARY"))
      val burglaryByHour = burglary
        .map(crime => {
          val crimeTime: Date = dateParser.parse(crime.getAs[String](3))
          val cal: Calendar = Calendar.getInstance()
          // cal.setTimeZone(TimeZone.getTimeZone("GMT-5"))
          cal.setTime(crimeTime)
          val crimeHour = cal.get(Calendar.HOUR_OF_DAY)
          (crimeHour, 1)
        })
        .countByKey()

      burglaryByHour.toArray.sortBy(-_._2).map(item => { println(s"Hour: ${item._1}, Count: ${item._2}") })

      // Output an DaVinci Visualization page that shows the top 5 most crime committed community
      val topCrimeComm = chicagoCommArea
        .spatialJoin(crimes)
        .mapValues(_ => 1L)
        .reduceByKey(_ + _)
        .sortBy(-_._2)
        .take(5)
        .map(_._1)

      sparkContext
        .parallelize(topCrimeComm)
        .plotPyramid("TopCrimeComm", 12, opts = "mercator" -> true)

    } finally {
      sparkSession.stop()
    }
  }
}
