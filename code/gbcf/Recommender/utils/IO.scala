package lsc.exam.diTo.utils

import java.util.concurrent.TimeUnit

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object IO {

    // Private attributes
    private val DataRoot   = "hdfs:///home/user30/Exam/datasets/"
    private val OutputRoot = "hdfs:///home/user30/Exam/outputs/"

    // Public attributes
    val Als = "als.csv"
    val Gbcf = "gbcf.csv"
    val Ibcf = "ibcf.csv"

    // Private methods
    private def dateDifference(totalMillis: Long): String = {
        val elapsedDays    = TimeUnit.MILLISECONDS.toDays(totalMillis)
        val elapsedHours   = TimeUnit.MILLISECONDS.toHours(totalMillis) % 24
        val elapsedMinutes = TimeUnit.MILLISECONDS.toMinutes(totalMillis) % 60
        val elapsedSeconds = TimeUnit.MILLISECONDS.toSeconds(totalMillis) % 60
        val elapsedMillis  = totalMillis % 1000

        "\nElapsed time: %dd %dH %dm %ds %dms".format(
            elapsedDays, elapsedHours, elapsedMinutes,
            elapsedSeconds, elapsedMillis)
    }

    // Public methods
    def parallelizeCsv(ss: SparkSession, filePath: String,
                       isData: Boolean = false, header: Boolean = true,
                       delimiter: String = ","): RDD[Row] = {
        val absFilePath = "%s%s".format(if (isData) DataRoot
                else OutputRoot, filePath)

        ss.read.format("csv")
            .option("header", header.toString)
            .option("delimiter", delimiter)
            .option("mode", "DROPMALFORMED")
            .option("inferSchema", "true")
            .load(absFilePath).rdd
    }

    def timeIt[A](f: => A): A = {
        val startTime   = System.nanoTime
        val outputValue = f

        println(dateDifference(((System.nanoTime
            - startTime) / 1e6).toLong))

        outputValue
    }

}