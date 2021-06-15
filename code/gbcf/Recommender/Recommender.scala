package lsc.exam.diTo

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

import scala.math.pow

// Custom imports
import lsc.exam.diTo.utils.{Data, IO}

object Recommender {

    // Implicit methods
    implicit class OpsNum(val str: String) extends AnyVal {
        def isInteger: Boolean = scala.util
            .Try(str.toInt).isSuccess

        def isNatural: Boolean = (str.isInteger
            && str.toInt > 0)
    }

    implicit class PowerInt(i: Int) {
        def ** (b: Int): Int = pow(i, b).intValue
    }

    // Private attributes
    private val PredictorType = "gbcf"
    private val UsageMsg = ("\nUsage is as follows: "
            + "<jar_file> <user_id> [predictor_type=gbcf]")

    private val Predictors = Array("gbcf", "ibcf", "als")

    // Private methods
    private def loadPredictions(ss: SparkSession,
                                hdfsPredType: String = null): RDD[Row] = {
        if (hdfsPredType == null)
            Data.dummyPredictions(ss)
        else IO.parallelizeCsv(ss, hdfsPredType + ".csv")
    }

    private def loadTitles(ss: SparkSession,
                           hdfsTitles: String = null): RDD[Row] = {
        if (hdfsTitles == null)
            Data.dummyTitles(ss)
        else IO.parallelizeCsv(ss, hdfsTitles
            + ".csv", isData = true)
    }

    private def deserializeScore(r: Row): (Int, Map[Int, Double]) = {
        val userId = r.getInt(0)
        val strRecommends = r.getString(1)

        val mapRecommends = strRecommends
            .split("\\|").map {
            s =>
                val t = s.split(":")

                val movieId = t(0).toInt
                val predScore = t(1).toDouble

                (movieId, predScore)
        }

        (userId, mapRecommends.toMap)
    }

    def main(args: Array[String]): Unit = {
        if (args.length == 0 || args.length > 2) {
            throw new IllegalArgumentException(UsageMsg)
        }

        var _userId = -1
        var _predictorType = PredictorType

        if (args(0).isNatural) {
            _userId = args(0).toInt
        } else {
            throw new NumberFormatException("user_id must be a positive Int")
        }

        if (args.length == 2) {
            _predictorType = args(1)

            if (!Predictors.contains(_predictorType)) {
                throw new IllegalArgumentException("Allowed predictor_type are: %s"
                    .format(Predictors.mkString(" ")))
            }
        }

        val sparkConf = new SparkConf()
            .setAppName("Recommender")

        val ss = SparkSession.builder()
            .config(sparkConf).getOrCreate()

        val sc = ss.sparkContext
        sc.setLogLevel("ERROR")

        try {
            IO.timeIt {
                val rddPredictions = loadPredictions(ss, _predictorType)
                val rddTitles = loadTitles(ss,
                    "titles")

                val userPreds = rddPredictions.filter {
                    r => r.getInt(0) == _userId
                }
                    .map(deserializeScore)
                    .flatMapValues(x => x)
                    .map {
                        case (_, movieInfo) =>
                            val movieId = movieInfo._1
                            val predScore = movieInfo._2

                            (movieId, predScore)
                    }
                    .join(rddTitles.map {
                        r => (r.getInt(0), r.getString(1))
                    })
                    .map {
                        case (_, movieInfo) =>
                            (movieInfo._2, movieInfo._1)
                    }.collect()

                if (userPreds.isEmpty) {
                    println(("\nNo recommendations available " +
                        "for user %d...").format(_userId))
                } else {
                    println("\nPredictions for user %d:".format(_userId))
                    userPreds.sortBy(_._2)(Ordering[Double]
                        .reverse).zipWithIndex.foreach(x =>
                        println("%2d.".format(x._2 + 1) + " "
                            + x._1._1.trim + ", " + x._1._2))
                }
            }
        } catch {
            case x: Exception =>
                x.printStackTrace()
        } finally {
            sc.stop()
            ss.close()
        }
    }

}
