package lsc.exam.diTo

import lsc.exam.diTo.models.GBCF
import lsc.exam.diTo.utils.IO

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Predictor {

    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf()
            .setAppName("GBCF Predictor")

        val ss = SparkSession.builder()
            .config(sparkConf).getOrCreate()

        val sc = ss.sparkContext
        sc.setLogLevel("ERROR")

        import ss.sqlContext.implicits._

        try {
            IO.timeIt {
                val gbcf = new GBCF(ss, csvMovies = IO.AvgRatings,
                    csvUsers = IO.UserFavGenres, csvTest = IO.Test)

                val moviesRecommends = gbcf.makeRecommendations()

                val gbcfAccuracy = moviesRecommends._1
                println(("\nGBCF model recommendations done!\n" +
                    "Test accuracy: %.3f\n").format(gbcfAccuracy))

                val dfMoviesPreds = moviesRecommends._2
                    .toDF("userid", "recommendations")
                    .sort($"userid".asc)

                // Show first 50 predictions
                dfMoviesPreds.show(50, truncate = false)
                // Save predictions to CSV file on HDFS
                IO.writePredsToCsv(dfMoviesPreds, "gbcf.csv")
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
