package lsc.exam.diTo.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object Data {

    def dummyPredictions(ss: SparkSession): RDD[Row] = {
        // RDD schema: userid, recommendations
        ss.sparkContext.parallelize(Array(
            Row(1, "5:5.0|18:4.5|130:4.35|65:4.05|180:1.9"),
            Row(2, "18:5.0|130:4.5|5:4.35|100:3.23|65:1.9"),
            Row(3, "180:5.0|65:4.5|100:4.35|18:3.23|5:1.9")
        ))
    }

    def dummyTitles(ss: SparkSession): RDD[Row] = {
        // RDD schema: movieid, titles
        ss.sparkContext.parallelize(Array(
            Row(5,   "Pinocchio (2019)"),
            Row(18,  "Transformers (2007)"),
            Row(65,  "Pokemon (2010)"),
            Row(100, "Morto e sepolto (2012)"),
            Row(130, "Che tempo che fa? (2014)"),
            Row(180, "Quo vado (2016)")
        ))
    }

}
