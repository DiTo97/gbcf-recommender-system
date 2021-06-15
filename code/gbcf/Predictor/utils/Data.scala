package lsc.exam.diTo.utils

import org.apache.commons.math3.util.Precision
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object Data {

    // Public methods
    def dummyMovies(ss: SparkSession): RDD[Row] = {
        // RDD schema: movieid, avgrating, genres
        ss.sparkContext.parallelize(Array(
            Row(1, 4.5, "Action|Drama|Romantic|"),
            Row(2, 3.0, "Romantic|Action|Drama|"),
            Row(3, 3.5, "Action|Romantic|"),
            Row(4, 5.0, "Drama|Romantic|"),
            Row(5, 3.0, "Action|Drama|"),
            Row(6, 4.0, "Comedy|Action|")
        ))
    }

    def dummyUsers(ss: SparkSession): RDD[Row] = {
        // RDD schema: userid, favgenres
        ss.sparkContext.parallelize(Array(
            Row(1, "Romantic|Drama|"),
            Row(2, "Action|"),
            Row(3, "Drama|"),
            Row(4, "Action|Comedy|")
        ))
    }

    def dummyTest(ss: SparkSession): RDD[Row] = {
        // RDD schema: userid, movieid, rating, timestamp, genres
        ss.sparkContext.parallelize(Array(
            Row(1, 5, 3.0, 543, "Action|Drama|"),
            Row(1, 4, 2.0, 600, "Drama|Romantic|"),
            Row(1, 6, 2.5, 750, "Comedy|Action|"),
            Row(2, 6, 4.0, 789, "Comedy|Action|"),
            Row(2, 2, 4.5, 900, "Romantic|Action|Drama|"),
            Row(2, 4, 2.0, 800, "Drama|Romantic|"),
            Row(3, 3, 3.5, 234, "Action|Romantic|"),
            Row(3, 4, 5.0, 400, "Drama|Romantic|"),
            Row(3, 2, 3.0, 950, "Romantic|Action|Drama|"),
            Row(4, 2, 1.0, 567, "Romantic|Action|Drama|"),
            Row(4, 4, 1.5, 635, "Drama|Romantic|"),
            Row(4, 6, 3.0, 789, "Comedy|Action|")
        ))
    }

    /**
     * Verifies rddInput schema validity and extracts useful information
     * towards the goal of the GBCF procedure
     */
    def normalizeRatings(rddInput: RDD[Row]): Option[RDD[Row]] = {
        try {
//            val maxRatings = rddInput
//                // Make (userid, rating) pairs
//                .map { row => (row.getInt(0), row.getDouble(2)) }
//                .reduceByKey { (acc, r) =>
//                    acc max r
//                }
//                .collectAsMap()

            // If the format is correct return the RDD...
            Some(rddInput.map{
                case Row(userId: Int, movieId: Int, oldRating: Double,
                _: Int, _: String) =>
//                    val trueScore = Precision.round((oldRating * 5)
//                        / maxRatings(userId), 3)

                    Row(userId, movieId, oldRating)
            })
        } catch {
            case _: Exception =>
                // ... else return None
                None
        }
    }

}
