package lsc.exam.diTo.models

import org.apache.commons.math3.util.Precision
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

// Custom imports
import lsc.exam.diTo.utils.{IO, Data}

/*
 * Genre-based collaborative filtering
 * S. Choi, S. Ko, Y. Han, A movie recommendation algorithm based on genre correlations, 2012
 */
class GBCF(val ss: SparkSession, csvMovies: String = null,
           csvUsers: String = null, csvTest:String = null) extends Serializable {

    // Private attributes
    private val rddMovies =
        if (csvMovies == null)
            Data.dummyMovies(ss)
        else IO.parallelizeCsv(ss, csvMovies)

    private val rddGenresCorr = rddMovies.map {
        case Row(_: Int, _: Double, genres: String) =>
            genres.split("\\|")
                .filter(s => s != "IMAX").toVector
    }
        .flatMap { _.combinations(2) }
        .map { pair => (pair, 1) }
        .reduceByKey(_ + _)
        .map { case (genres, cnt) => (genres(0), (genres(1), cnt)) }
        .groupByKey()
        .map { case (genre, matches) =>
            val pairs = matches.toSet.unzip

            val genres = pairs._1
            val cnts = pairs._2

            val sum = cnts.sum
            val percentages = cnts.map(x => Precision.
                round(x.toDouble / sum, 3))

            val correls = genres.zip(percentages).toMap
            genre -> correls }

    private val rddUsers =
        if (csvUsers == null)
            Data.dummyUsers(ss)
        else IO.parallelizeCsv(ss, csvUsers)

    private var rddTest =
        if (csvTest == null)
            Data.dummyTest(ss)
        else IO.parallelizeCsv(ss, csvTest)

    // Private methods
    private def predictScores(X: RDD[(Int, (String,
        Iterable[Int]))], nMovies: Int):
                    RDD[(Int, Map[Int, Double])] = {
        val moviesData = rddMovies.map {
            case Row(movieId: Int, avgRating: Double, genres: String) =>
                movieId -> (genres, avgRating)
        }.collectAsMap()

        X.map {
            case (userId, moviesInfo) =>
                val favGenres = moviesInfo._1
                val moviesIds = moviesInfo._2.toVector

                var recommMovies = moviesIds.map { movieId =>
                    val recommScore = recommendationScore(favGenres,
                        moviesData(movieId))

                    (movieId, recommScore)
                }.sortBy(_._2)(Ordering[Double]
                    .reverse).take(nMovies)

//                val highestScore = recommMovies(0)._2 / 5.0

                recommMovies = recommMovies.map{case (movieId, recommScore) =>
                    (movieId, Precision.round(recommScore, 3))}

                userId -> recommMovies.toMap
        }
    }

    private def recommendationScore(favGenres: String,
                movieData: (String, Double)): Double = {
        var totScore = 0.0

        val avgRating = movieData._2

        val movieGenres = movieData._1.split("\\|")
            .filter(s => s != "IMAX").toVector
        val userGenres = favGenres.split("\\|")
            .filter(s => s != "IMAX").toVector

        for (uGenre <- userGenres) {
            var normFactor = movieGenres.length

            if (movieGenres.contains(uGenre)) {
                normFactor -= 1
            }

            for (mGenre <- movieGenres) {
                totScore += corrCoeff(uGenre, mGenre, normFactor)
            }
        }

        Precision.round(totScore * avgRating
            / userGenres.length, 3)
    }

    /*
     * Counts how many of the top 10 predicted movies per-user in predMovieIds
     * are within the domain of positive-rated movies per-user in testY
     */
    private def calcAccuracy(predY: RDD[(Int, Map[Int, Double])],
                             testY: RDD[(Int, Map[Int, Double])]): Double = {
        val accMetrics = predY.join(testY)
            .map {
                case (_, scoresInfo) =>
                    // Make sure predictions in scoresInfo._1 are still
                    // ordered by score after Spark combines
                    var predMovieIds = scoresInfo._1.toVector
                        .sortBy(_._2)(Ordering[Double]
                            .reverse).toMap.keys.toList

                    // Extract positive ratings in testY
                    val testPositives = scoresInfo._2.filter {
                        case (_, doubleScore) => doubleScore >= 3.0
                    }

//                    // Extract top 10 ratings from testY
//                    val testPositives = scoresInfo._2.toVector
//                        .sortBy(_._2)(Ordering[Double].reverse)
//                        .take(predMovieIds.size).toMap

                    val positiveMovieIds = testPositives
                        .keys.toList

                    var numEqual = 0
                    val numTotal = positiveMovieIds.size min
                        predMovieIds.size

                    // If positiveMovieIds are less than 10
                    // then we limit predMovieIds also
                    predMovieIds = predMovieIds.take(numTotal)

                    // Find movieIds matches
                    for(movieId <- predMovieIds) {
                        if(positiveMovieIds.contains(movieId)) {
                            numEqual += 1
                        }
                    }

                    (numEqual, numTotal)
                    // TODO: Extract per-user metrics
            }
            .reduce {
                case (metricAcc, metricSgl) =>
                    (metricAcc._1 + metricSgl._1,
                        metricAcc._2 + metricSgl._2)
            }

        val numEqual = accMetrics._1
        val numTotal = accMetrics._2

        numEqual * 1.0 / numTotal
    }

    // Public methods
    def corrCoeff(genre_A: String, genre_B: String,
                  normFactor: Double = 1.0): Double = {
        if (genresCorrMatrix == null
                    || genresCorrMatrix.isEmpty) {
            throw new RuntimeException("Correlation RDD not created yet")
        }

        if (!uniqueGenres.contains(genre_A)) {
            throw new RuntimeException("Gender doesn't exists: %s".format(genre_A.capitalize))
        } else if (!uniqueGenres.contains(genre_B)) {
            throw new RuntimeException("Gender doesn't exists: %s".format(genre_B.capitalize))
        }

        if (genre_A == genre_B) {
            1.0
        } else if (!genresCorrMatrix.keys.toList.contains(genre_A)) {
            0.0
        } else if (!genresCorrMatrix(genre_A).keys.toList.contains(genre_B)) {
            0.0
        } else {
            Precision.round(genresCorrMatrix(genre_A)(genre_B)
                * (1.0 / normFactor), 3)
        }
    }

    def makeRecommendations(nMovies: Int = 10):
    (Double, RDD[(Int, String)]) = {
        val rddTestX = rddUsers.map {
            case Row(userId: Int, favGenres: String) =>
                (userId, favGenres)
        }.join(rddTest.map {
            case Row(userId: Int, movieId: Int, _: Double) =>
                (userId, movieId)
        }.groupByKey())

        // Take fist nMovies predictions
        val rddPredsY = predictScores(rddTestX,
            nMovies).cache()

        // Change the format to match rddPredsY's
        val rddTestY = rddTest.map {
            case Row(userId: Int, movieId: Int, trueRating: Double) =>
                (userId, (movieId, trueRating))
        }
            .groupByKey()
            .map {
                case (userId, moviesInfo) =>
                    (userId, moviesInfo.toMap)
            }.cache()

        val gbcfAccuracy = calcAccuracy(rddPredsY, rddTestY)

        rddTestY.unpersist()
        rddTest.unpersist()

        val rddTopPreds = rddPredsY
            .repartition(1).mapValues {
            moviesInfo =>
                // Serialization with :,|
                moviesInfo.toVector
                    .sortBy(_._2)(Ordering[Double].reverse)
                    .map {
                        // Assure predScore is within 5.0 bound
                        case (movieId, predScore) =>
                            val predInScale = if(predScore > 5.0)
                                5.0 else predScore

                            (movieId, predInScale)
                    }
                    .map(_.productIterator
                        .mkString(":")).mkString("|")
        }

        (gbcfAccuracy, rddTopPreds.cache())
    }

    // Constructor body
    private val normalizedTest = Data.normalizeRatings(rddTest)

    // Check if it is None
    if (normalizedTest.isEmpty) {
        throw new RuntimeException("Test RDD in the wrong format")
    } else {
        rddTest = normalizedTest.get.cache()
    }

    private val uniqueGenres = rddMovies.cache()
        .map {
            case Row(_: Int, _: Double, genres: String) =>
                genres.split("\\|")
                    .filter(s => s != "IMAX").toVector
        }.flatMap(_.toList).distinct().collect()

    private val genresCorrMatrix = rddGenresCorr
        .cache().collectAsMap()

    println("\nGenres matrix generated successfully!\n")

}