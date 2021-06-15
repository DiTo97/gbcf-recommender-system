"""
Preprocess the MovieLens dataset (either small or full) stored on HDFS with Spark.

Refer to the attached Jupyter notebook to follow our reasoning behind the choices
that we made to preprocess the full MovieLens datasets.
"""

# Python imports
import argparse
import time
import numpy as np

from functools import reduce

# PySpark imports
from pyspark import StorageLevel
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.functions import col
from pyspark.sql.functions import regexp_extract
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StringType

#
# HDFS endpoints
#

Hadoop = {
    'URI': 'hdfs://master:9000',
    'PREPROCESSING_DIR': '/home/user30/Exam/preprocessing/'
}

Hadoop['OUTPUT_DIR'] = Hadoop['PREPROCESSING_DIR'] + 'outputs/'
Hadoop['DATA_ROOT_URI'] = 'hdfs://' + Hadoop['PREPROCESSING_DIR'] + 'rawdata/'

#
# IO helper functions
#


def load_csv_hdfs(ss, csv_name,
                header=True, delimiter=","):
        abs_file_path = "%s%s" % (Hadoop['DATA_ROOT_URI'], csv_name)

        return ss.read.option("inferSchema", "true") \
            .option("header", str(header))   \
            .option("delimiter", delimiter)  \
            .option("mode", "DROPMALFORMED") \
            .option("inferSchema", "True")   \
            .csv(abs_file_path)

#
# Spark map UDFs
#


def get_genres_hist(all_seen_genres):
    """Given a list of the genres of all the movies seen by a user
            returns their histogram (counts per-genre) from the set"""

    flat_genres = [i for sublist in all_seen_genres for i in sublist]
    distinct_genres = set(flat_genres)

    genres_cnts = {}

    for genre in distinct_genres:
        cnt = flat_genres.count(genre)
        genres_cnts[genre] = cnt

    return genres_cnts, len(flat_genres)


def user_fav_genres(all_seen_genres):
    """Given a list of the genres of all the movies extracts the favourite genres
            for the user as the most frequent ones from the set"""
    genres_hist = get_genres_hist(all_seen_genres)

    num_total_genres = genres_hist[1]
    genres_cnt = genres_hist[0]

    genres_pct = dict([(k, v / num_total_genres)
        for (k, v) in genres_cnt.items()])

    genres_above_thresh = dict(filter(lambda el:
        el[1] >= 0.2, genres_pct.items()))

    # If there are at least two clear (occurence >= 0.2)
    # favourtite genres take those...
    if (len(genres_above_thresh) >= 2):
        return list(genres_above_thresh.keys())
    # ...otherwise just take the
    # first two in sorted order
    else:
        genres_pct = list(sorted(genres_pct.items(),
            key=lambda el: el[1], reverse=True))
        return list(map(lambda el: el[0], genres_pct[:2]))


if __name__ == "__main__":
    m_raw_filename = "movies.csv"
    r_raw_filename = "ratings.csv"

    args_parser = argparse.ArgumentParser(allow_abbrev=False)
    args_parser.add_argument('-t', '--type', type=str, choices=['small', 'full'],
                default="full", help="Size of the raw MovieLens datasets")

    args = args_parser.parse_args()
    
    if args.type == 'small':
        m_raw_filename = "moviessmall.csv"
        r_raw_filename = "ratingssmall.csv"

    ss = SparkSession.builder \
            .appName("Pre-processing MovieLens") \
            .config("spark.kryoserializer.buffer.max", "512") \
            .getOrCreate()

    sc = ss.sparkContext
    sc.setLogLevel("ERROR")

    pipe_splitter = UserDefinedFunction(lambda x: x.split('|'),
        ArrayType(StringType()))

    # DF schema: movieId,title,genres
    df_movies  = load_csv_hdfs(ss, m_raw_filename)

    # DF schema: userId,movieId,rating,timestamp
    df_ratings = load_csv_hdfs(ss, r_raw_filename)

    # Extract year embedded inside title
    df_movies_with_year = df_movies.withColumn('year',
        regexp_extract(col('title'), '\\(([0-9]{4})\\)', 1))

    # Filter movies that either didn't have a year (row[3]) in the title
    # or whose year is older than 1942 (Netflix standard); then, filter them
    # by those with no listed gernes and subtract the IMAX genre.
    rdd_valid_movies = df_movies_with_year.select("movieId", "title",
                    pipe_splitter("genres"), "year").rdd \
        .filter(lambda row: row[3] != '') \
        .filter(lambda row: int(row[3]) > 1942) \
        .map(lambda row: (row[0], row[1], tuple(row[2]), int(row[3]))) \
        .filter(lambda row: "(no genres listed)" not in row[2]) \
        .map(lambda row: (row[0], row[1], tuple(set(row[2])
                    - set(['IMAX'])), row[3])) \
        .persist(StorageLevel.DISK_ONLY)

    # Extract movieid,genres
    rdd_valid_movies_genres = rdd_valid_movies.map(lambda row: (row[0], row[2]))

    # Extract the ratings related to the valid movies
    # just filtered above and keep the genres.
    # RDD schema: userid,movieid,rating,timestamp,genres 
    rdd_ratings = df_ratings.map(lambda row: (row[1],
                        (row[0], row[2], row[3]))) \
                    .join(rdd_valid_movies_genres).map(lambda row:
                        (row[1][0][0], row[0], row[1][0][1],
                            row[1][0][2], row[1][1])).cache()
    
    rdd_pos_ratings = rdd_ratings.filter(lambda row: row[2] >= 3)

    ## Apply percentiles on the RDDs to find the most meaningful
    ## split between training and test sets

    # Find the min threshold of ratings to have for
    # a movie to be counted as valid
    num_ratings_per_movie = rdd_pos_ratings.map(lambda row: (
        row[1], 1)).reduceByKey(lambda acc, val: acc + val).collectAsMap()

    num_ratings_movies = list(num_ratings_per_movie.values())
    l_tresh = float(np.percentile(num_ratings_movies, 70))

    # Filter the movies by l_thresh
    rdd_ratings_per_movie = sc.parallelize(list(num_ratings_per_movie.items()))
    rdd_valid_movies_ratings = rdd_ratings_per_movie.filter(lambda row: row[1]
        >= l_tresh).map(lambda row: (row[0], 1))

    # Re-filter the ratings based on the newly
    # extracted valid movies by positive ratings.
    # RDD schema: userid,movieid,rating,timestamp,genres
    rdd_ratings = rdd_ratings.map(lambda row:
                        (row[1], ((row[0], row[2], row[3], row[4])))) \
                .join(rdd_valid_movies_ratings) \
                .map(lambda row: (row[1][0][0], row[0], row[1][0][1],
                        row[1][0][2], row[1][0][3])).cache()

    rdd_pos_ratings = rdd_ratings.filter(lambda row: row[2] >= 3)

    # Find the min/max thresholds of ratings to have for
    # a user to be counted as valid
    num_ratings_per_user = rdd_pos_ratings.map(lambda row: (
        row[0], 1)).reduceByKey(lambda acc, val: acc + val).collectAsMap()

    num_ratings_users = list(num_ratings_per_user.values())

    l_tresh  = float(np.percentile(num_ratings_users, 50))
    h_thresh = float(np.percentile(num_ratings_users, 98))

    # Filter the users by l/h_thresh
    rdd_ratings_per_user = sc.parallelize(list(num_ratings_per_user.items()))
    rdd_valid_users = rdd_ratings_per_user                 \
        .filter(lambda row: l_tresh <= row[1] <= h_thresh) \
        .map(lambda row: (row[0], 1))

    rdd_ratings = rdd_ratings.map(lambda row: (row[0],
                        (row[1], row[2], row[3], row[4]))) \
                .join(rdd_valid_users) \
                .map(lambda row: (row[0], row[1][0][0], row[1][0][1],
                        row[1][0][2], row[1][0][3])).cache()

    #
    # Split training/test sets 80-20 based on
    # a timestamp basis per-user
    #

    rdd_users_max_timestamps = rdd_ratings.map(lambda row:
                (row[0], row[3])).reduceByKey(max)

    rdd_users_min_timestamps = rdd_ratings.map(lambda row:
                (row[0], row[3])).reduceByKey(min)

    # RDD schema: userid,mintimestamp,maxtimestamp
    rdd_users_minmax_timestamps = rdd_users_min_timestamps \
                .join(rdd_users_max_timestamps)

    # Take as training set all the ratings per-user
    # which fall in its respective min/max timestamps
    # RDD schema: userid,movieid,rating,timestamp,genres
    rdd_training = rdd_ratings.map(lambda row: (row[0],
                        (row[1], row[2], row[3], row[4]))) \
                .join(rdd_users_minmax_timestamps) \
                .map(lambda row: (row[0], row[1][0][0], row[1][0][1],
                        row[1][0][2], row[1][0][3], row[1][1][0], row[1][1][1])) \
                .filter(lambda row: row[3] <= float((row[6]
                        - row[5]) / 5) * 4 + row[5]) \
                .map(lambda row: (row[0], row[1], row[2],
                        row[3], row[4])).cache()

    # Take as test set all what's left
    rdd_test = rdd_ratings.subtract(rdd_training).cache()

    #
    # Assure that all the movies in the test set are
    # also present in training set, otherwise the models
    # would have no information to predict those
    #

    rdd_movies_training = rdd_training.map(lambda row: row[1]).distinct()
    rdd_movies_test = rdd_test.map(lambda row: row[1]).distinct()

    # Find movies to discard
    rdd_movies_discard = rdd_movies_test.subtract(
        rdd_movies_training).map(lambda el: (el, 1))

    # Update the test set removing all the
    # movies to discard in surplus
    rdd_test = rdd_test.map(lambda row: (row[0], (row[1],
                        row[2], row[3], row[4]))) \
                .subtractByKey(rdd_movies_discard) \
                .map(lambda row: (row[0], row[1][0],
                        row[1][1], row[1][2], row[1][3])).cache()

    # Force an action on the test set to let everything
    # beforehand be computed; in this way the UTC-time-based
    # directory creation is forced to happen after
    # all the major transformantions have been computed
    rdd_test.count()

    #
    # Extract the information which will be saved in the
    # auxiliary datasets (avg_ratings.csv, users_fav_genres.csv)
    #

    rdd_avg_rating_per_movie = rdd_training.map(lambda row: (row[1], float(row[2]))) \
                .map(lambda row: (row[1], (float(row[2]), 1))) \
                .reduceByKey(lambda acc, el: (acc[0] + el[0], acc[1] + el[1])) \
                .map(lambda el: (el[0], el[1][0] / el[1][1])) \
                .join(rdd_valid_movies_genres) \
                .map(lambda row: (row[0], row[1][0], reduce((lambda acc, el:
                        acc + el+"|"), list(row[1][1]), ''))).map(list)

    rdd_users_fav_genres = rdd_training.map(lambda row: (row[0], row[4])) \
                .groupByKey() \
                .mapValues(lambda g: user_fav_genres(g)) \
                .map(lambda row: (row[0], reduce((lambda acc, el:
                        acc + el + "|"), list(row[1]), '')))

    #
    # Extract Spark objects from the context to enable
    # manipulation of HDFS and create the UTC-time-based directory
    #

    URI        = sc._gateway.jvm.java.net.URI
    Path       = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

    fs = FileSystem.get(URI(Hadoop['URI']),
                sc._jsc.hadoopConfiguration())

    dir_utc_id = round(time.time() * 1000)

    if args.type == 'small':
        dir_name = "%s%d%s" % (Hadoop['OUTPUT_DIR'],
                dir_utc_id, "-small")
    else:
        dir_name = "%s%d" % (Hadoop['OUTPUT_DIR'],
                dir_utc_id)

    # If the directory is created successfully, collect
    # the RDDs and save them as CSV files...
    if fs.mkdirs(Path(dir_name)):
        output_path = Hadoop['URI'] + dir_name + "/"

        print("\nDirectory %s created successfully!\n" % dir_name)

        # avg_ratings.csv
        df_avg_ratings = ss.createDataFrame(rdd_avg_rating_per_movie.map(lambda row: Row(
                                movieid = row[0], avgrating = row[1],
                                        genres = row[2]))).select(
                                "movieid", "avgrating", "genres")

        df_avg_ratings.write.option("header", "true") \
                .format("com.databricks.spark.csv")   \
                .save(output_path + "avg_ratings.csv")

        # test.csv
        df_test = ss.createDataFrame(rdd_test.map(lambda row: (row[0], row[1], row[2], row[3],
                                reduce((lambda acc, el: acc + el + "|"), list(row[4]), ''))) \
                        .map(lambda row: Row(userid = row[0], movieid = row[1],
                                rating = row[2], timestamp = row[3], genres = row[4]))) \
                        .select("userid", "movieid", "rating", "timestamp", "genres")

        df_test.write.option("header", "true") \
                .format("com.databricks.spark.csv") \
                .save(output_path + "test.csv")

        # training.csv
        df_training = ss.createDataFrame(rdd_training.map(lambda row: (row[0], row[1], row[2], row[3],
                                reduce((lambda acc, el: acc + el + "|"), list(row[4]), ''))) \
                        .map(lambda row: Row(userid = row[0], movieid = row[1],
                                rating = row[2], timestamp = row[3], genres = row[4]))) \
                        .select("userid", "movieid", "rating", "timestamp", "genres")

        df_training.write.option("header", "true") \
                .format("com.databricks.spark.csv") \
                .save(output_path + "training.csv")

        # users_fav_gernes.csv
        df_users_fav_genres = ss.createDataFrame(rdd_users_fav_genres.map(
                        lambda row: Row(userid = row[0],
                                favgenres = row[1]))).select("userid", "favgenres")

        df_users_fav_genres.write.option("header", "true") \
                .format("com.databricks.spark.csv") \
                .save(output_path + "users_fav_genres.csv")

        # titles.csv
        df_titles = ss.createDataFrame(rdd_valid_movies.map(
                        lambda row: Row(movieid = row[0], title = row[1]))) \
                .select("movieid", "title")

        df_titles.write.option("header", "true") \
                .format("com.databricks.spark.csv") \
                .save(output_path + "titles.csv")
                
    # ...otherwise stop the Spark job
    else:
        print("\nSomething went wrong...\n")

    ss.stop()
