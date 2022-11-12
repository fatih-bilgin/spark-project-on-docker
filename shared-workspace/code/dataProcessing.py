from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def readFromParquet(file):
    df = spark.read.parquet(file)
    return df


if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("spark://spark-master:7077") \
        .appName("yelp-dataset-cleaning-step") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    # Read Parquet files
    businessDF = readFromParquet("/opt/workspace/cleaned/business.parquet")
    checkinDF = readFromParquet("/opt/workspace/cleaned/checkin.parquet")
    tipDF = readFromParquet("/opt/workspace/cleaned/tip.parquet")
    userDF = readFromParquet("/opt/workspace/cleaned/user.parquet")
    reviewDF = readFromParquet("/opt/workspace/cleaned/review.parquet")



    # Aggregation 1 : The stars per business on a weekly basis
    # Join review with business to get businessName
    reviewBusinessDF = reviewDF. \
        join(businessDF, reviewDF["business_id"] == businessDF["business_id"]). \
        drop(businessDF.business_id)

    # overalStars are same for all records at each group. so I used avg.
    weeklyStarsAndOveralStarDF = reviewBusinessDF. \
        withColumn("year", year("date")).withColumn("weekofyear", weekofyear("date")). \
        groupBy("businessName", "business_id", "year", "weekofyear"). \
        agg(avg("stars").alias("weeklyStar"), avg("overalStar").alias("overalStar")). \
        orderBy("businessName", desc("year"), "weekofyear")




    # Aggregation 2 : The number of checkins of a business compared to the overall star rating
    # explode the date array for computation.
    checkinDF = checkinDF. \
        withColumn("spDate", split("date", ", ")). \
        withColumn("date", explode(col("spDate"))). \
        select("business_id", "date")

    # Join checkin with business to get businessName
    checkinBusinessDF = checkinDF. \
        join(businessDF, checkinDF["business_id"] == businessDF["business_id"]). \
        drop(businessDF.business_id)

    # overalStars are same for all records at each group. so I used avg.
    checkinCountAndOveralStarDF = checkinBusinessDF. \
        groupBy("businessName", "business_id"). \
        agg(count("date").alias("checkinCount"),
            avg("overalStar").alias("overalStar")). \
        orderBy(desc("checkinCount"))



    # Aggregation 3 : the number of checkins of a business compared to the overall star rating
    # Join checkin with business to get businessName
    userReviewerDF = reviewDF. \
        join(userDF, reviewDF["user_id"] == userDF["user_id"]). \
        drop(userDF["user_id"])

    userReviewInThreeYearsDF = userReviewerDF. \
        withColumn("year", year("date")). \
        filter(col("year").isin(2020, 2021, 2022)). \
        groupBy("userName","user_id"). \
        agg(count("*").alias("reviewCountFor3Years"), avg("reviewCount").alias("totalReview")). \
        orderBy(desc("reviewCountFor3Years"))



    # Aggregation 4 : Business categories from most popular.
    businessCategories = businessDF. \
        withColumn("spCat", split("categories", ", ")). \
        withColumn("categories", explode(col("spCat"))). \
        groupBy("categories"). \
        agg(count("*").alias("categoryCount")). \
        orderBy(desc("categoryCount"))



    # Aggregation 5 : Businesses by states and cities
    businessByStateAndCity = businessDF. \
        groupBy("state", "city"). \
        agg(count("*").alias("businessCount")). \
        orderBy("state", "city")


    # Save the aggregated dataframes
    weeklyStarsAndOveralStarDF.repartition(1). \
        write.csv("/opt/workspace/aggregated/weeklyStarsAndOveralStarDF", sep=";", header="true")
    checkinCountAndOveralStarDF.repartition(1). \
        write.csv("/opt/workspace/aggregated/checkinCountAndOveralStarDF", sep=";", header="true")
    userReviewInThreeYearsDF.repartition(1). \
        write.csv("/opt/workspace/aggregated/userReviewInThreeYearsDF", sep=";", header="true")
    businessCategories.repartition(1). \
        write.csv("/opt/workspace/aggregated/businessCategories", sep=";", header="true")
    businessByStateAndCity.repartition(1). \
        write.csv("/opt/workspace/aggregated/businessByStateAndCity", sep=";", header="true")