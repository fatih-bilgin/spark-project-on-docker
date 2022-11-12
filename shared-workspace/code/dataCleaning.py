from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def readFromJson(file,columns=["*"]):
    df = spark.read.json(file).selectExpr(columns)
    return df

def readFromParquet(file):
    df = spark.read.parquet(file)
    return df

def dropNas(dataset):
    return dataset.na.drop("all")

def dropNulls(dataset, cols):
    for c in cols:
        dataset = dataset.filter(col(c) != "null")
    return dataset

def trimCols(dataset, cols):
    for c in cols:
        dataset = dataset.withColumn(c, trim(c))
    return dataset

def renameCols(dataset, oldname, newname):
    for i in range(len(oldname)):
        dataset = dataset.withColumnRenamed(oldname[i], newname[i])
    return dataset



if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("spark://spark-master:7077") \
        .appName("yelp-dataset-cleaning-step") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    # Read raw JSON files
    raw_businessDF = readFromJson("/opt/workspace/yelp_academic_dataset_business.json")
    raw_checkinDF = readFromJson("/opt/workspace/yelp_academic_dataset_checkin.json")
    raw_reviewDF = readFromJson("/opt/workspace/yelp_academic_dataset_review.json")
    raw_userDF = readFromJson("/opt/workspace/yelp_academic_dataset_user.json")
    raw_tipDF = readFromJson("/opt/workspace/yelp_academic_dataset_tip.json")

    # Save files in parquet raw directory
    raw_businessDF.repartition(12).write.mode('overwrite'). \
        parquet("/opt/workspace/raw/business.parquet")
    raw_checkinDF.repartition(12).write.mode('overwrite'). \
        parquet("/opt/workspace/raw/checkin.parquet")
    raw_tipDF.repartition(12).write.mode('overwrite'). \
        parquet("/opt/workspace/raw/tip.parquet")
    raw_userDF.repartition(12).write.mode('overwrite'). \
        parquet("/opt/workspace/raw/user.parquet")
    raw_reviewDF.repartition(12).write.mode('overwrite'). \
        parquet("/opt/workspace/raw/review.parquet")


    """
    Cleaning step:
        1.Drop if all columns of a record are Na.
        2.Some records have "null" values. Clear this categorical features in those that will be used in data analysis.
        3.Trim the spaces of important columns to be used in the data analysis. Like businessName, city, userName etc.
        4.Rename some columns
    """

    businessDF = dropNas(raw_businessDF)
    checkinDF = dropNas(raw_checkinDF)
    reviewDF = dropNas(raw_reviewDF)
    userDF = dropNas(raw_userDF)
    tipDF = dropNas(raw_businessDF)

    businessDF = dropNulls(businessDF, ["business_id", "categories", "name", "city"])
    checkinDF = dropNulls(checkinDF, ["business_id"])
    reviewDF = dropNulls(reviewDF, ["business_id", "review_id", "date", "user_id"])
    userDF = dropNulls(userDF, ["user_id", "name", "yelping_since"])
    tipDF = dropNulls(tipDF, ["business_id", "categories", "city", "name"])

    businessDF = trimCols(businessDF, ["name", "city"])
    userDF = trimCols(userDF, ["name"])
    tipDF = trimCols(tipDF, ["name", "city"])

    businessDF = renameCols(businessDF,
                            ["name", "review_count", "stars"],
                            ["businessName", "reviewCount", "overalStar"])

    userDF = renameCols(userDF,
                            ["name", "review_count", "yelping_since", "average_stars"],
                            ["userName", "reviewCount", "memberSince", "averageStars"])

    #Save files in parquet cleaned directory
    businessDF.repartition(12).write.mode('overwrite'). \
        parquet("/opt/workspace/cleaned/business.parquet")
    checkinDF.repartition(12).write.mode('overwrite'). \
        parquet("/opt/workspace/cleaned/checkin.parquet")
    tipDF.repartition(12).write.mode('overwrite'). \
        parquet("/opt/workspace/cleaned/tip.parquet")
    userDF.repartition(12).write.mode('overwrite'). \
        parquet("/opt/workspace/cleaned/user.parquet")
    reviewDF.repartition(12).write.mode('overwrite'). \
        parquet("/opt/workspace/cleaned/review.parquet")