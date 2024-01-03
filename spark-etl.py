import sys
from datetime import datetime

from pyspark.sql import SparkSession #responsible for create spark session
from pyspark.sql.functions import * #functions to manipulate dataframe with spark

if __name__ == "__main__": #main function that verifies if the script is being executed
    
    #ETL

    print(len(sys.argv))
    if (len(sys.argv) != 3): #verify if the number of arguments is correct
        print("Usage: spark-etl [input-folder] [output-folder]")
        sys.exit(0)

    spark = SparkSession\
        .builder\
        .appName("SparkETL")\
        .getOrCreate() #create spark session

    nyTaxi = spark.read.option("inferSchema", "true").option("header", "true").csv(sys.argv[1]) #read csv file and load data into dataframe

    updatedNYTaxi = nyTaxi.withColumn("current_date", lit(datetime.now())) #add column with current date

    updatedNYTaxi.printSchema() #print dataframe schema

    print(updatedNYTaxi.show()) #print updated dataframe

    print("Total number of records: " + str(updatedNYTaxi.count())) #print total number of records

    updatedNYTaxi.write.parquet(sys.argv[2]) #write dataframe into parquet file