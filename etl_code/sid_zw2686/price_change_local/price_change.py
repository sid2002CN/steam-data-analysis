from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Window
import pyspark.sql.functions as func
def file(filename):
    spark = SparkSession.builder.master("local[1]").getOrCreate()
    customSchema = StructType([
      StructField("date", DateType(), True),
      StructField("Owners", StringType(), True),
      StructField("event", StringType(), True),
      StructField("link", StringType(), True),
      StructField("color", StringType(), True),
      StructField("Price", DoubleType(), True),
      StructField("val3", StringType(), True),
    ])

    # read file into dataframe
    df = spark.read.load('download/'+filename, format="csv", header="true", sep=',', schema=customSchema)
    #df.printSchema()
    #df.show()

    #genrate the window based on date
    w = Window.orderBy("date")

    # generate a new column with the previous price value
    # generate the discount based on the previous price value and price value
    # only keep the rows which previous price value not equal to price value
    df.withColumn("rn",row_number().over(w)).\
    withColumn("PriceChangeTo",lead(col("Price"),1).over(w)).\
    withColumn("discount", func.round((col("PriceChangeTo")/ col("Price"))*100, 2)).\
    where(col("Price") != col("PriceChangeTo")).\
    drop("rn","event","link","color", "val3").\
    write.options(header='True', delimiter=',').csv("res/"+filename)

my_file = open("list.txt", "r")
  
# reading the file
data = my_file.read()
  
list = data.split("\n")
for i in list:
    file(i)