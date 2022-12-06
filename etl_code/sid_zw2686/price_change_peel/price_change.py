from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import Window
import pyspark.sql.functions as func

if __name__ == "__main__":
  sc = SparkContext(appName = "price change")
  spark = SparkSession(sc)
  hdfs_path = "./project/project/"
  input_df = spark.read.csv(hdfs_path + "list.txt")
  input_list = [row[input_df.columns[0]] for row in input_df.select(input_df.columns[0]).collect()]
  for filename in input_list:
    df = spark.read.csv(hdfs_path + 'download/', header=True, inferSchema=True).dropna()
    w = Window.orderBy("date")
    df.withColumn("rn",func.row_number().over(w)).\
    withColumn("PriceChangeTo",func.lead(func.col("Price"),1).over(w)).\
    withColumn("discount", func.round((func.col("PriceChangeTo")/ func.col("Price"))*100, 2)).\
    where(func.col("Price") != func.col("PriceChangeTo")).\
    drop("rn","event","link","color", "val3").\
    write.options(header='True', delimiter=',').\
    csv(hdfs_path + "res/"+filename)
  sc.stop()