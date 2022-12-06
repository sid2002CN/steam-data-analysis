from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Window
import pyspark.sql.functions as func

spark = SparkSession.builder.master("local[1]").getOrCreate()
hdfs_path = "./"
df = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(hdfs_path + "joint_category_genre.csv")
df_temp = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(hdfs_path + "joint.csv")
df = df.alias("a").join(df_temp.alias("b"),df.id == df_temp.id, "inner").select("a.id", "a.category", "a.genre", "b.price", "b.popularity")
df.show()

df1 = df.withColumn(
    "genre",
    func.explode(func.split("genre", r"\s*,\s*"))
).groupBy("genre").agg(
    func.avg("price").alias("avg_price")
)
df_temp2 = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(hdfs_path + "genre_index.csv")
df1=df1.filter(df1.genre != "51")
df1=df1.filter(df1.genre != "53")
df1=df1.filter(df1.genre != "55")
df1=df1.filter(df1.genre != "57")
df1 = df1.alias("a").join(df_temp2.alias("b"),df1.genre == df_temp2.id, "inner").select("b.name", "a.avg_price")
df1.show()

df2 = df.withColumn(
    "genre",
    func.explode(func.split("genre", r"\s*,\s*"))
).groupBy("genre").agg(
    expr('percentile(price, array(0.5))')[0].alias('med_val')
)
df2=df2.filter(df2.genre != "51")
df2=df2.filter(df2.genre != "53")
df2=df2.filter(df2.genre != "55")
df2=df2.filter(df2.genre != "57")
df2 = df2.alias("a").join(df_temp2.alias("b"),df2.genre == df_temp2.id, "inner").select("b.name", "a.med_val")
df2.show()

df1.write.options(header='True', delimiter=',').csv(hdfs_path + "genre_res/avg")
df2.write.options(header='True', delimiter=',').csv(hdfs_path + "genre_res/med")