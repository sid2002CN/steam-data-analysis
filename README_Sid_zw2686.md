# ETL codes

### Price change :

#### Input files: 

​	A folder of game_id.csv files.

​	Each game_id.csv has the schema of "date","Owners","event","link","color","Price","val3".

Example input: 220.csv

```
"date","Owners","event","link","color","Price","val3"
"2015-04-01","8804000","","","","9.99",""
"2015-04-02","8804000","","","","9.99",""
"2015-04-03","8797000","","","","9.99",""
"2015-04-04","8780000","","","","9.99",""
```



#### Output files: 

​	A folder of game_id.csv files.

​	Each game_id.csv has the schema of "date,Owners,Price,PriceChangeTo,discount"

​	Only select the rows which the "Price" column value is changed from the previous row. 

​	Kept the "date,Owners,Price“ columns, and generate "PriceChangeTo, discount" based on the selected "Price".

Example output: 220.csv

```
date,Owners,Price,PriceChangeTo,discount
2015-06-11,8968000,9.99,2.49,24.92
2015-06-22,8989000,2.49,9.99,401.2
2015-11-10,9218000,9.99,2.49,24.92
```



#### Detailed Steps:

##### 1. Generate List.txt

​	Generate the list of filenames with in the input folder using spark:

```
    var Fsys1 = FileSystem.get(sparksession.sparkContext.hadoopConfiguration)
    var FileNames = Fsys1 .listStatus(new  Path("hdfspath").filter(_.isFile).map(_.getPath.getName).toList
```

##### 2. Local test with spark env (Local Path: "final_code_drop\etl_code\sid_zw2686\price_change_local"):

```python
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
```

##### 3.Run on Peel. (Input HDFS Path: "zw2686/project/priceChange", Output HDFS Path: "zw2686/project/project/res"):

```python
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import Window
import pyspark.sql.functions as func

if __name__ == "__main__":
  sc = SparkContext(appName = "price change")
  spark = SparkSession(sc)
  hdfs_path = "./project/priceChange/"
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
```

###### Note: 

The code for some unknown reason cannot be runed with spark-submit, although worked perfectly with local machine. The only way to run it on Peel is by using the "pyspark --deploy-mode client" and run it in the shell.

###### Screen Shot of successful running on Peel:

![priceChange](.\screenshots\sid_zw2686\priceChange.png)

# Data analytic

### Genre Analytics:

#### Input files: 

##### 	category_index.csv:

​	category id and its name

​	Generated from Steam official API. (See Cindy's readme for detail)

##### 	genre_index.csv:

​	genre id and its name

​	Generated from Steam official API. (See Cindy's readme for detail)

##### 	joint_category_genre.csv:

​	game id with its category ids and genre ids

​	Generated from Steam official API. (See Cindy's readme for detail)

##### 	joint.csv:

​	Each game_id.csv has the schema of "date","Owners","event","link","color","Price","val3".

​	Scrapped from SteamPriceHistory.com. (See Cindy's readme for detail)



#### Output files: 

##### 	genre_avgPrice.csv:

​	Game Genre name and its average price.

##### 	genre_medPrice.csv:

​	Game Genre name and its median price.

#### Detailed Steps on Local Test

#### (Local Path: "final_code_drop\ana_code\sid_zw2686\genre_local"):

​	Same as the following steps on peel.

#### Detailed Steps on Peel

#### (HDFS Path: "zw2686/project/genre/"):

##### 1. Generate the dataframe that is used for later Analytic

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Window
import pyspark.sql.functions as func

spark = SparkSession.builder.master("local[1]").getOrCreate()
hdfs_path = "project/genre/"
df = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(hdfs_path + "joint_category_genre.csv")
df_temp = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(hdfs_path + "joint.csv")
df = df.alias("a").join(df_temp.alias("b"),df.id == df_temp.id, "inner").select("a.id", "a.category", "a.genre", "b.price", "b.popularity")
df.show()
```

###### Screenshot:

![genre_step1](.\screenshots\sid_zw2686\genre_step1.png)

##### 2. Calculate the average price for each genre using explode()

```python
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
```

###### Screenshot:

![genre_step2](.\screenshots\sid_zw2686\genre_step2.png)

##### 3.Calculate the median price for each genre using explode()

```python
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
```

###### Screenshot:

![genre_step3](.\screenshots\sid_zw2686\genre_step3.png)

##### 4. Write to csv files

```python
df1.write.options(header='True', delimiter=',').csv(hdfs_path + "genre_res/avg")
df2.write.options(header='True', delimiter=',').csv(hdfs_path + "genre_res/med")
```

###### Screenshot:

![genre_ste4](.\screenshots\sid_zw2686\genre_step4.png)