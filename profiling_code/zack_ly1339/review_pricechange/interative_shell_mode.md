```python
from pyspark.sql import SparkSession
from pyspark import SparkContext
import pyspark.sql.functions as func
from pyspark.sql import Window
from pyspark.sql.types import *

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)
```

```python
# My Net Id: ly1339
hdfs_path = "./project/discount/"
input_path = hdfs_path + "raw_input/"
```

1. Choose a single input as test, Full ETL code is in ELT folder.

```python
df = spark.read.csv(input_path+"240.csv", header=True, inferSchema=True)
df.show(10)
```

<img src="..\..\..\\screenshots\zack_ly1339\profiling\review_pricechange\step0.png" style="zoom: 67%;" />

2. Check schema and rows

```python
df.printSchema()
df.count()
```

3. Check how many null values drop if drop null.

```python
df.count() - df.dropna().count()
```

<img src="..\..\..\\screenshots\zack_ly1339\profiling\review_pricechange\step1.png" style="zoom: 80%;" />

4. Found that discount calculation is Wrong, recalculate discount

```python
df = df.dropna()
df = df.withColumn("discount", func.round((df.Price - df.PriceChangeTo) / df.Price * 100, 0).cast('Int'))
df.show(10)
```

<img src="..\..\..\\screenshots\zack_ly1339\profiling\review_pricechange\step3.png" style="zoom:80%;" />

5. Add app id column, for future combine use.

```python
df = df.withColumn('app_id', func.lit('240'))
df.show(10)
```

<img src="..\..\..\\screenshots\zack_ly1339\profiling\review_pricechange\step4.png" style="zoom:80%;" />

6. Add index column

```python
df = df.withColumn("index", func.row_number().over(Window.orderBy(func.monotonically_increasing_id())))
df.show(5)
```

<img src="..\..\..\\screenshots\zack_ly1339\profiling\review_pricechange\step5.png" style="zoom:80%;" />

7. Bunch of codes to create desired features, Full version in ETL folders as single .py Job

```python
colums = ["date", "total_reviews", "total_positive", "total_negative", "review_score", "discount"]
for c in colums:
    df = df.withColumn("prev_"+c, func.lag(func.col(c), 1).over(Window.orderBy("index")))
for c in colums:
    df = df.withColumn("next_"+c, func.lead(func.col(c), 1).over(Window.orderBy("index")))
res = df.filter(df.discount < 0).dropna()
res = res.withColumn("total_increase", res.total_reviews - res.prev_total_reviews)\
        .withColumn("positive_increase", res.total_positive - res.prev_total_positive)\
        .withColumn("negative_increase", res.total_negative - res.prev_total_negative)\
        .withColumn("days_increase", func.datediff(res.date, res.prev_date))\
        .withColumn("total_normal", res.next_total_reviews - res.total_reviews)\
        .withColumn("positive_normal", res.next_total_positive - res.total_positive)\
        .withColumn("negative_normal", res.next_total_negative - res.total_negative)\
        .withColumn("days_normal", func.datediff(res.next_date, res.date))\
        .withColumn("raw_price", res.PriceChangeTo)\
        .withColumn("sale_price", res.Price)\
        .withColumn("discount", res.prev_discount)
res = res.withColumn("total_increase_rate", res.total_increase / res.days_increase)\
        .withColumn("total_normal_rate", res.total_normal / res.days_normal)
res.select("date", "raw_price", "sale_price", "discount", "total_increase_rate", "total_normal_rate").show()
```

<img src="..\..\..\\screenshots\zack_ly1339\profiling\review_pricechange\step6.png" style="zoom:80%;" />

8.  Drop cols don't need, calculate sale_price_scale

```python
res = res.drop('Price', 'PriceChangeTo')
res = res.withColumn("sale_price_scale", (res.sale_price/10).cast('Int'))
res.select("sale_price_scale").show(5)
```

<img src="..\..\..\\screenshots\zack_ly1339\profiling\review_pricechange\step7.png" style="zoom:80%;" />

9. Steps to get Historical Lowest Price Label, Full version in ETL folder single *.py Job

```python
distinct_sale_price = res.dropDuplicates(['sale_price']).select("index", "sale_price").withColumn("historical_low", func.lit(1))
distinct_sale_price.show()
distinct_sale_price = distinct_sale_price.withColumn('prev_sale_price', \
    func.lag(func.col('sale_price'), 1).over(Window.orderBy("index")))
distinct_sale_price = distinct_sale_price.withColumn('diff', distinct_sale_price.sale_price - distinct_sale_price.prev_sale_price).fillna(-1)
distinct_sale_price.show()
distinct_sale_price.where(distinct_sale_price.diff > 0).count()
distinct_sale_price = distinct_sale_price.withColumn('historical_low', func.when(distinct_sale_price.diff > 0, 0).otherwise(1))\
    .where(distinct_sale_price.historical_low == 1)
distinct_sale_price.show()

distinct_sale_price = res.dropDuplicates(['sale_price']).select("index", "sale_price")

distinct_sale_price = distinct_sale_price.withColumn('prev_sale_price', \
    func.lag(func.col('sale_price'), 1).over(Window.orderBy("index")))

distinct_sale_price = distinct_sale_price.withColumn('diff', \
    distinct_sale_price.sale_price - distinct_sale_price.prev_sale_price).fillna(-1)

isRepeat = distinct_sale_price.where(distinct_sale_price.diff > 0).count() > 0

while isRepeat:
    distinct_sale_price = distinct_sale_price.withColumn('historical_low', \
        func.when(distinct_sale_price.diff > 0, 0).otherwise(1))
    distinct_sale_price = distinct_sale_price.where(distinct_sale_price.historical_low == 1)

    distinct_sale_price = distinct_sale_price.withColumn('prev_sale_price', \
        func.lag(func.col('sale_price'), 1).over(Window.orderBy("index")))
    
    distinct_sale_price = distinct_sale_price.withColumn('diff', \
        distinct_sale_price.sale_price - distinct_sale_price.prev_sale_price).fillna(-1)

    isRepeat = distinct_sale_price.where(distinct_sale_price.diff > 0).count() > 0

distinct_sale_price.show()
```

<img src="..\..\..\\screenshots\zack_ly1339\profiling\review_pricechange\step8_1.png" style="zoom: 67%;" />

<img src="..\..\..\\screenshots\zack_ly1339\profiling\review_pricechange\step8_2.png" style="zoom: 67%;" />

<img src="..\..\..\\screenshots\zack_ly1339\profiling\review_pricechange\step8_3.png" style="zoom: 80%;" />

10. Joint back to main df

```python
index_list = [row['index'] for row in distinct_sale_price.select('index').collect()]
res = res.withColumn('historical_low', func.when(res.index.isin(index_list), 1).otherwise(0))
res.select('sale_price', 'historical_low').show()
```

<img src="..\..\..\\screenshots\zack_ly1339\profiling\review_pricechange\step9.png" style="zoom: 80%;" />

11. Load Category and Genre Dataset

```python
tags_df = spark.read.csv(hdfs_path+"tags_input/joint_category_genre.csv", header=True, inferSchema=True)
tags_df.show(5)
tags_df = tags_df.withColumn('category', func.split(func.col('category'), ',')).withColumn('genre', func.split(func.col('genre'), ','))
tags_df.show(5)
tags_df.printSchema()
```

<img src="..\..\..\\screenshots\zack_ly1339\profiling\review_pricechange\step10.png" style="zoom:67%;" />

12. Inner joint with main df

```python
joint_df = res.join(tags_df, res.app_id == tags_df.id, 'inner')
joint_df.show(5)
```

![](..\..\..\\screenshots\zack_ly1339\profiling\review_pricechange\step11.png)

13. Prepare Dataset for analysis

```python
input_path = "step2_input/"
df = spark.read.csv(hdfs_path+input_path, header=True, inferSchema=True)
df.show(5)
```

![](..\..\..\\screenshots\zack_ly1339\profiling\review_pricechange\step12.png)

14. Select features and rename columns

```python
res = df.select('app_id', 'index', func.col('prev_date').alias('date'), func.col('prev_total_reviews').alias('popularity'), func.col('prev_review_score').alias('review_score'), 'discount', 'historical_low', 'sale_price_scale', func.col('days_increase').alias('days'), func.col('total_increase_rate').alias('sale_increase_rate'), func.col('total_normal_rate').alias('normal_increase_rate'))
res.show(5)
```

<img src="..\..\..\\screenshots\zack_ly1339\profiling\review_pricechange\step13.png" style="zoom: 80%;" />

15. Clean discount < 0, error rows

```python
print(res.where(res.discount < 0).count())
res.where(res.discount < 0).show(5)
res = res.where(res.discount > 0)
res.count()
```

<img src="..\..\..\\screenshots\zack_ly1339\profiling\review_pricechange\step14.png" style="zoom: 80%;" />

16. Prepare Potential output features, cast date to year

```python
res = res.withColumn('effect_min', res.sale_increase_rate - res.normal_increase_rate)
res = res.withColumn('effect_plus', res.sale_increase_rate + res.normal_increase_rate)
res = res.withColumnRenamed('date', 'year')
res = res.withColumn('year', func.year(res.year))
res.show(5)
```

![](..\..\..\\screenshots\zack_ly1339\profiling\review_pricechange\step15.png)

17. Get Genre and Category as Dummy Variables, Full Version of code in ETL single *.py Job

```python
res = res.withColumn("index", func.row_number().over(Window.partitionBy(func.col('app_id')).orderBy(func.monotonically_increasing_id())))
joint_df = res.join(tags_df, res.app_id == tags_df.id, 'inner').drop('id')
joint_df = joint_df.withColumn("uid", func.row_number().over(Window.orderBy(func.monotonically_increasing_id())))
df1 = joint_df.select('uid', func.explode('genre').alias('genre_id'))
df2 = df1.groupby('uid').pivot('genre_id').agg(func.lit(1)).fillna(0)
genre_id = ['1', '25', '37', '29', '3', '23', '28', '2', '4', '51', '53', '55', '57', '70', '9', '18', '73', '74', '58', '71', '72', '54', '56', '60', '59']
genre_col_id = [x for x in df2.columns if x in genre_id]
genre_col_name = ['gen_'+x for x in df2.columns if x in genre_id]
print(genre_col_id)
print(genre_col_name)
for i in range(len(genre_col_id)):
    df2 = df2.withColumnRenamed(genre_col_id[i], genre_col_name[i])
df2.show(5)
joint_df = joint_df.join(df2, on='uid')
joint_df.where(joint_df.app_id == 578080).select(genre_col_name).show(1)

df1 = joint_df.select('uid', func.explode('category').alias('category_id'))
df2 = df1.groupby('uid').pivot('category_id').agg(func.lit(1)).fillna(0)
cate_id = ['1', '49', '36', '15', '41', '42', '2', '9', '38', '22', '28', '29', '13', '30', '23', '8', '16', '14', '43', '44', '35', '47', '48', '27', '17', '18', '39', '24', '51', '20', '25', '37', '32', '31', '40']
cate_col_id = [x for x in df2.columns if x in cate_id]
cate_col_name = ['cate_'+x for x in df2.columns if x in cate_id]
print(cate_col_id)
print(cate_col_name)
for i in range(len(cate_col_id)):
    df2 = df2.withColumnRenamed(cate_col_id[i], cate_col_name[i])
df2.show(5)
joint_df = joint_df.join(df2, on='uid')
joint_df.where(joint_df.app_id == 578080).select(cate_col_name).show(1)
```

![](..\..\..\\screenshots\zack_ly1339\profiling\review_pricechange\step16_1.png)

![](..\..\..\\screenshots\zack_ly1339\profiling\review_pricechange\step16_2.png)

![](..\..\..\\screenshots\zack_ly1339\profiling\review_pricechange\step16_3.png)

