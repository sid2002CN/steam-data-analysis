from pyspark.sql import SparkSession
from pyspark import SparkContext
import pyspark.sql.functions as func
from pyspark.sql import Window

hdfs_path = "./project/discount/"
input_path = hdfs_path + "step2_input/"
output_path = hdfs_path + "analysis_input/"

if __name__ == "__main__":
    sc = SparkContext(appName="step2")
    spark = SparkSession(sc)

    df = spark.read.csv(input_path, header=True, inferSchema=True)
    res = df.select('app_id', 'index', func.col('prev_date').alias('date'), func.col('prev_total_reviews').alias('popularity'), func.col('prev_review_score').alias('review_score'), 'discount', 'historical_low', 'sale_price_scale', func.col('days_increase').alias('days'), func.col('total_increase_rate').alias('sale_increase_rate'), func.col('total_normal_rate').alias('normal_increase_rate'))
    res = res.where(res.discount > 0)
    res = res.withColumn("index", func.row_number().over(Window.partitionBy(func.col('app_id')).orderBy(func.monotonically_increasing_id())))
    res = res.withColumn('effect_min', res.sale_increase_rate - res.normal_increase_rate)
    res = res.withColumnRenamed('date', 'year')
    res = res.withColumn('year', func.year(res.year))
    res = res.withColumn('effect_plus', res.sale_increase_rate + res.normal_increase_rate)

    tags_df = spark.read.csv(hdfs_path+"tags_input/joint_category_genre.csv", header=True, inferSchema=True)
    tags_df = tags_df.withColumn('category', func.split(func.col('category'), ',')).withColumn('genre', func.split(func.col('genre'), ','))
    joint_df = res.join(tags_df, res.app_id == tags_df.id, 'inner').drop('id')

    joint_df = joint_df.withColumn("uid", func.row_number().over(Window.orderBy(func.monotonically_increasing_id())))

    df1 = joint_df.select('uid', func.explode('genre').alias('genre_id'))
    df2 = df1.groupby('uid').pivot('genre_id').agg(func.lit(1)).fillna(0)

    genre_id = ['1', '25', '37', '29', '3', '23', '28', '2', '4', '51', '53', '55', '57', '70', '9', '18', '73', '74', '58', '71', '72', '54', '56', '60', '59']
    genre_col_id = [x for x in df2.columns if x in genre_id]
    genre_col_name = ['gen_'+x for x in df2.columns if x in genre_id]

    for i in range(len(genre_col_id)):
        df2 = df2.withColumnRenamed(genre_col_id[i], genre_col_name[i])
    joint_df = joint_df.join(df2, on='uid')

    df1 = joint_df.select('uid', func.explode('category').alias('category_id'))
    df2 = df1.groupby('uid').pivot('category_id').agg(func.lit(1)).fillna(0)

    cate_id = ['1', '13', '14', '15', '16', '17', '18', '2', '20', '22', '23', '24', '25', '27', '28', '29', '30', '31', '32', '35', '36', '37', '38', '39', '40', '41', '42', '43', '44', '47', '48', '49', '51', '8', '9']
    cate_col_id = [x for x in df2.columns if x in cate_id]
    cate_col_name = ['cate_'+x for x in df2.columns if x in cate_id]

    for i in range(len(cate_col_id)):
        df2 = df2.withColumnRenamed(cate_col_id[i], cate_col_name[i])
    joint_df = joint_df.join(df2, on='uid')

    joint_df.drop('category', 'genre').coalesce(1).write.options(header='True', delimiter=',').csv(output_path)

    sc.stop()