from pyspark.sql import SparkSession
from pyspark import SparkContext
import pyspark.sql.functions as func
from pyspark.sql import Window

input_path = "./raw_input/"
output_path_0 = "./output/step0/"
output_path_1 = "./output/step1/"

if __name__ == "__main__":
    sc = SparkContext(appName="discount analysis")
    spark = SparkSession(sc)
    input_df = spark.read.csv("input.txt")
    # input_df = input_df.toPandas()
    # input_list = input_df[input_df.columns[0]].to_list()
    input_list = [row[input_df.columns[0]]
                  for row in input_df.select(input_df.columns[0]).collect()]

    # input_list = input_list[:3]

    for file in input_list:
        df = spark.read.csv(
            input_path+file, header=True, inferSchema=True).dropna()

        # Recalculate dicount
        df = df.withColumn("discount", func.round(
            (df.Price - df.PriceChangeTo) / df.Price * 100, 0).cast('Int'))
        # Add steam app id column
        df = df.withColumn('app_id', func.lit(file.split('.')[0]))
        # Add index column
        df = df.withColumn("index", func.row_number().over(
            Window.orderBy(func.monotonically_increasing_id())))
        # Create lag and lead columns
        colums = ["date", "total_reviews", "total_positive",
                  "total_negative", "review_score", "discount"]
        for c in colums:
            df = df.withColumn(
                "prev_"+c, func.lag(func.col(c), 1).over(Window.orderBy("index")))
        for c in colums:
            df = df.withColumn(
                "next_"+c, func.lead(func.col(c), 1).over(Window.orderBy("index")))

        #! Outout for step 0
        df.write.options(header='True', delimiter=',').csv(output_path_0+file)

        # * Get important infos for analysis
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
        res = res.drop('Price', 'PriceChangeTo')
        res = res.withColumn("sale_price_scale",
                             (res.sale_price/10).cast('Int'))

        # * Difficult Point: historical low price
        distinct_sale_price = res.dropDuplicates(
            ['sale_price']).select("index", "sale_price")
        distinct_sale_price = distinct_sale_price.withColumn('prev_sale_price',
                                                             func.lag(func.col('sale_price'), 1).over(Window.orderBy("index")))
        distinct_sale_price = distinct_sale_price.withColumn('diff',
                                                             distinct_sale_price.sale_price - distinct_sale_price.prev_sale_price).fillna(-1)

        isRepeat = distinct_sale_price.where(
            distinct_sale_price.diff > 0).count() > 0

        while isRepeat:
            distinct_sale_price = distinct_sale_price.withColumn('historical_low',
                                                                 func.when(distinct_sale_price.diff > 0, 0).otherwise(1))
            distinct_sale_price = distinct_sale_price.where(
                distinct_sale_price.historical_low == 1)
            distinct_sale_price = distinct_sale_price.withColumn('prev_sale_price',
                                                                 func.lag(func.col('sale_price'), 1).over(Window.orderBy("index")))
            distinct_sale_price = distinct_sale_price.withColumn('diff',
                                                                 distinct_sale_price.sale_price - distinct_sale_price.prev_sale_price).fillna(-1)

            isRepeat = distinct_sale_price.where(
                distinct_sale_price.diff > 0).count() > 0

        index_list = [row['index']
                      for row in distinct_sale_price.select('index').collect()]
        res = res.withColumn('historical_low', func.when(
            res.index.isin(index_list), 1).otherwise(0))

        #! Output for step 1
        res.write.options(header='True', delimiter=',').csv(output_path_1+file)

    sc.stop()
