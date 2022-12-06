1. Import dataset

```python
from pyspark.sql import SparkSession
from pyspark import SparkContext
import pyspark.sql.functions as func

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

hdfs_path = "./project/discount/"
```

```python
df = spark.read.csv(hdfs_path+"analysis_input/", header=True, inferSchema=True)
df.show(10)
```

![](..\..\screenshots\zack_ly1339\analysis\step1.png)

2. Average Treatment Effect (ATE) on 'historical_low'?

```python
df.groupBy('historical_low').avg('sale_increase_rate', 'normal_increase_rate', 'effect_min', 'effect_plus').show()
```

![](..\..\screenshots\zack_ly1339\analysis\step2.png)

It did have some effect

3. ATE on Year?

```python
df.groupBy('year').avg('sale_increase_rate', 'normal_increase_rate', 'effect_min', 'effect_plus').sort('year').show()
```

![](..\..\screenshots\zack_ly1339\analysis\step3.png)

Not really.

4. ATE on review_score?

```
df.groupBy('review_score').avg('sale_increase_rate', 'normal_increase_rate', 'effect_min', 'effect_plus').sort('review_score').show()
```

![](..\..\screenshots\zack_ly1339\analysis\step4.png)

Higher score have stronger effect.

5. ATE on sale_price_scale?

```python
df.groupBy('sale_price_scale').avg('sale_increase_rate', 'normal_increase_rate', 'effect_min', 'effect_plus').sort('sale_price_scale').show()
```

![](..\..\screenshots\zack_ly1339\analysis\step5.png)

Interestingly, 5 get the highest price, maybe 3A games?

6. ATE for sale_price_scale on popularity?

```python
df.groupBy('sale_price_scale').avg('popularity').sort('sale_price_scale').show()
```

![](..\..\screenshots\zack_ly1339\analysis\step6.png)

7. ATE for discount?

```python
df.groupBy('discount').avg('sale_increase_rate', 'normal_increase_rate', 'effect_min', 'effect_plus').sort('discount').show()
```

![](..\..\screenshots\zack_ly1339\analysis\step7.png)

A lot of noise.

8. Do linear Regression and Random forest to predict effect_min and effect_plus from feature variables.

```python
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import RandomForestRegressor

non_feature = ['uid', 'app_id', 'sale_increase_rate', 'normal_increase_rate', 'effect_min', 'effect_plus', 'year']
featureCols = [x for x in df.columns if x not in non_feature]
print(featureCols)
```

```python
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import RandomForestRegressor

#Linear
final_df = df.select(featureCols + ['effect_min'])

# final_df = final_df.where(final_df['effect_min'] < 1000)

assembler = VectorAssembler(inputCols=featureCols, outputCol="features")
assembled_df = assembler.transform(final_df)
standardScaler = StandardScaler(inputCol="features", outputCol="features_scaled")
scaled_df = standardScaler.fit(assembled_df).transform(assembled_df)

train_data, test_data = scaled_df.randomSplit([.7, .3], seed=1339)

lr = (LinearRegression(featuresCol='features_scaled', labelCol="effect_min", predictionCol='effect_min_pred',
                       maxIter=100, standardization=False))
linearModel = lr.fit(train_data)
predictions = linearModel.transform(test_data)

pred_and_labels = predictions.select("effect_min_pred", "effect_min")

evaluator = RegressionEvaluator(predictionCol='effect_min_pred', labelCol='effect_min', metricName='rmse')
print("RMSE: {0}".format(evaluator.evaluate(pred_and_labels)))

evaluator = RegressionEvaluator(predictionCol='effect_min_pred', labelCol='effect_min', metricName='mae')
print("MAE: {0}".format(evaluator.evaluate(pred_and_labels)))

evaluator = RegressionEvaluator(predictionCol='effect_min_pred', labelCol='effect_min', metricName='r2')
print("R2: {0}".format(evaluator.evaluate(pred_and_labels)))


print("=============")

# Random Forest
rf = RandomForestRegressor(labelCol="effect_min", featuresCol="features", predictionCol='effect_min_pred')

rfModel = rf.fit(train_data)
predictions = rfModel.transform(test_data)

pred_and_labels = predictions.select("effect_min_pred", "effect_min")

evaluator = RegressionEvaluator(predictionCol='effect_min_pred', labelCol='effect_min', metricName='rmse')
print("RMSE: {0}".format(evaluator.evaluate(pred_and_labels)))

evaluator = RegressionEvaluator(predictionCol='effect_min_pred', labelCol='effect_min', metricName='mae')
print("MAE: {0}".format(evaluator.evaluate(pred_and_labels)))

evaluator = RegressionEvaluator(predictionCol='effect_min_pred', labelCol='effect_min', metricName='r2')
print("R2: {0}".format(evaluator.evaluate(pred_and_labels)))
```

```python
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import RandomForestRegressor

#Linear
final_df = df.select(featureCols + ['effect_plus'])

# final_df = final_df.where(final_df['effect_plus'] < 1000)

assembler = VectorAssembler(inputCols=featureCols, outputCol="features")
assembled_df = assembler.transform(final_df)
standardScaler = StandardScaler(inputCol="features", outputCol="features_scaled")
scaled_df = standardScaler.fit(assembled_df).transform(assembled_df)

train_data, test_data = scaled_df.randomSplit([.7, .3], seed=1339)

lr = (LinearRegression(featuresCol='features_scaled', labelCol="effect_plus", predictionCol='effect_plus_pred',
                       maxIter=100, standardization=False))
linearModel = lr.fit(train_data)
predictions = linearModel.transform(test_data)

pred_and_labels = predictions.select("effect_plus_pred", "effect_plus")

evaluator = RegressionEvaluator(predictionCol='effect_plus_pred', labelCol='effect_plus', metricName='rmse')
print("RMSE: {0}".format(evaluator.evaluate(pred_and_labels)))

evaluator = RegressionEvaluator(predictionCol='effect_plus_pred', labelCol='effect_plus', metricName='mae')
print("MAE: {0}".format(evaluator.evaluate(pred_and_labels)))

evaluator = RegressionEvaluator(predictionCol='effect_plus_pred', labelCol='effect_plus', metricName='r2')
print("R2: {0}".format(evaluator.evaluate(pred_and_labels)))


print("=============")

# Random Forest
rf = RandomForestRegressor(labelCol="effect_plus", featuresCol="features", predictionCol='effect_plus_pred')

rfModel = rf.fit(train_data)
predictions = rfModel.transform(test_data)

pred_and_labels = predictions.select("effect_plus_pred", "effect_plus")

evaluator = RegressionEvaluator(predictionCol='effect_plus_pred', labelCol='effect_plus', metricName='rmse')
print("RMSE: {0}".format(evaluator.evaluate(pred_and_labels)))

evaluator = RegressionEvaluator(predictionCol='effect_plus_pred', labelCol='effect_plus', metricName='mae')
print("MAE: {0}".format(evaluator.evaluate(pred_and_labels)))

evaluator = RegressionEvaluator(predictionCol='effect_plus_pred', labelCol='effect_plus', metricName='r2')
print("R2: {0}".format(evaluator.evaluate(pred_and_labels)))
```

<img src="..\..\screenshots\zack_ly1339\analysis\step8.png" style="zoom: 80%;" />



