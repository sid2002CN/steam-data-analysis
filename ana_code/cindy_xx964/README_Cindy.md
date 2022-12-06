# Data Profiling
1. drop rows that have price=0 in general.csv and join it with leak.csv based on ids.(Using PySpark)
   1. Define a shape() function for checking the dataframe's shape
   ```python
    def sparkShape(dataFrame):
        return (dataFrame.count(), len(dataFrame.columns))
    pyspark.sql.dataframe.DataFrame.shape = sparkShape
   ```
   2. import data
   ```python
    df_leak = spark.read.csv("clean_leak.csv", header=True, inferSchema=True).drop("Title")
    df_general = spark.read.csv("general.csv", header=True, inferSchema=True)
    df_leak = df_leak.withColumnRenamed("Steam App ID", "id")
   ```
   3. drop rows that have price = 0 
   ```python
   df_general = df_general.filter(df_general["price"] != 0)
   ```
    4. join with leak data frame and output a new csv file joint.csv
    ```
    df_joint = df_general.join(df_leak,["id"])
    df_joint.shape()
    df_joint.write.csv("joint.csv",header=True)
    ```

2. Machine learning analysis on reviews number, reviews score, and players estimation.
   1. import libraries
   ```python
    import pyspark
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
    from pyspark.sql.types import IntegerType
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.regression import LinearRegression
    from pyspark.ml.regression import DecisionTreeRegressor
    from pyspark.ml.regression import RandomForestRegressor
    from pyspark.ml.evaluation import RegressionEvaluator
   ```
   2. Define shape() function to check a data frame's shape 
   ```python
   def sparkShape(dataFrame):
    return (dataFrame.count(), len(dataFrame.columns))
    pyspark.sql.dataframe.DataFrame.shape = sparkShape
   ```
   3. Read all files: 2018 leak csv, the release time csv, and the reviews csv
   ```python
   df_leak = spark.read.csv("clean_leak.csv", header=True, inferSchema=True).drop("Title")
    df_time = spark.read.csv("leak_time_result.csv", header = True, inferSchema=True)
    df_reviews = spark.read.csv("reviews_2018_all.csv", header = True, inferSchema=True).drop("review_score_desc")
   ```
    4. Correct mis-labled data types and rename some columns.
    ```python
    df_leak = df_leak.withColumn("Players Estimate", col("Players Estimate").cast(IntegerType()))
    df_leak = df_leak.withColumnRenamed("Players Estimate","Players")
    df_time = df_time.withColumn("year", col("year").cast(IntegerType()))
    df_reviews = df_reviews.withColumnRenamed("app_id","id")
    df_leak = df_leak.withColumnRenamed("Steam App ID", "id")
    ```
    5. Handle missing values: drop all rows that have NA values or have no reviews.
   ```python
   df_time = df_time.na.drop()
   df_reviews = df_reviews.filter((df_reviews["total_reviews"] != 0) & \
                               (df_reviews["total_positive"] != 0) & \
                               (df_reviews["total_negative"] != 0) & \
                               (df_reviews["review_score"] != 0) \
                               ).select(df_reviews.columns)
   ```
    6. Join these three updated frames into one frame df_general
   ```python
    df_general = df_time.join(df_reviews, ["id"]) \
                    .join(df_leak, ["id"])
    df_general.show()
   ```
   7. Group df_general by year, count each year's number. Since there are less data before 2013 and after 2019, y2013 includes all years data that before(and include) 2013; y2019 includes all years data that after(and include 2019)
   ```python
   y2013 = df_general.filter("year <= 2013").select(df_general.columns)  #includes years no more than 2013
    y2014 = df_general.filter("year == 2014").select(df_general.columns)
    y2015 = df_general.filter("year == 2015").select(df_general.columns)
    y2016 = df_general.filter("year == 2016").select(df_general.columns)
    y2017 = df_general.filter("year == 2017").select(df_general.columns)
    y2018 = df_general.filter("year == 2018").select(df_general.columns)
    y2019 = df_general.filter("year >= 2019").select(df_general.columns) # includes years older than 2019
   ```
   8. Using boxplot and .describe() to exclude outliers of each year
   ```python
    y2013.toPandas().plot.box();
    y2013.describe().show()
    y2013 = y2013.filter((y2013["Players"] <= 10000000))
    
    y2014.toPandas().plot.box();
    y2014.describe().show()
    y2014 = y2014.filter((y2014["Players"] <= 1000000))
   
    y2015.toPandas().plot.box();
    y2015.describe().show()
    y2015 = y2015.filter((y2015["Players"] <= 3000000))
   
    y2016.toPandas().plot.box();
    y2016.describe().show()
    y2016 = y2016.filter((y2016["Players"] <= 2000000))
   
    y2017.toPandas().plot.box();
    y2017.describe().show()
    y2017 = y2017.filter((y2017["Players"] <= 5000000))
   
    y2018.toPandas().plot.box();
    y2018.describe().show()
    y2018 = y2018.filter((y2018["Players"] <= 2000000))
   
    y2019.toPandas().plot.box();
    y2019.describe().show()
    y2019 = y2019.filter((y2019["Players"] <= 1000000))
   ```
   9. put all the years in a year_group, and create a year_strings list for future use
   ```python
    year_group = [y2013,y2014,y2015,y2016,y2017,y2018,y2019]
    year_strings = [f"year 201{i}" for i in range(3,10)]
   ```
   **10.Regression based on different years**
    1. Run a linear regression: x = total reviews, y = players. First take year 2018 as an example before running all years.
        a. set models
        ```python
        linear_featureassebler = VectorAssembler(inputCols=["total_reviews"],outputCol="Features")
        output2018 = linear_featureassebler.transform(y2018)
        final2018 = output2018.select("Features", "Players")
        ```
        b. train-test-split:
        ```python
        train, test = final2018.randomSplit([0.75,0.25],seed="964")
        ```
        c. set and fit regressor
        ```python
        regressor = LinearRegression(featuresCol="Features", labelCol="Players")
        regressor = regressor.fit(train)
        ```
        d. print coeeficients and intercept
        ```python
        regressor.coefficients
        regressor.intercept
        ```
        e. prediction and metrics R2
        ```python
        pred_result = regressor.evaluate(test)
        pred_result.predictions.sort(col("Players").desc()).show()
        pred_result.r2
        ```
    2. Using the same logic above, run the same linear regression of all years. Print all the coefficients and R2.
    ```python
    linear_featureassebler = VectorAssembler(inputCols=["total_reviews"],outputCol="Features")
    output_years = [linear_featureassebler.transform(year) for year in year_group]
    final_years = [output.select("Features", "Players") for output in output_years]
   
    train_groups = []
    test_groups = []
    for final_year in final_years:
        train,test = final_year.randomSplit([0.75,0.25],seed="964")
        train_groups.append(train)
        test_groups.append(test)
   
    regressor = LinearRegression(featuresCol="Features", labelCol="Players")
    regressors = [regressor.fit(train) for train in train_groups]
    coefficients = [regressor.coefficients.values[0] for regressor in regressors]
   
    print("Coefficients:")
    print(list(zip(year_strings, coefficients)))
   
    pred_results = [regressors[i].evaluate(test_groups[i]) for i in range(7)]
    R2s = [pred_result.r2 for pred_result in pred_results]
    print("\n")
    print("R2s:")
    print(list(zip(year_strings,R2s)))
    ```
    3. Run a multi-variable regression using linear regression model, decision tree model, and random forest model on year 2018.
       a. set models and prepare train,test subsets
       ```python
       featureassebler2 = VectorAssembler(inputCols=["review_score","total_positive","total_negative"], outputCol="Features")
        output2018 = featureassebler2.transform(y2018)
        final2018 = output2018.select("Features", "Players")
        train, test = final2018.randomSplit([0.75,0.25],seed="964")
       ```
       b. Linear regression model
       ```python
       regressor = LinearRegression(featuresCol="Features", labelCol="Players")
        regressor = regressor.fit(train)
        pred_result = regressor.evaluate(test)
        pred_result.predictions.sort(col("Players").desc()).show()
        pred_result.r2, pred_result.r2adj
       ```
       c. Decision tree model
       ```python
       regressor = DecisionTreeRegressor(featuresCol="Features", labelCol="Players")
        model = regressor.fit(train)
        predictions = model.transform(test)
        evaluator = RegressionEvaluator(
            labelCol="Players", predictionCol="prediction", metricName="r2")
        r2 = evaluator.evaluate(predictions)
        print("R2 on test data = %g" % r2)
       ```
       d. random forest model
       ```python
       regressor = RandomForestRegressor(featuresCol="Features", labelCol="Players")
        model = regressor.fit(train)
        predictions = model.transform(test)
        evaluator = RegressionEvaluator(
            labelCol="Players", predictionCol="prediction", metricName="r2")
        r2 = evaluator.evaluate(predictions)
        print("R2 on test data = %g" % r2)
       ```
    4. Similarly, run all three models above on all the years.
        a. set models
        ```python
        featureassebler2 = VectorAssembler(inputCols=["review_score","total_positive","total_negative"], outputCol="Features")
        output_years = [featureassebler2.transform(year) for year in year_group]
        final_years = [output.select("Features", "Players") for output in output_years]
        
        train_groups = []
        test_groups = []
        for final_year in final_years:
            train,test = final_year.randomSplit([0.75,0.25],seed="964")
            train_groups.append(train)
            test_groups.append(test)
        ```
        b. Linear regression model
        ```python
        regressor = LinearRegression(featuresCol="Features", labelCol="Players")
        regressors = [regressor.fit(train) for train in train_groups]
        
        pred_results = [regressors[i].evaluate(test_groups[i]) for i in range(7)]
        R2s = [pred_result.r2 for pred_result in pred_results]
        adj_R2s = [pred_result.r2adj for pred_result in pred_results]
        
        coefficients = [regressor.coefficients.values[0] for regressor in regressors]
        
        print("Coefficients:")
        print(list(zip(year_strings, coefficients)))
        print("\n")
        
        print("R2s:")
        print(list(zip(year_strings,R2s)))
        print("\n")
        
        print("Adj_R2s")
        print(list(zip(year_strings,adj_R2s)))
        ```
        c. Decision tree model
        ```python
        regressor = DecisionTreeRegressor(featuresCol="Features", labelCol="Players")
        models = [regressor.fit(train) for train in train_groups]
        predictions = [models[i].transform(test_groups[i]) for i in range(7)]
        
        evaluator = RegressionEvaluator(
            labelCol="Players", predictionCol="prediction", metricName="r2")
        r2s = [evaluator.evaluate(prediction) for prediction in predictions]
        
        print("R2s:")
        print(list(zip(year_strings,r2s)))
        print("\n")
        ```
        d. Random Forest model
        ```python
        regressor = RandomForestRegressor(featuresCol="Features", labelCol="Players")
        models = [regressor.fit(train) for train in train_groups]
        predictions = [models[i].transform(test_groups[i]) for i in range(7)]
        evaluator = RegressionEvaluator(
            labelCol="Players", predictionCol="prediction", metricName="r2")
        r2s = [evaluator.evaluate(prediction) for prediction in predictions]
        
        print("R2s:")
        print(list(zip(year_strings,r2s)))
        print("\n")
        ```