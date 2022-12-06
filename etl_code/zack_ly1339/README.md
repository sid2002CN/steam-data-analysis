interative output of etl process is in 

\profiling_code\zack_ly1339\review_pricechange\interative_shell_mode.md

With some explaination.


ETL Job files:

1. step0_1_peel.py
2. step2_peel.py

Run the job by:

```
spark-submit xxx.py
```

You should be able to specify my hdfs folder by changeing

```
hdfs_path = "./project/discount/"
```

line from code.
