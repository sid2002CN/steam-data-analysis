# Steam Market Analysis

**Note*: Our project contains massive scraping work with python and 3rd party software, which has asked for permission from Professor Ann, and we included all scraping codes in data ingestion and etl part (/data_ingest & /etl)**

**Please acknowledge the effort we put into scraping first-hand data**

![](.\screenshots\scrape.png)

**Our group have three people: Zack_ly1339, Cindy_xx964 and Sid_zw2686**

We have created individual folders for our work, with proper README files in subfolders if necessary.

## Zack (ly1339)

**HDFS Directory:**

```
user/ly1339/project/discount/
```

**Main Task:**

1. Scrape Price, Owners history data from Steamspy.com, and Review Count from Steam Web API.
2. Analysis the relationship between discount and sale increase (measured by review count increase)

**Scrape Part:**

1. \data_ingest\zack_ly1339\2018_reviews_scrape
   - download_review_2018.py to download data
   - run json_to_csv.py to transfer json to csv
   - **Output for this part is used for Cindy's analysis**
2. \data_ingest\zack_ly1339\review_pricechange_scrape
   - Run download.py to download data
   - **Output for this part is used for Zack's Profiling & ETL**
3. \data_ingest\zack_ly1339\steamspy_scrape
   - See README files in folders

You'll may need to install the packages in requirements.txt to run python code, or `pandas`, and `requests` is the least requirements.

**Analysis Part**:

1. \profiling_code\zack_ly1339\clean_2018_leak

   1. MapReduce Job to clean 2018 leak data
2. HDFS: `user/ly1339/hw/7/raw.csv`

2. The remaining profile code and etl code are all explained in readme files in the folders.

## Cindy (xx964)

HDFS: user/xx964/steam

Scraping part in ETL, Analysis in Profile

## Sid (zw2686)

HDFS:

user/zw2686/project/genre

user/zw2686/project/priceChange#   s t e a m - d a t a - a n a l y s i s  
 