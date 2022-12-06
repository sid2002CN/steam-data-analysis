hadoop jar clean.jar Clean ./hw/7/raw.csv ./hw/7/clean/output

hdfs dfs -get -f ./hw/7/clean/output .

head ./output/part-r-00000

cp -rf ./output/part-r-00000 ../clean.csv
