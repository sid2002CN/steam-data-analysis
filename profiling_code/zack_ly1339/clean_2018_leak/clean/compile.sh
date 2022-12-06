hdfs dfs -put commons-csv-1.9.0.jar .

javac -classpath .:commons-csv-1.9.0.jar:`yarn classpath` -d . CleanMapper.java

javac -classpath `yarn classpath` -d . CleanReducer.java

javac -classpath `yarn classpath`:. -d . Clean.java

jar -cvf clean.jar *.class
