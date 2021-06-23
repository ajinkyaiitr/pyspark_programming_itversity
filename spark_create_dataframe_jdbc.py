===========================================================
CREATE DATAFRAMES USING JDBC
===========================================================

##Spark also facilitate us to read data from relational databases over JDBC.

@We need to make sure jdbc jar file is registered using --packages or --jars and --driver-class-path while launching pyspark
@In Pycharm, we need to copy relevant jdbc jar file to SPARK_HOME/jars
We can either use spark.read.format(‘jdbc’) with options or spark.read.jdbc with jdbc url, table name and other properties as dict to read data from remote relational databases.
We can pass a table name or query to read data using JDBC into Data Frame
While reading data, we can define number of partitions (using numPartitions), criteria to divide data into partitions (partitionColumn, lowerBound, upperBound)
Partitioning can be done only on numeric fields
If lowerBound and upperBound is specified, it will generate strides depending up on number of partitions and then process entire data. Here is the example
We are trying to read order_items data with 4 as numPartitions
partitionColumn – order_item_order_id
lowerBound – 10000
upperBound – 20000
order_item_order_id is in the range of 1 and 68883
But as we define lowerBound as 10000 and upperBound as 20000, here will be strides – 1 to 12499, 12500 to 14999, 15000 to 17499, 17500 to maximum of order_item_order_id
You can check the data in the output path mentioned ##

===================
SAMPLE QUERY
===================#

from pyspark.sql import SparkSession

spark = SparkSession. \
    builder. \
    master('local'). \
    appName('Create Dataframe over JDBC'). \
    getOrCreate()

orders = spark.read. \
  format('jdbc'). \
  option('url', 'jdbc:mysql://ms.itversity.com'). \
  option('dbtable', 'retail_db.orders'). \
  option('user', 'retail_user'). \
  option('password', 'itversity'). \
  load()

orders.show()

orderItems = spark.read. \
    jdbc("jdbc:mysql://ms.itversity.com", "retail_db.order_items",
          properties={"user": "retail_user",
                      "password": "itversity",
                      "numPartitions": "4",
                      "partitionColumn": "order_item_order_id",
                      "lowerBound": "10000",
                      "upperBound": "20000"})

orderItems.write.json('/user/training/bootcamp/pyspark/orderItemsJDBC')

query = "(select order_status, count(1) from retail_db.orders group by order_status) t"
queryData = spark.read. \
    jdbc("jdbc:mysql://ms.itversity.com", query,
         properties={"user": "retail_user",
                     "password": "itversity"})

queryData.show()	
