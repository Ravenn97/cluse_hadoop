

from pyspark.sql import SparkSession
def check_log_file():
	sparkSession = SparkSession.builder.appName("example-pyspark-read-and-write").getOrCreate()

	df_load = sparkSession.read.parquet('hdfs://192.168.23.200:9000/data/Parquet/AdnLog/*')
	df_load.show()
	return None

if __name__ == '__main__':
	check_log_file()



