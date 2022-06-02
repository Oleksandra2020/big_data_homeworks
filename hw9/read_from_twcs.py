from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SimpleSparkProject').getOrCreate()


def count_rows():
	file = "./twcs.csv"
	df = spark.read.csv(file)
	print(f"---------------------------------------------------------------------------------\nCount is: {df.count()}\n---------------------------------------------------------------------------------")

if __name__ == "__main__":
	count_rows()
