import sys, argparse
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_replace, split, lower
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, ArrayType
import boto3, datetime

app_start = datetime.datetime.now().timestamp()

# Parse arguments. Not all arguments are required for this ETL job. Only required arguments will be used by this job.
# Eg. spark-submit etl2.py --date 2020-04-06 --output-path s3://<your-bucket>/<output-path> --ddb-table Covid-project --glue-database covid_project --glue-table twitter_data
parser = argparse.ArgumentParser()
parser.add_argument("--date", type=str, required=True, help="YYYY-mm-dd eg. 2020-10-30")
parser.add_argument("--input-path", type=str, help="Eg. s3://my-bucket/prefix")
parser.add_argument("--output-path", type=str, required=True, help="Eg. s3://my-bucket/prefix")
parser.add_argument("--ddb-table", type=str, required=True, help="Name of DynamoDB table")
parser.add_argument("--glue-database", type=str, help="Name of Glue database. Default value is default.")
parser.add_argument("--glue-table", type=str, required=True, help="Name of table for Twitter data")
parser.add_argument("--region", type=str, help="AWS Region")
args = parser.parse_args()

# Set global variables from arguments
date = args.date.split("-")
year, month, day = date[0], date[1], date[2]
ddb_table = args.ddb_table
database = args.glue_database if args.glue_database else "default"
table = args.glue_table
region = args.region

# Output
output_path = args.output_path
output_table = "{}.covid_twitter_etl2_v3".format(database)

ddb = boto3.client("dynamodb", region_name=region)

dt = "{}-{}-{}".format(year, month, day)

def notify_running(sc):
	app_id = sc.applicationId
	update_attr = {
		'app_id': {'Value': {'S': app_id}}, 
		'app_starttime': {'Value': {'N': str(app_start)}},
		'status': {'Value': {'S': 'RUNNING'}},
		'user': {'Value': {'S': sc.sparkUser()}},
		'ui_web_url': {'Value': {'S': sc.uiWebUrl}},
		'queue': {'Value': {'S': sc.getConf().get("spark.yarn.queue")}}
	}
	update_ddb_item(update_attr)

def notify_completion():
	app_end = datetime.datetime.now().timestamp()
	time_taken = int(app_end - app_start) # in seconds
	update_attr = {
		'app_endtime': {'Value': {'N': str(app_end)}},
		'status': {'Value': {'S': 'COMPLETED'}},
		'app_runtime': {'Value': {'N': str(time_taken)}}
	}
	update_ddb_item(update_attr)

def notify_failure(e):
	app_end = datetime.datetime.now().timestamp()
	time_taken = int(app_end - app_start) # in seconds
	update_attr = {
		'app_endtime': {'Value': {'N': str(app_end)}},
		'status': {'Value': {'S': 'FAILED'}},
		'app_runtime': {'Value': {'N': str(time_taken)}}
	}
	update_ddb_item(update_attr)
	sys.exit(str(e))

def update_ddb_item(update_attr):
	etl = '2'
	try:
		updateresponse = ddb.update_item(TableName=ddb_table, Key={'date': {'S': dt}, 'etl': {'N': etl}}, AttributeUpdates=update_attr)
		if updateresponse['ResponseMetadata']['HTTPStatusCode'] == 200:
			print("Item {} updated successfully!".format(pk))
	except Exception as e: print(e)

def getTop10(map_collection):
	# Sort in order of highest word count 
	sorted_x = sorted(map_collection.items(), key=lambda kv: kv[1], reverse=True)
	top10words = []
	# while len(top10words) < 10:
	for i in sorted_x:
		if len(top10words) >= 10:
			break

		if i[0] != "":    # remove empty word
			top10words.append(i[0])
		# else:
		# 	top10words.append(sorted_x[10][0])
	return top10words

def calculateTop10Words(df2):
	# Convert tweet to an array
	df_transf_new= df2.withColumn("tweet", split(lower(col("tweet")), " "))

	# Count appearence of each word
	d = df_transf_new.rdd.flatMap(lambda a: a.tweet).countByValue()
	return getTop10(d)

def calculateTop10HashtagsPerCountry(df2):
	# Convert hashtags to an array
	df_transf_new= df2.withColumn(
	    "hashtags",
	    split(regexp_replace(lower(col("hashtags")), r"(^\[)|(\]$)|(')", ""), ", ")
	)
	d = df_transf_new.rdd.flatMap(lambda a: a.hashtags).countByValue()
	return getTop10(d)

def calculateNumberOfCovidTweetsRetrieved(df2):
	return df2.count()

# Main Method
def main():
	try:
		# Create Session
		spark = SparkSession \
		    .builder \
		    .appName("PySpark Process Covid19 Twitter Data") \
		    .enableHiveSupport() \
		    .config("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive","true") \
		    .config("hive.mapred.supports.subdirectories","true") \
		    .getOrCreate()

		# Update DDB
		notify_running(spark.sparkContext)

		# Read into DF -> drop any rows that have 'None' as hashtag
		query = 'select country, tweet, lower(hashtags) as hashtags from {}.{} where year="{}" AND month="{}" AND day="{}" AND hashtags NOT LIKE "%None%"'.format(database, table, year, month, day)
		df = spark.sql(query)
		df.persist()

		final_schema = StructType([
			StructField('country', StringType(), False),
			StructField('day', StringType(), False),
			StructField('numberOfCovidTweetsRetrieved', IntegerType(), False),
			StructField('top10WordsPerCountry', ArrayType(StringType()), False),
			StructField('top10HashtagsPerCountry', ArrayType(StringType()), False)
		])

		row_list = []

		# Get unique countries
		countries_code = df.select('country').distinct().collect()
		for country in countries_code:
			country = country['country']  # .encode('utf-8')
			c_df = df.filter(df.country == country)
			top10WordsPerCountry = calculateTop10Words(c_df)
			top10HashtagsPerCountry = calculateTop10HashtagsPerCountry(c_df)
			numberOfCovidTweetsRetrieved = calculateNumberOfCovidTweetsRetrieved(c_df)
			# new_row = spark.createDataFrame([(country, dt, numberOfCovidTweetsRetrieved, top10WordsPerCountry, top10HashtagsPerCountry)], final_schema)
			new_row = (country, dt, numberOfCovidTweetsRetrieved, top10WordsPerCountry, top10HashtagsPerCountry)
			row_list.append(new_row)

		# final_df = reduce(DataFrame.unionAll, df_series)
		final_df = spark.createDataFrame(row_list, final_schema)

		# Write to Hive table and S3
		final_df.repartition(1).write.mode("append").option("path", output_path).format("Parquet").saveAsTable(output_table)
		print("Wrote the output!")

		# Update DDB record
		notify_completion()

	except Exception as e:
		print(e)
		# Update DDB record
		notify_failure(e)

# Main Method Caller
if __name__ == '__main__':
	main()