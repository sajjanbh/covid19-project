#!/usr/bin/python3

import sys, argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lit, col, first
from pyspark.sql.types import IntegerType, FloatType
import pycountry
import boto3
import datetime

app_start = datetime.datetime.now().timestamp()

# Parse arguments. Not all arguments are required for this ETL job. Only required arguments will be used by this job.
# Eg. spark-submit etl1.py --date 2020-04-06 --input-path s3://<your-bucket>/covid-ingest-data --output-path s3://<your-bucket>/<output-path> --ddb-table Covid-project --glue-database covid_project --region us-east-1
parser = argparse.ArgumentParser()
parser.add_argument("--date", type=str, required=True, help="YYYY-mm-dd eg. 2020-10-30")
parser.add_argument("--input-path", type=str, required=True, help="Eg. s3://my-bucket/prefix")
parser.add_argument("--output-path", type=str, required=True, help="Eg. s3://my-bucket/prefix")
parser.add_argument("--ddb-table", type=str, required=True, help="Name of DynamoDB table")
parser.add_argument("--glue-database", type=str, help="Name of Glue database. Default value is default.")
parser.add_argument("--glue-table", type=str, help="Name of table for Twitter data")
parser.add_argument("--region", type=str, help="AWS Region")
args = parser.parse_args()

# Set global variables from arguments
date = args.date.split("-")
year, month, day = date[0], date[1], date[2]
input_path = "{}/csse_covid_19_data/csse_covid_19_daily_reports/{}-{}-{}.csv".format(args.input_path, month, day, year)
output_path = args.output_path
ddb_table = args.ddb_table
glue_db = args.glue_database if args.glue_database else "default"
region = args.region

processed_output_path = "{}/etl1_processed/".format(output_path)
aggregaeted_output_path = "{}/etl1_aggregated/".format(output_path)

glue_processed_table = "{}.etl1_processed_v3".format(glue_db)
glue_aggregated_table = "{}.etl1_aggregated_v3".format(glue_db)

ddb = boto3.client("dynamodb", region_name=region)

# To handle mapping of country name to country code 
input_countries = {}
unknown_countries = []
# make manual list of country codes from prvious unknown_countries
input_countries['Mainland China'] = "CN"
input_countries['Macau'] = 'MO'
input_countries['South Korea'] = 'KR'
input_countries['Ivory Coast'] = 'CI'
input_countries['North Ireland'] = 'GB'
input_countries['Republic of Ireland'] = 'IE'
input_countries['St. Martin'] = 'MF'
input_countries['Iran (Islamic Republic of)'] = 'IR'
input_countries['Taipei and environs'] = 'TW'
input_countries['occupied Palestinian territory'] = 'PS'
input_countries['Channel Islands'] = 'GBR'
input_countries['Korea, South'] = 'KR'
input_countries['Cruise Ship'] = 'XZ' # international waters
input_countries['Taiwan*'] = 'TW'
input_countries['Congo (Kinshasa)'] = 'CD'
input_countries['Congo (Brazzaville)'] = 'CG'
input_countries['Gambia, The'] = 'GM'
input_countries['Bahamas, The'] = 'BS'
input_countries['Cape Verde'] = 'CV'
input_countries['East Timor'] = 'TL'
input_countries['Laos'] = 'LA'
input_countries['Diamond Princess'] = 'XZ' # Cruise ship
input_countries['West Bank and Gaza'] = 'PS'
input_countries['Burma'] = 'MM'
input_countries['MS Zaandam'] = 'XZ' # Cruise ship
input_countries['Others'] = 'XZ'

# Types of schema discovered so far
#01-22-2020.csv
known_schema = [['Province/State', 'Country/Region', 'Last Update', 'Confirmed', 'Deaths', 'Recovered']]
#03-01-2020.csv
known_schema.append(['Province/State', 'Country/Region', 'Last Update', 'Confirmed', 'Deaths', 'Recovered', 'Latitude', 'Longitude'])
#03-22-2020.csv
known_schema.append(['FIPS', 'Admin2', 'Province_State', 'Country_Region', 'Last_Update', 'Lat', 'Long_', 'Confirmed', 'Deaths', 'Recovered', 'Active', 'Combined_Key'])
#05-29-2020.csv
known_schema.append(['FIPS', 'Admin2', 'Province_State', 'Country_Region', 'Last_Update', 'Lat', 'Long_', 'Confirmed', 'Deaths', 'Recovered', 'Active', 'Combined_Key', 'Incidence_Rate', 'Case-Fatality_Ratio'])
#11-09-2020.csv
known_schema.append(['FIPS', 'Admin2', 'Province_State', 'Country_Region', 'Last_Update', 'Lat', 'Long_', 'Confirmed', 'Deaths', 'Recovered', 'Active', 'Combined_Key', 'Incident_Rate', 'Case_Fatality_Ratio'])

def to_country_code(country):
	existing = input_countries.get(country)
	if existing: 
		return existing
	elif country in unknown_countries:
		return country
	else:
		try:
			cc = pycountry.countries.search_fuzzy(country)[0]
			input_countries[country] = cc.alpha_2
			return cc.alpha_2
		except Exception as e:
			unknown_countries.append(country)
			return country

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
	pk = "{}-{}-{}".format(year, month, day)
	etl = '1'
	try:
		updateresponse = ddb.update_item(TableName=ddb_table, Key={'date': {'S': pk}, 'etl': {'N': etl}}, AttributeUpdates=update_attr)
		if updateresponse['ResponseMetadata']['HTTPStatusCode'] == 200:
			print("Item {} updated successfully!".format(pk))
	except Exception as e: print(e)

# Main Method
def main():
	try:
		# Create Spark Session
		spark = SparkSession \
			.builder \
			.appName("PySpark Process Covid19 Github data") \
			.enableHiveSupport() \
			.getOrCreate()

		notify_running(spark.sparkContext)
		# Register custom function as UDF
		myudf = udf(to_country_code)

		raw = spark.read.csv(input_path, header=True)

		# Conditional processing of input file based on match from known schema
		if raw.columns not in known_schema:
			print("New Schema Found! {}".format(raw.columns))
			notify_failure("New Schema Detected!")

		if known_schema.index(list(raw.columns)) == 0:
		    raw = raw.withColumnRenamed("Province/State", "state").withColumnRenamed("Country/Region", "country")\
		        .withColumnRenamed("Last Update", "last_update").withColumnRenamed("Confirmed", "confirmed")\
		        .withColumnRenamed("Deaths", "deaths").withColumnRenamed("Recovered", "recovered")\
		        .withColumn('latitude', lit("null")).withColumn('longitude', lit("null"))\
		        .withColumn("active", lit("null")).withColumn("incidence_rate", lit("null"))\
		        .withColumn("case_fatality_ratio", lit("null"))
		elif known_schema.index(list(raw.columns)) == 1:
		    raw = raw.withColumnRenamed("Province/State", "state").withColumnRenamed("Country/Region", "country")\
		        .withColumnRenamed("Last Update", "last_update").withColumnRenamed("Confirmed", "confirmed")\
		        .withColumnRenamed("Deaths", "deaths").withColumnRenamed("Recovered", "recovered")\
		        .withColumnRenamed("Latitude", "latitude").withColumnRenamed("Longitude", "longitude")\
		        .withColumn("active", lit("null"))\
		        .withColumn("incidence_rate", lit("null")).withColumn("case_fatality_ratio", lit("null"))
		elif known_schema.index(list(raw.columns)) == 2:
		    raw = raw.withColumnRenamed("Province_State", "state").withColumnRenamed("Country_Region", "country")\
		        .withColumnRenamed("Last_Update", "last_update").withColumnRenamed("Lat", "latitude").withColumnRenamed("Long_", "longitude")\
		        .withColumnRenamed("Confirmed", "confirmed").withColumnRenamed("Deaths", "deaths")\
		        .withColumnRenamed("Recovered", "recovered").withColumnRenamed("Active", "active")\
		        .withColumn('incidence_rate', lit("null")).withColumn("case_fatality_ratio", lit("null"))
		elif known_schema.index(list(raw.columns)) == 3:
		    raw = raw.withColumnRenamed("Province_State", "state").withColumnRenamed("Country_Region", "country")\
		        .withColumnRenamed("Last_Update", "last_update").withColumnRenamed("Lat", "latitude")\
		        .withColumnRenamed("Long_", "longitude").withColumnRenamed("Confirmed", "confirmed").withColumnRenamed("Deaths", "deaths")\
		        .withColumnRenamed("Recovered", "recovered").withColumnRenamed("Active", "active")\
		        .withColumnRenamed("Incidence_Rate", "incidence_rate").withColumnRenamed("Case-Fatality_Ratio", "case_fatality_ratio")
		elif known_schema.index(list(raw.columns)) == 4:
		    raw = raw.withColumnRenamed("Province_State", "state").withColumnRenamed("Country_Region", "country")\
		        .withColumnRenamed("Last_Update", "last_update").withColumnRenamed("Lat", "latitude")\
		        .withColumnRenamed("Long_", "longitude").withColumnRenamed("Confirmed", "confirmed").withColumnRenamed("Deaths", "deaths")\
		        .withColumnRenamed("Recovered", "recovered").withColumnRenamed("Active", "active")\
		        .withColumnRenamed("Incident_Rate", "incidence_rate").withColumnRenamed("Case_Fatality_Ratio", "case_fatality_ratio")


		mapped = raw.withColumn("country", myudf(raw.country))
		mapped = mapped.fillna('null')
		mapped = mapped.dropDuplicates()

		# Cast columns to proper data type
		mapped = mapped.withColumn("confirmed", col("confirmed").cast(IntegerType()))\
		    .withColumn("deaths", col("deaths").cast(IntegerType()))\
		    .withColumn("recovered", col("recovered").cast(IntegerType()))\
		    .withColumn("latitude", col("latitude").cast(FloatType()))\
		    .withColumn("longitude", col("longitude").cast(FloatType()))\
		    .withColumn("active", col("active").cast(IntegerType()))\
		    .withColumn("incidence_rate", col("incidence_rate").cast(FloatType()))\
		    .withColumn("case_fatality_ratio", col("case_fatality_ratio").cast(FloatType()))\
		    .select("state", "country", "last_update", "latitude", "longitude", "confirmed", "deaths", "recovered", "active", "incidence_rate", "case_fatality_ratio")

		processed = mapped.withColumn('month', lit("{}-{}".format(year, month))).withColumn('day', lit("{}".format(day)))

		# Create another dataframe by grouping the data by country and getting sum of stats.
		summed = mapped.groupBy(['country']).sum('confirmed','deaths','recovered')\
		.withColumnRenamed("sum(confirmed)", "confirmed").withColumnRenamed("sum(deaths)", "deaths")\
		.withColumnRenamed("sum(recovered)", "recovered")

		aggregated_df = summed.withColumn('month', lit("{}-{}".format(year, month))).withColumn('day', lit("{}".format(day)))

		# Write un-aggregated and processed data to Hive and S3. This retains original stats by co-ordinates for a day.
		processed.coalesce(1).write.mode("append").option("path",processed_output_path)\
			.partitionBy("country", "month").format("Parquet").saveAsTable(glue_processed_table)	
		print("Processed data written successfully!")

		# Write aggregaeted data to Hive and S3. This summarizes stats by state and country for a day.
		aggregated_df.repartition(1).write.mode("append").option("path", aggregaeted_output_path).format("Parquet").saveAsTable(glue_aggregated_table)	
		print("Aggregated data written successfully!")

		# Update DDB record
		notify_completion()
	except Exception as e:
		print(e)
		# Update DDB record with FAILED state
		notify_failure(e)

# Main Method Caller
if __name__ == '__main__':
	main()
