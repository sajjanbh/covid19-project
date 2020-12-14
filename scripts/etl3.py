import sys, argparse
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import boto3

app_start = datetime.now().timestamp()

# Parse arguments. Not all arguments are required for this ETL job. Only required arguments will be used by this job.
# Eg. spark-submit etl3.py --date 2020-04-06 --output-path s3://<your-bucket>/<output-path> --ddb-table Covid-project --glue-database covid_project
parser = argparse.ArgumentParser()
parser.add_argument("--date", type=str, required=True, help="YYYY-mm-dd eg. 2020-10-30")
parser.add_argument("--input-path", type=str, help="Eg. s3://my-bucket/prefix")
parser.add_argument("--output-path", type=str, required=True, help="Eg. s3://my-bucket/prefix")
parser.add_argument("--ddb-table", type=str, required=True, help="Name of DynamoDB table")
parser.add_argument("--glue-database", type=str, help="Name of Glue database. Default value is default.")
parser.add_argument("--glue-table", type=str, help="Name of table for Twitter data")
parser.add_argument("--region", type=str, help="AWS Region")
args = parser.parse_args()

# Set global variables from arguments
date = args.date.split("-")
year, month, day = date[0], date[1], date[2]
ddb_table = args.ddb_table
database = args.glue_database if args.glue_database else "default"
region = args.region

output_path = args.output_path
output_table = "{}.covid_etl3_v3".format(database)

# These are the output table names from ETL1 and ETL2.
etl1_table = "{}.etl1_aggregated_v3".format(database)
etl2_table = "{}.covid_twitter_etl2_v3".format(database)

# DynamoDB parameters
ddb = boto3.client("dynamodb", region_name=region)

# Process data from yesterday (Tuesday) to last Wednesday
dt = "{}-{}-{}".format(year, month, day)
this_day = datetime.strptime(dt, "%Y-%m-%d")
tues = this_day - timedelta(days=1)
last_wed = this_day - timedelta(days=7)

yesterday = tues.strftime("%Y-%m-%d")
that_day = last_wed.strftime("%Y-%m-%d")
processed_days = []

def get_where_clause():
    # Prepare a WHERE clause for ETL1 data based on month and day values for last week
    m_d = {}
    for i in range(7):
        d = tues - timedelta(days=i)
        # Add day to processed_days list
        processed_days.append({'S': d.strftime("%Y-%m-%d")})

        m = '{}-{}'.format(d.year, "{0:0=2d}".format(d.month))
        if m not in m_d: m_d[m] = ["{0:0=2d}".format(d.day)]
        else: m_d[m].append("{0:0=2d}".format(d.day))

    conditions = []
    for m, d in m_d.items():
        conditions.append('month = "{}" AND day IN ("{}")'.format(m, '", "'.join(d)))

    return '({})'.format(') OR ('.join(conditions))

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
    app_end = datetime.now().timestamp()
    time_taken = int(app_end - app_start) # in seconds
    update_attr = {
        'app_endtime': {'Value': {'N': str(app_end)}},
        'status': {'Value': {'S': 'COMPLETED'}},
        'app_runtime': {'Value': {'N': str(time_taken)}}
    }
    update_ddb_item(update_attr)

def notify_failure(e):
    app_end = datetime.now().timestamp()
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
  etl = '3'
  try:
    updateresponse = ddb.update_item(TableName=ddb_table, Key={'date': {'S': pk}, 'etl': {'N': etl}}, AttributeUpdates=update_attr)
    if updateresponse['ResponseMetadata']['HTTPStatusCode'] == 200:
      print("Item {} updated successfully!".format(pk))
  except Exception as e: print(e)

# Main Method
def main():
    try:
        # Create Session
        spark = SparkSession \
            .builder \
            .appName("Pyspark - ETL3 - Join Github and Twitter data") \
            .enableHiveSupport() \
            .config("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive","true") \
            .config("hive.mapred.supports.subdirectories","true") \
            .getOrCreate()

        # Update DDB item with RUNNING state
        notify_running(spark.sparkContext)

        # Begin preparing the final query and create a DataFrame out of them.
        q1 = 'select country, cast(day as date), numberofcovidtweetsretrieved as numberofcovidtweetsretrieved from {} where country != "" AND cast(day as date) between cast("{}" as date) and cast("{}" as date)'.format(etl2_table, that_day, yesterday)
        # Twitter dataframe
        df1 = spark.sql(q1)

        q2 = 'select country, cast(concat(month, "-", day) as date) as dt, sum(confirmed) as confirmed, sum(deaths) as deaths, sum(recovered) as recovered from {} where {} group by country, dt'.format(etl1_table, get_where_clause())
        # Github dataframe
        df2 = spark.sql(q2)

        df3 = df2.join(df1, (df1.country == df2.country) & (df1.day == df2.dt)).select(df1.country, df1.day, df1.numberofcovidtweetsretrieved, df2.confirmed, df2.deaths, df2.recovered)

        df3.repartition(1).write.mode("append").option("path", output_path).format("Parquet").saveAsTable(output_table)

        # To write the data to MySQL RDS. For this, make sure to pass --jars /usr/share/java/mysql-connector-java.jar with spark-submit.
        # df3.write.format('jdbc').options(driver='com.mysql.jdbc.Driver', url='jdbc:mysql://<hostname>:3306/<database>', dbtable='<table-name>', user='<user>', password='<password>').mode('append').save()
        print("Wrote the output!")

        # Update DDB record with COMPLETED state
        notify_completion()
    except Exception as e:
        print(e)
        # Update DDB record with FAILED state
        notify_failure(e)

# Main Method Caller
if __name__ == '__main__':
    main()