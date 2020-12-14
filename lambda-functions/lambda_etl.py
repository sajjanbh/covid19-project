import os, json
import boto3
import datetime

# Global values get from Environment variables
cluster_name = os.environ.get('cluster_name')
ddb_table = os.environ.get('ddb_table')
script_location = os.environ.get('script_location') # will be populated for each ETL job
output_path = os.environ.get('output_path')
region = os.environ.get("AWS_REGION")

# Specified to ETL1
input_path = os.environ.get('github_input_path')

# Remove trailing slash from path if specified
if script_location.endswith('/'): script_location = script_location[:-1]
if output_path.endswith('/'): output_path = output_path[:-1]
if input_path.endswith('/'): input_path = input_path[:-1]

# Specific to ETL2
glue_db = os.environ.get('glue_db')
glue_table = os.environ.get('glue_table')

# Check for all required ENV variables. If not present, stop execution.
if not cluster_name or not ddb_table or not script_location or not output_path or not input_path or not glue_db or not glue_table:
    print("Not all required environment variables are present!")
    exit()

# Instantiate AWS service clients
glue = boto3.client("glue")
emr = boto3.client('emr')
ddb = boto3.client('dynamodb')
    
def get_emr_cluster_id():
    try:
        # Fetch the cluster ID
        clusters = emr.list_clusters(ClusterStates=["RUNNING", "WAITING", "STARTING"])
        for cluster in clusters['Clusters']:
            if cluster['Name'] == cluster_name:
                print("Cluster {} found!".format(cluster['Id']))
                return cluster['Id']
        print("No valid cluster found!")
        return None
    except Exception as e:
        print(e)
        return None

def add_emr_step(cluster_id, name, args):
    try:
        response = emr.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[
                {
                    'Name': name,
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Args': args,
                        'Jar': 'command-runner.jar'
                    }
                }
            ]
        )
        step_id = response["StepIds"][0]
        print("Submitted {}".format(step_id))
        return step_id
    except Exception as e:
        print(e)
        return None
        
def check_emr_step_status(cluster_id, step_id, submit_time, job_date, etl):
    try:
        response = emr.describe_step(ClusterId=cluster_id, StepId=step_id)
        status = response['Step']['Status']['State']
        print("{} state is {}".format(step_id, status))
        if status == "PENDING" or status == "RUNNING":
            return True
        else:
            # Let's try to update the DDB item to retain previous submission time for record-keeping.
            try:
                uexp = "SET previous_submits_on = list_append(if_not_exists(previous_submits_on, :empty_list), :i)"
                ex_attr_values = {
                    ":empty_list": {"L": []},
                    ":i": {"L": [{"N": submit_time}]}
                }
                ddb.update_item(TableName=ddb_table, Key={'date': {'S': job_date}, 'etl': {'N': etl}}, UpdateExpression=uexp, ExpressionAttributeValues=ex_attr_values)
            except Exception as e:
                print("DDB update failed")
                print(e)
            return False
    except Exception as e: print(e)
    
def check_if_already_processed(etl, job_date):
    try:
        items = ddb.get_item(TableName=ddb_table, Key={'date': {'S': job_date}, 'etl': {'N': etl}})
        item = items.get('Item', None)
        if item:
            print("Record exists for {}".format(job_date))
            job_status = item.get('status', None)['S']
            if job_status == "COMPLETED":
                print("Record already completed processing!")
                return item
            elif job_status == "SUBMITTED":
                print("Job was already submitted!")
                # Check whether the Step is in active or inactive state. If inactive, but not completed, let's run it again.
                return check_emr_step_status(item.get('cluster_id')['S'], item.get('step_id')['S'], item.get('submit_time')['N'], job_date, etl)
            elif job_status == "RUNNING":
                print("Job is in running state! Not re-submitting now.")
                if not check_emr_step_status(item.get('cluster_id')['S'], item.get('step_id')['S'], item.get('submit_time')['N'], job_date, etl):
                    return False
                else:
                    return item
            elif job_status == "FAILED":
                print("Job failed! Re-submitting!")
                return False
        else:
            return False
    except Exception as e:
        print(e)
        return True

def put_record_in_ddb(etl, cluster_id, job_date, step_id):
    now = str(datetime.datetime.today().timestamp())
    item = {
        'cluster_id': {'Value': {'S': cluster_id}},
        'step_id': {'Value': {'S': step_id}},
        'submit_time': {'Value': {'N': now}},
        'status': {'Value': {'S': 'SUBMITTED'}}
    }
    try:
        response = ddb.update_item(TableName=ddb_table, Key={'date': {'S': job_date}, 'etl': {'N': etl}}, AttributeUpdates=item)
        print("DDB Put Status: {}".format(response['ResponseMetadata']['HTTPStatusCode']))
    except Exception as e: print(e)
    
def get_glue_table():
    try:
        table = glue.get_table(DatabaseName=glue_db, Name=glue_table)
        if table:
            return table['Table']
    except Exception as e:
        print(e)
        return None
        
def add_glue_partition(year, month, day):
    table = get_glue_table()
    if table:
        values = [year, month, day]
        sd = table['StorageDescriptor']
        sd['Location'] = "{}/{}/{}/{}/".format(sd['Location'], year, month, day)
        part_input = {'Values': values, 'StorageDescriptor': sd}
        try:
            partition = glue.create_partition(DatabaseName=glue_db, TableName=glue_table, PartitionInput=part_input)
            if partition: 
                print("Partition {}/{}/{} added!".format(year, month, day))
                return True
            else: 
                print("Partition {}/{}/{} failed to add!".format(year, month, day))
                return None
        except Exception as e:
            if "AlreadyExistsException" in str(e):
                print("Partition {}/{}/{} already exists! Continuing...".format(year, month, day))
                return True
            else:
                print(e)
                return None
    else: return None

def lambda_handler(event, context):
    if event.get('year', None) and event.get('month', None) and event.get('day', None):
        # Used to back-fill old data.
        year = event['year']
        month = event['month']
        day = event['day']
        today = datetime.datetime.strptime('{}-{}-{}'.format(year, month, day), '%Y-%m-%d')
    else:
        # For Cloudwatch trigger
        today = datetime.datetime.today()
        yesterday = today - datetime.timedelta(days=1)
        year = str(yesterday.year)
        month = str("{0:0=2d}".format(yesterday.month))
        day = str("{0:0=2d}".format(yesterday.day))
    
    etls=['1', '2']
    
    job_date = "{}-{}-{}".format(year, month, day)
    
    # If it's Wednesday, let's run ETL3 as well.
    if today.strftime("%A") == "Wednesday":
        print("{} is wednesday!".format(job_date))
        etls.append('3')
    
    cluster_id = get_emr_cluster_id()
    if not cluster_id:
        print("No cluster found with name {}".format(cluster_name))
        return True
    
    for etl in etls:
        print("ETL {}".format(etl))
        if etl == '2':
            if not add_glue_partition(year, month, day):
                continue
            
        previous = check_if_already_processed(etl, job_date)
        if previous:
            print(previous)
            continue

        queue = "etl{}".format(etl) if etl == "1" or etl == "2" else "interactive"
        step_args = ['spark-submit',
                        '--queue', queue,
                        '--deploy-mode', 'cluster',
                        '--proxy-user', 'etl'
                    ]
        if etl == '3':
            step_args.extend(['--jars', '/usr/share/java/mysql-connector-java.jar'])
        step_args.extend([
                        '{}/etl{}.py'.format(script_location, etl),
                        '--date', job_date,
                        '--input-path', input_path,
                        '--output-path', '{}/etl{}'.format(output_path, etl),
                        '--ddb-table', ddb_table,
                        '--glue-database', glue_db,
                        '--glue-table', glue_table,
                        '--region', region
                    ])
        step_name = 'ETL{}-{}-{}-{}'.format(etl, year, month, day)
        print("Step {}: {}".format(step_name, " ".join(step_args)))
        
        step_id = add_emr_step(cluster_id, step_name, step_args)
        if step_id:
            put_record_in_ddb(etl, cluster_id, job_date, step_id)
    
    return {
        'statusCode': 200,
        'body': "Submitted {}".format(job_date)
    }
