<!-- TABLE OF CONTENTS -->
<details open="open">
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
    </li>
    <li>
    	<a href="#components">Components</a>
    </li>
    <li>
      <a href="#architecture">Architecture</a>
    </li>
  </ol>
</details>


<!-- ABOUT THE PROJECT -->
## About The Project
The main objective of this project is to analyze the Covid-19 related data and draw analytics using BigData and Serverless technologies. It utilizes two data sources: John Hopkins University's Covid-19 data published in Github and Twitter tweets related to Covid-19. Then, these data are ingested, processed and analyzed in scheduled basis usng various AWS services.

## Components
1. We’ve 2 data sources here, which are then written to our main S3 bucket (datalake):
- Covid-19 stats: Maintained and published by John Hopkins University and available in [this Github repository](https://github.com/CSSEGISandData/COVID-19). It is updated in daily basis and contains very useful information such as total confirmed cases, deaths, recovered cases, incident rate, case fatality ratio by country, state and coordinates.
- Twitter data: These are mainly tweets related to Covid-19. These arrive in almost real-time and are fetched via Twitter Developer method and written to S3 bucket using service like Kinesis Firehose.
2. After the data come into the S3 bucket, we’ll run our applications on top of these data. For this, Spark is being used on top of Elastic Map Reduce (EMR) service. Spark will read the incoming raw data, process them into desired layout/format and then write the processed results back to S3. While doing so, it’ll also utilize Glue Data Catalog to store the table metadata.
3. Basically, above 2 are the main components to get this project going. Nevertheless, it is better to implement some additional mechanism to properly automate, monitor and manage the workflow. For this, I’m introducing below components:
- Lambda Function to submit Spark job to EMR via Step. This function is one of the main components in this architecture as it determines which job to submit, when and how. It also puts a record in predefined DynamoDB table to keep track of jobs submitted to EMR. Additionally, it also has logic to handle several scenarios such as determining concerned EMR cluster, adding required partition into Glue table, drafting arguments to pass with job submission, preventing duplicate job submission, and so on.
- To invoke above Lambda function in daily basis, a CloudWatch scheduled rule is used. Whereas to process past data, we’ll invoke this function manually or with a script.
- As mentioned above, we use DynamoDB table here to keep track of the ETL jobs. This is updated by above Lambda function as well as the Spark ETL jobs. And we also enable Stream on this table and use it to trigger another Lambda function, which is designed to send notifications. So that, whenever an item gets updated in DynamoDB table, it’ll invoke the function.
- The notifier function takes the updated DynamoDB record and pushes it to destination, which in this case is Amazon Chime chatroom. Similarly, other communication tools like Slack, Team, etc. can also be used to receive notifications. This function also does some formatting to make the notifications more user friendly.
4. Then, we get into another part of this project, which is to analyze and visualize the processed data. As can be seen in the diagram, we’ll be using Athena and QuickSight for this purpose. And lastly to share the [visualization dashboard](https://covid19.sajjan.com.np/) with our users, we’ll be utilizing yet another Lambda function and API Gateway, and then embed it to a static webpage hosted in S3 bucket.
5. Finally, to wrap this up, we’ll be writing a CloudFormation template(s) to launch all these components as a single stack. This way, it can be easily automated and managed.

## Architecture
![alt Architecture Diagram](https://github.com/sajjanbh/covid19-project/blob/master/Architecture-Diagram.jpg?raw=true)