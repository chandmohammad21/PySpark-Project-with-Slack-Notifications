# Data engineering Project (Data quality validation, Unit Tests and Slack Notifications included)

## Design
Application started -> Slack alert -> Source Raw Data -> Automated source data quality validation -> Data ingestion -> Slack alert -> Data transformation -> Slack alert -> Data load -> slack alert -> Automated output data quality validation

## Notifications
Application will be sending notification alerts via slack on important milestones stages and errors.
*Processing starts -> Slack alert -> Data ingestion -> Slack alert -> Data transformation -> Slack alert -> Data loaded -> Slack alert.*
Beside the above whenever an error is encountered, slack alert is sent.

## Approach
1. Data ingestion: Data can be ingested from various sources like AWS S3, Azure Storage, Google storage etc. For this assignment, we are ingesting our data from local file system using spark. (In case of data ingestion from cloud, authorization and authentication will come into picture)<br>
*source data location: chandmohammad21-data-engineering\input*

2. Automated source data quality validation: Distrubuted and scalable automated data quality framework has been written using python library AWS Pydeequ. This is reusable and used to validate data quality of output as well. Various quality test cases that are used are:
    - Not Null or completeness tests for mandatory columns
    - Non negetive validation for fields which cannot be negative
    - Fields with specific values only, example difficulty should have only easy, medium and hard
    - Uniqueness of fields like name, ingresients, picture, URL etc.

    The functionality is not limited to these test cases and can be a lot more.<br>
    *Validation report: chandmohammad21-data-engineering\data_quality_files\recipe_data\recipe_validation_report, chandmohammad21-data-engineering\data_quality_files\recipe_output\recipe_aggregate_validation_report*
    
3. Data transformation: Data transformation is done as part of multi functions for an effective unit testing.
    - data_transform_beef: takes raw source and returns beef only recipes
    - data_transform_time_minutes: uses a udf and converts time duration from OSI 8601 format to seconds
    - data_transform_total_cooking_time: calculates and add column, total cooking time = prep time + cookiing time
    - data_transform_add_difficulty: adds up difficulty level based on total cooking time
    - data_transform_aggregate: does all the aggregation and returns data in desired form, difficulty, average cooking time at difficulty level
4. Data load: Final step is to persist the transformed data on disk in csv format.<br>
*output data location: chandmohammad21-data-engineering\output*
5. Logging: Logging can be done by custom logging framework or generic available like log4j. The one used in the assignment is python logging library. (Ignore log4j.properties, it is placed to suppress warnings)
6. Exception handling: Exceptions are handled and raised whenever required in the code. As of now, just to save time and complete the assignment ASAP, have used general Exception syntax which should be replaced by specific exceptions for production grade code.
7. Unit Testing: This is performed on data persent in *chandmohammad21-data-engineering\recipes-data-app\testingData\AppTests*
8. Notifications as described in above section.

## Assumptions and considerations:
1. Alert notification would be required on important stages and failures.
2. Input files will be placed in input directory having uniform schema for all the files
3. There is no PII data in the source files that needs to be masked.
3. JSON files with format = multiline=False, this will stand true for all the files
4. File fields such as prep_time and cooking_time will have valid ISO 8601 time duration format
5. Input files would be coming tested from the upstream system and no other than mentioned transformation would be required in the assignment
6. Latest stable versions of the application, libraries, modules to be used in the assignemt
7. For difficulty level, medium between 30 and 60, boundary values are included
8. As a CSV output file, headers will also be provided.
9. Unit tests for spark application are in place however not for Automation framework. <br>
(Not included due to time constraint, in case you want to have look how they would look like, please go through https://github.com/chandmohammad21/DistributedTestAutomationDeequ/blob/main/src/test/scala/org/data2scoops/ValidateTests/DistributedDataValidationTests.scala)
10. At the moment, the application would run on local. Possible to run on cloud? Yes, after minimal changes done.

## Files, folders and descriptions
1. chandmohammad21-data-engineering\recipes-data-app\RecipeDataApp\App.py - This is the main application
2. chandmohammad21-data-engineering\recipes-data-app\RecipeDataApp\AppTests.py - This is the Unit tests file
3. chandmohammad21-data-engineering\recipes-data-app\RecipeDataApp\Helper.py - This is application helper module
4. chandmohammad21-data-engineering\recipes-data-app\DataQualityChecks\DataQuality.py - This is the Automation framework application file
5. chandmohammad21-data-engineering\recipes-data-app\DataQualityChecks\DataQualityHelper.py - This is the Automation framework helper module
6. chandmohammad21-data-engineering\input - Source files
7. chandmohammad21-data-engineering\output - output files
8. chandmohammad21-data-engineering\data_quality_files - Data files and data validation reports in csv
9. chandmohammad21-data-engineering\recipes-data-app\testingData\AppTests - Unit tests testing data

## Bonus points questions:
1. Config management: 
    - Spark Configs:
        - We can add them inside code during creation of spark session
        - We can provide them through command line ex. *spark-submit --master yarn --deploy-mode cluster --config "spark.sql.shuffle.partitions=100" App.py*
        - To run application in cloud, we can set these configs while creating cluster
    - Other configs: For other configs, we can write configs in key value pair format (Json, Yaml), import this file in our application and use them accordingly.
2. Data quality checks (like input/output dataset validation)
    - Created an automated data quality validation framework using AWS Deequ library to perform various data check activities like:
        - Data analysis report: mean, median, max, min, etc.
        - Data checks: Checks like Null, Not null, set of values, range, Uppercase, Lowercase, Trim, less than, greater than etc.
        - We can add custom data checks as well for eg. reconcilation of files, columns etc.
    - These can be integrated in data pipelines for seemless data validation on source and target data.
3. How would you implement CI/CD for this application?
    - CI:
        - Will create a process on respective platform (cloud/on prem) where CI will be triggered when there is a commit to any branch in GIT repository
        - CI will run all the unit tests, in case the test cases are passed, build will be successful else failed. (PySpark Tests with Maven or SBT)
        - CI will create/publish artifacts which will be stored in a repository to be consumed by CD pipeline for deployment.
    - CD:
        - CD will be triggered manually or continuously whenever there is a commit in the develop/master/hotfix/release branch after suficient required approvals.
        - CD will pick up latest build version or custom build version as per requirements and will deploy the artifacts in various environemts as stages.
        (Stage1=Test, Stage2=UAT, Stage3=Preview, Stage4=Prod)
         
    - Cloud: Have worked extensively on CI/CD devops build/release pipelines.

4. How would you diagnose and tune the application in case of performance problems?
    - Diagnosis:
        - Spark application diagnosis can be done via Spark UI. We can explore various tabs to see a probable performace or other issues.
        - Spark execution plan
        - Unusual number of tasks and stages for even a simple join which should take 2-3 stages
        - Driver and executor logs
    - Performance tuning:
        - Take advantage of project tungsten, use dataframe and datasets
        - Caching and broadcasting
        - Reduce data shuffles as much as possible, Coalesce vs partition
        - Take advantage of distributed compution (paritions), partition your data
        - Manage your data skewness
        - Data formats, Avro for full scan, parquet for columnar scan
        - Resource related improvements like powerful CPU's and memory efficient cluster instances, executor memory, driver memory etc etc
        - Experiment with various configurations like default parallelism, num shuffle partitions,
        - Avoid UDF's as much as possible and use inbuild functions
        - Serialization, Kyro is faster than java
5. How would you schedule this pipeline to run periodically?
    - I would use an orchestrator to run the application periodically without manual intervention. Airflow, Oozie, Azure Data factory are few good orchestators available.
    - We can also run a cron job which will trigger our application periodically but this won't be a feasible solution in case of large and complex data pipelines
    - We can run our application like a structured streaming application which runs baches periodically; this could be a good option if the frequency to run the application is high.

6. We appreciate good combination of Software and Data Engineering
    - I know it is not a question but a statement :)
    - I understand and agree that a data engineer has to be good in data domain as well as software engineering.
    - I make sure that i understand the requirement completely and correctly which helps me deliver the right asked product.

