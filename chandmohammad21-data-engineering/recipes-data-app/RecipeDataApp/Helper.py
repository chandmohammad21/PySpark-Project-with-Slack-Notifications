from dataclasses import dataclass
import logging
from typing import Tuple
import pydeequ
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lower, udf, when, lit, avg
import json
import sys
import requests
import isodate


@dataclass
class RunConfiguration:
    slackTitle: str
    baseFolder: str
    appName: str


class Logger(object):
    @staticmethod
    def get_logger():
        """
        This method will give the logging instance for further logging use.
        :param: None
        :return: logger
        """
        # create logger
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        # create console handler and set level to debug
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # create formatter
        formatter = logging.Formatter('%(asctime)s %(levelname)s: ' + 'Line - ' + '%(lineno)d' + ':' + '%(message)s',
                                      "%Y%m%d%H%M%S")

        # add formatter to handler
        console_handler.setFormatter(formatter)

        # add handlers to loggers
        logger.addHandler(console_handler)
        return logger


class HelperFunctions(object):
    def __init__(self, log: logging.RootLogger, run_configuration: RunConfiguration):
        """
        Initialization method.
        :param log: Logging instance
        :param run_configuration: RunConfiguration instance
        """
        self.log = log
        self.run_configuration = run_configuration

    @staticmethod
    def convert_iso_duration(val: str) -> int:
        return isodate.parse_duration(val).seconds

    def slack_notification(self, title: str, message: str) -> bool:
        """
        This method will send out slack notification for alerts in the data pipeline
        :param title: (string) Title of the slack notification
        :param message: (string) Message that will be shared in Slack Channel
        :return: bool
        """
        # Endpoint URL of the slack services
        url = "https://hooks.slack.com/services/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"

        # Json body of slack data to be sent over the request
        slack_data = {
            "username": "DataNotification",
            "icon_emoji": ":satellite:",
            "channel": "data-ontification",
            "attachments": [
                {
                    "color": "#9733EE",
                    "fields": [
                        {
                            "title": title,
                            "value": message,
                            "short": "false",
                        }
                    ]
                }
            ]
        }
        byte_length = str(sys.getsizeof(slack_data))

        # Creating headers of the request
        headers = {'Content-Type': "application/json",
                   'Content-Length': byte_length}

        # Sending request and getting response
        response = ""
        try:
            response = requests.post(url,
                                     data=json.dumps(slack_data),
                                     headers=headers)
        except Exception as e:
            self.log.warning("Message=WARNING: Incorrect Slack Notification request: {}"
                             .format(str(e)))
            return False

        if response.status_code != 200:
            self.log.warning("Message=WARNING: Notification sent via Slack: {}".format(False))
            return False
        else:
            self.log.info("Message=SUCCESS: Notification sent via Slack: {}".format(True))
            return True

    def session_builder(self, app_name: str) -> SparkSession:
        """
        Method used to create a spark session
        :param app_name: (string) spark application name
        :return: SparkSession
        """
        try:
            return SparkSession \
                .builder \
                .config("spark.jars.packages", pydeequ.deequ_maven_coord) \
                .config("spark.jars.excludes", pydeequ.f2j_maven_coord) \
                .master("local") \
                .appName(app_name) \
                .getOrCreate()
        except Exception as e:
            self.log.error("Message=Error: {}".format(str(e)))

            # sending out failure notification over slack
            HelperFunctions(self.log, self.run_configuration) \
                .slack_notification(self.run_configuration.slackTitle,
                                    "Message=ERROR: {}"
                                    .format("Data pipeline failed while spark session building"))
            raise Exception("Failed in session_builder: {}"
                            .format(str(e)))

    def build_path(self) -> Tuple[str, str]:
        """
        Method will build path of various files
        :return: (Tuple) of inputs/output/test results path
        """
        # create root variable to hardcode base location of cloud storage
        root = "C:/GitHub/Data/"
        input_path = root + self.run_configuration.baseFolder + '/' + 'input'
        output_path = root + self.run_configuration.baseFolder + '/' + 'output'
        return input_path, output_path

    def data_read(self, spark_session: SparkSession, file_path: str, file_format="json") -> DataFrame:
        """
        Method is used to read data from physical disc and returns DataFrame
        :param spark_session: (SparkSession) Spark session to the application
        :param file_path: (string) file path from where data to be read
        :param file_format: (string) with default format as json
        :return: DataFrame
        """
        try:
            return spark_session \
                .read.format(file_format) \
                .option("multiLine", "False") \
                .json(file_path)
        except Exception as e:
            self.log.error("Message=Error: {}".format(str(e)))

            # sending out failure notification over slack
            HelperFunctions(self.log, self.run_configuration) \
                .slack_notification(self.run_configuration.slackTitle,
                                    "Message=ERROR: {}"
                                    .format("Data pipeline failed while data ingestion"))
            raise Exception("Failed in data_read: {}"
                            .format(str(e)))

    @staticmethod
    def data_transform_beef(df: DataFrame) -> DataFrame:
        """
        Method to make the required transformations, filter only beef ingredients
        :param df: (DataFrame) inputs dataframe
        :return: (DataFrame) transformed dataframe
        """
        # extraction of beef only recipes
        beef_only_df = df \
            .filter("lower(ingredients) like '%beef%'")

        return beef_only_df

    @staticmethod
    def data_transform_time_minutes(df: DataFrame) -> DataFrame:
        """
        Method to make the required transformations, convert ISO duration to minutes
        :param df: (DataFrame) inputs dataframe
        :return: (DataFrame) transformed dataframe
        """

        # udf to convert ISO format duration to seconds
        convert_udf = udf(lambda z: HelperFunctions.convert_iso_duration(z))

        # data with extra columns that have minutes as cook duration
        recipes_minutes_df = df \
            .withColumn("prepTimeMinutes", (convert_udf(col("prepTime")) / 60).cast("Long")) \
            .withColumn("cookTimeMinutes", (convert_udf(col("cookTime")) / 60).cast("Long"))

        return recipes_minutes_df

    @staticmethod
    def data_transform_total_cooking_time(df: DataFrame) -> DataFrame:
        """
        Method to make the required transformations, calculate total cooking time
        :param df: (DataFrame) inputs dataframe
        :return: (DataFrame) transformed dataframe
        """

        # calculating total cooking time
        recipes_total_cooking_time = df \
            .withColumn("total_cook_time", (col("cookTimeMinutes") + col("prepTimeMinutes")))

        return recipes_total_cooking_time

    @staticmethod
    def data_transform_add_difficulty(df: DataFrame) -> DataFrame:
        """
        Method to make the required transformations, add difficulty based on cooking time
        :param df: (DataFrame) inputs dataframe
        :return: (DataFrame) transformed dataframe
        """

        # calculating total cooking time
        recipes_total_cooking_time = df \
            .withColumn("total_cook_time", (col("cookTimeMinutes") + col("prepTimeMinutes")))

        # adding difficulty level based on total cook time
        recipes_difficulty_levels = recipes_total_cooking_time \
            .withColumn("difficulty", when(col("total_cook_time") < 30, lit("easy"))
                        .when(col("total_cook_time").between(30, 60), lit("medium"))
                        .otherwise(lit("hard")))

        return recipes_difficulty_levels

    @staticmethod
    def data_transform_aggregate(df: DataFrame) -> DataFrame:
        """
        Method to make the required transformations, aggregations based on difficulty level
        :param df: (DataFrame) inputs dataframe
        :return: (DataFrame) transformed dataframe
        """

        # Calculate average cooking time duration per difficulty level
        avg_cooking_difficulty = df \
            .select("difficulty", "total_cook_time") \
            .groupBy("difficulty") \
            .agg(avg("total_cook_time")
                 .alias("avg_total_cooking_time"))
        return avg_cooking_difficulty

    def data_write(self, df: DataFrame, file_path: str, file_format="csv") -> bool:
        """
        Method writes/persist dataframe to files in respective file path
        :param df: (DataFrame) Dataframe to be written to file
        :param file_path: (string) path where file to be writted
        :param file_format: (string) default as csv
        :return: (bool)
        """
        try:
            df \
                .coalesce(1) \
                .write.option("header", "True") \
                .mode(saveMode="overwrite") \
                .format(file_format) \
                .save(file_path)
            return True
        except Exception as e:
            self.log.error("Message=Error: {}".format(str(e)))

            # sending out failure notification over slack
            HelperFunctions(self.log, self.run_configuration) \
                .slack_notification(self.run_configuration.slackTitle,
                                    "Message=ERROR: {}"
                                    .format("Data pipeline failed while data loading"))
            raise Exception("Failed in data_write: {}"
                            .format(str(e)))
