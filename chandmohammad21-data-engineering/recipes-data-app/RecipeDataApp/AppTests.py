import json
import os
import unittest
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from RecipeDataApp.Helper import Logger, HelperFunctions, RunConfiguration
from chispa.dataframe_comparer import *


def data_location(func_name: str) -> str:
    """
    Method fetches test data location by providing the function/method name
    :param func_name: (string) function/method name
    :return: (string) test data path
    """
    return os.path.join(os.getcwd(), "../testingData", "AppTests", "func_" + func_name)


def file_loc(func_name: str, location: str, file_name: str) -> str:
    """
    Method gives the complete file path for a file
    :param func_name: (string) function name where file is present
    :param location: (string) directory of file
    :param file_name: (string) file name which needs to be loaded
    :return: (string) complete file path
    """
    return "{0}\\{1}\\{2}".format(data_location(func_name), location, file_name)


def reader(spark: SparkSession, file_format: str, header: bool, path: str) -> DataFrame:
    """
    Method used to read a file and return a Dataframe
    :param spark: (SparkSession) spark session to the application
    :param file_format: (string) file format of the file
    :param header: (bool) does the file has headers?
    :param path: (string) file path
    :return: (DataFrame) the dataframe of the loaded file
    """
    if file_format.lower() == "csv":
        return spark.read.format(file_format).option("header", header).load(path)
    else:
        return spark.read.format(file_format).load(path)


class PySparkTestCase(unittest.TestCase):
    spark = SparkSession.builder.appName("App Unit Tests").master("local").getOrCreate()
    logger = Logger().get_logger()
    runConfigJson = '{"slackTitle":"Data Alerts", ' \
                    '"baseFolder":"chandmohammad21-data-engineering", ' \
                    '"appName":"DataApp"} '
    runConfiguration = json.loads(runConfigJson)
    inputObject = RunConfiguration(runConfiguration['slackTitle'],
                                   runConfiguration['baseFolder'],
                                   runConfiguration['appName'])

    def test_func_slack_notification(self):
        expected_result = True
        actual_result = HelperFunctions(self.logger, self.inputObject) \
            .slack_notification(self.inputObject.slackTitle,
                                "Message=SUCCESS: {}"
                                .format("Sent from Unit Test Cases"))
        assert expected_result == actual_result

    def test_func_data_transform_beef(self):
        input_file_df = reader(self.spark, "json", False,
                               file_loc("data_transform_beef", "inputs", "input"))
        actual_result = HelperFunctions.data_transform_beef(input_file_df)
        expected_result = reader(self.spark, "json", False,
                                 file_loc("data_transform_beef", "answers", "answer"))
        assert_df_equality(actual_result, expected_result,
                           ignore_row_order=False, ignore_column_order=True)

    def test_func_data_transform_time_minutes(self):
        input_file_df = reader(self.spark, "json", True,
                               file_loc("data_transform_time_minutes", "inputs", "input"))
        actual_result = HelperFunctions.data_transform_time_minutes(input_file_df)
        expected_result = reader(self.spark, "json", True,
                                 file_loc("data_transform_time_minutes", "answers", "answer"))
        assert_df_equality(actual_result, expected_result,
                           ignore_column_order=True, ignore_row_order=False)

    def test_func_data_transform_total_cooking_time(self):
        input_file_df = reader(self.spark, "json", True,
                               file_loc("data_transform_total_cooking_time", "inputs", "input"))
        actual_result = HelperFunctions.data_transform_total_cooking_time(input_file_df)
        expected_result = reader(self.spark, "json", True,
                                 file_loc("data_transform_total_cooking_time", "answers", "answer"))
        assert_df_equality(actual_result, expected_result,
                           ignore_column_order=True, ignore_row_order=False)

    def test_func_data_transform_add_difficulty(self):
        input_file_df = reader(self.spark, "json", True,
                               file_loc("data_transform_add_difficulty", "inputs", "input"))
        actual_result = HelperFunctions.data_transform_add_difficulty(input_file_df)
        expected_result = reader(self.spark, "json", True,
                                 file_loc("data_transform_add_difficulty", "answers", "answer"))
        assert_df_equality(actual_result, expected_result, ignore_column_order=True,
                           ignore_row_order=False, ignore_nullable=True)

    def test_func_data_transform_aggregate(self):
        input_file_df = reader(self.spark, "json", True,
                               file_loc("data_transform_aggregate", "inputs", "input"))
        actual_result = HelperFunctions.data_transform_aggregate(input_file_df)
        expected_result = reader(self.spark, "json", True,
                                 file_loc("data_transform_aggregate", "answers", "answer"))
        assert_df_equality(actual_result, expected_result,
                           ignore_column_order=True, ignore_row_order=False)


if __name__ == '__main__':
    unittest.main()