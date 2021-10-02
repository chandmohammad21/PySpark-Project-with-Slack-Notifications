from dataclasses import dataclass
from typing import Tuple
import logging
from abc import ABC, abstractmethod
from pydeequ.checks import *
from pydeequ.verification import *
from pyspark.sql.functions import col, lower, upper, concat, lit, monotonically_increasing_id


@dataclass
class RunConfiguration:
    baseFolder: str
    fleName: str


class FileInfo(ABC):
    def __init__(self, log: logging.RootLogger):
        self.log = log

    @abstractmethod
    def get_checks(self, file_name: str, spark: SparkSession):
        pass


class Recipe(FileInfo):
    def get_checks(self, file_name: str, spark: SparkSession) -> Check:
        """
        Method gives Check object which contains the test case checks for file Recipe
        :param file_name: (string) file name to be validated
        :param spark: (SparkSession) spark session of the application
        :return: (Check) Check object
        """
        try:
            return Check(spark, CheckLevel.Error, file_name) \
                .isComplete("name") \
                .isComplete("cookTime") \
                .isComplete("prepTime") \
                .isComplete("ingredients") \
                .isComplete("image") \
                .isComplete("url") \
                .isUnique("name") \
                .isUnique("ingredients") \
                .isUnique("url")
        except Exception as e:
            self.log.error("Message=Error: {}".format(str(e)))
            raise Exception("Failed in Recipe.get_checks for file: {}:{})"
                            .format(file_name, str(e)))


class RecipeAggregate(FileInfo):
    def get_checks(self, file_name: str, spark: SparkSession) -> Check:
        """
        Method gives Check object which contains the test case checks for file Recipe aggregates
        :param file_name: (string) file name to be validated
        :param spark: (SparkSession) spark session of the application
        :return: (Check) Check object
        """
        try:
            return Check(spark, CheckLevel.Error, file_name) \
                .isComplete("difficulty") \
                .isComplete("avg_total_cooking_time") \
                .isContainedIn("difficulty", ['easy', 'medium', 'hard']) \
                .isUnique("difficulty") \
                .isNonNegative("avg_total_cooking_time")
        except Exception as e:
            self.log.error("Message=Error: {}".format(str(e)))
            raise Exception("Failed in RecipeAggregate.get_checks for file: {}: {}"
                            .format(file_name, str(e)))


class DataQualityHelperFunctions(object):
    def __init__(self, log: logging.RootLogger, run_configuration: RunConfiguration):
        """
        Initialization method.
        :param log: Logging instance
        :param run_configuration: RunConfiguration instance
        """
        self.log = log
        self.run_configuration = run_configuration

    def get_file(self, name: str, spark: SparkSession):
        """
        Method to get FileInfo object
        :param name: (string) file name to be validated
        :param spark: (SparkSession) spark session of the application
        :return: (FileInfo)
        """
        if name == 'recipe':
            return Recipe(self.log)
        elif name == 'recipe_aggregate':
            return RecipeAggregate(self.log)
        else:
            raise Exception("Failed in get_file with Invalid file name: {}"
                            .format(name))

    def build_path(self) -> Tuple[str, str]:
        """
        Method will build path of various files
        :return: (Tuple) of inputs/output/test results path
        """
        # create root variable to hardcode base location of cloud storage
        root = "C:/GitHub/Data/"
        input_path = root + self.run_configuration.baseFolder + '/' + '/'.join(
            self.run_configuration.fleName.split('|'))
        validation_report = root + self.run_configuration.baseFolder + '/' + '/'.join(
            self.run_configuration.fleName.split('|')) + '_' + 'validation_report'
        return input_path, validation_report

    def run_verification(self, file_name: str, file: FileInfo, df: DataFrame,
                         spark: SparkSession) -> VerificationResult:
        """
        Method runs the verification
        :param file_name: (string) file name to be validated
        :param file: (FileInfo)
        :param df: (DataFrame)
        :param spark: (SparkSession) spark session of the application
        :return: (VerificationResult)
        """
        try:
            return VerificationSuite(spark) \
                .onData(df) \
                .addCheck(file.get_checks(file_name, spark)) \
                .run()
        except Exception as e:
            self.log.error("Message=Error: {}".format(str(e)))
            raise Exception("Failed in run_verification for file: {}: {}"
                            .format(file_name, str(e)))

    def reformat_test_report(self, df: DataFrame) -> DataFrame:
        """
        Method reformat the test results as per requirements
        :param df: (DataFrame)
        :return: (DataFrame)
        """
        try:
            return df \
                .withColumn("Test_Case_Id", concat(upper(col("check")),
                                                   lit("_TC_"), monotonically_increasing_id())) \
                .select(upper(col("check")).alias("File_Name"),
                        col("Test_Case_Id"),
                        col("constraint").alias("Test_Case"),
                        col("constraint_status").alias("Test_Result"))
        except Exception as e:
            self.log.error("Message=Error: {}".format(str(e)))
            raise Exception("Failed in reformat_test_report for file: {}"
                            .format(str(e)))
