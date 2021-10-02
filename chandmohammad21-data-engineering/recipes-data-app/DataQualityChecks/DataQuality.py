# Importing required modules and libraries
from DataQualityChecks.DataQualityHelper import RunConfiguration, DataQualityHelperFunctions
from RecipeDataApp.Helper import Logger, HelperFunctions
from pydeequ.verification import *
from pydeequ.suggestions import *


if __name__ == "__main__":
    # getting logger instance to log the progress of data pipeline
    logger = Logger().get_logger()
    logger.info("Message=SUCCESS: {}"
                .format("Data quality application started"))

    # run configuration, this will be sent from outside of application as an argument
    runConfigJson = '{"baseFolder":"chandmohammad21-data-engineering", ' \
                    '"fileName":"data_quality_files|recipe_output|recipe_aggregate"} '
    # data_quality_files|recipe_data|recipe & data_quality_files|recipe_output|recipe_aggregate - use this as
    # fileName to test final output file

    runConfiguration = RunConfiguration(str(None), str(None))

    # logging inputs configuration for audit/review purpose
    logger.info("Message=SUCCESS: {0}: {1}"
                .format("Json inputs arguments", runConfigJson))

    # creating json object from json string
    try:
        runConfiguration = json.loads(runConfigJson)
    except ValueError:
        logger.error("Message=Error: {}"
                     .format("Json decoding failed"))
        raise Exception("Failed in runConfiguration for config: {}"
                        .format(runConfigJson))
    else:
        logger.info("Message=SUCCESS: {}"
                    .format("Json parsing completed successfully"))

    # creating instance of RunConfiguration
    inputObject = RunConfiguration(runConfiguration['baseFolder'],
                                   runConfiguration['fileName'])

    # creating spark session
    spark = HelperFunctions(logger, inputObject) \
        .session_builder("Data Quality App")
    logger.info("Message=SUCCESS: {}"
                .format("Spark Session created successfully"))

    # getting inputs and data validation report paths
    input_path, validation_report_path = DataQualityHelperFunctions(logger, inputObject) \
        .build_path()
    logger.info("Message=SUCCESS: input path:{0} validation report path:{1}"
                .format(input_path, validation_report_path))

    # getting file data into spark dataframe
    data_df = HelperFunctions(logger, inputObject) \
        .data_read(spark, input_path)
    logger.info("Message=SUCCESS: {}"
                .format("Dataframe created successfully"))

    if data_df:
        logger.info("Message=SUCCESS: {}"
                    .format("Dataframe not empty, proceeding."))

        # deriving file name from input configuration
        file_name = inputObject.fleName.split("|")[-1].lower()
        logger.info("Message=SUCCESS: File name: {0}"
                    .format(file_name))

        # getting FileInfo object to get checks
        file = DataQualityHelperFunctions(logger, inputObject) \
            .get_file(file_name, spark)
        logger.info("Message=SUCCESS: {}"
                    .format("File object retrieved successfully"))

        # getting the results in Check format
        checkResult = DataQualityHelperFunctions(logger, inputObject) \
            .run_verification(file_name, file, data_df, spark)
        logger.info("Message=SUCCESS: {}"
                    .format("Check results created successfully"))

        # converting Check format result to a dataframe
        checkResult_df = VerificationResult \
            .checkResultsAsDataFrame(spark, checkResult)
        logger.info("Message=SUCCESS: {}"
                    .format("Check results dataframe created successfully"))

        # formatted test report for better visualization and understanding
        formatted_test_report = DataQualityHelperFunctions(logger, inputObject) \
            .reformat_test_report(checkResult_df)
        logger.info("Message=SUCCESS: {}"
                    .format("Report formatting done successfully"))

        # writing test report to output directory
        is_file_written = HelperFunctions(logger, inputObject) \
            .data_write(formatted_test_report,
                        validation_report_path, "csv")
        logger.info("Message=SUCCESS: {}"
                    .format("Test quality validation report created successfully"))
    else:
        logger.info("Message=WARN: {}"
                    .format("Data file is empty, No test quality report created"))
