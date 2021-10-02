# Importing required modules and libraries
from RecipeDataApp.Helper import Logger, HelperFunctions, RunConfiguration
import json
from datetime import datetime
import argparse


if __name__ == "__main__":
    # getting logger instance to log the progress of data pipeline
    logger = Logger.get_logger()
    logger.info("Message=SUCCESS: {}".format("Application started"))

    # run configuration, this will be sent from outside of application as an argument
    runConfigJson = '{"slackTitle":"Data Alerts", ' \
                    '"baseFolder":"chandmohammad21-data-engineering", ' \
                    '"appName":"DataApp"} '
    runConfiguration = RunConfiguration(str(None), str(None), str(None))

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
    inputObject = RunConfiguration(runConfiguration['slackTitle'],
                                   runConfiguration['baseFolder'],
                                   runConfiguration['appName'])

    # sending out initial start notification over slack
    HelperFunctions(logger, inputObject) \
        .slack_notification(inputObject.slackTitle,
                            "Message=SUCCESS: {} {}"
                            .format("Data pipeline started successfully at", datetime.now()))

    # creating spark session
    spark = HelperFunctions(logger, inputObject) \
        .session_builder(inputObject.appName)
    logger.info("Message=SUCCESS: {}"
                .format("Spark Session created successfully"))

    # getting inputs and output file paths
    input_data_path, output_data_path = HelperFunctions(logger, inputObject) \
        .build_path()
    logger.info("Message=SUCCESS: Input path: {0}, Output path:{1}"
                .format(input_data_path, output_data_path))

    # ingesting source data into dataframes
    input_data_df = HelperFunctions(logger, inputObject) \
        .data_read(spark, input_data_path)
    logger.info("Message=SUCCESS: {}"
                .format("Data ingested successfully"))

    # persisting dataframe in memory
    input_data_df.cache()

    # sending out ingestion notification over slack
    HelperFunctions(logger, inputObject) \
        .slack_notification(inputObject.slackTitle,
                            "Message=SUCCESS: {}"
                            .format("Data ingestion completed successfully"))

    # data transformations 1 - Filter beef ingredients
    transformed_data_1 = HelperFunctions \
        .data_transform_beef(input_data_df)

    # data transformations 2 - convert ISO to Minutes duration
    transformed_data_2 = HelperFunctions \
        .data_transform_time_minutes(transformed_data_1)

    # data transformations 3 - add difficulty level
    transformed_data_3 = HelperFunctions \
        .data_transform_add_difficulty(transformed_data_2)

    # data transformations 4 - calculate total cooking time
    transformed_data_4 = HelperFunctions \
        .data_transform_total_cooking_time(transformed_data_3)

    # data transformations 4 - aggregate based on difficulty level
    transformed_data_5 = HelperFunctions \
        .data_transform_aggregate(transformed_data_4)

    logger.info("Message=SUCCESS: {}"
                .format("Data transformed successfully"))

    # sending out transformation notification over slack
    HelperFunctions(logger, inputObject) \
        .slack_notification(inputObject.slackTitle,
                            "Message=SUCCESS: {}"
                            .format("Data transformation completed successfully"))

    # persist/write dataframe to filesystem
    is_df_written = HelperFunctions(logger, inputObject) \
        .data_write(transformed_data_5, output_data_path)
    logger.info("Message=SUCCESS: {0}: {1}"
                .format("Data loaded/persisted successfully", is_df_written))

    # sending out loading notification over slack
    HelperFunctions(logger, inputObject) \
        .slack_notification(inputObject.slackTitle,
                            "Message=SUCCESS: {} {}"
                            .format("Data pipeline completed successfully at", datetime.now()))