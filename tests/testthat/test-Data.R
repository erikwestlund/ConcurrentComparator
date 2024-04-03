test_that("CohortExtraction.sql translates to supported SQL DMBS dialects", {
    supportedDbms <- c(
        "oracle",
        "hive",
        "postgresql",
        "redshift",
        "sql server",
        "pdw",
        "netezza",
        "impala",
        "bigquery",
        "sqlite",
        "sqlite extended",
        # "spark", # spark errss
        "snowflake",
        "synapse",
        "duckdb"
    )

    for (dbms in supportedDbms) {
        expect_no_error(translateCohortExtractionSql(
            dbms,
            'cdmDatabaseSchema',
            'cohortDatabaseSchema',
            'cohortTable',
            'timeAtRiskEnd',
            'washoutTime',
            'targetId'
            ))
    }
})

test_that("CohortExtraction.sql yields temporary tables with correct number of rows", {
    writeMatchedCohortsToScratchDatabase(sqliteConnection,
                                         dbms,
                                         cdmDatabaseSchema,
                                         cdmDatabaseSchema,
                                         cohortTable,
                                         defaultRiskWindowAfterTargetExposureDays,
                                         defaultWashoutPeriodDays,
                                         defaultTargetId)

    expect_equal(
        nrow(DatabaseConnector::querySql(sqliteConnection, "SELECT * FROM temp.target")),
        defaultN * (1+defaultPercentSecondShot)
    )

    expect_equal(
        nrow(DatabaseConnector::querySql(sqliteConnection, "SELECT * FROM temp.comparator")),
        defaultN
    )

    # Test rest of tables
})

# TODO:
# Test correct results with default parameters
# Then may need to refactor some to make it make sense with non-default params
