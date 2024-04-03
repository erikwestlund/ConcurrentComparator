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
    n <- 1000
    proportionSecondShot <- 1

    testData <- scaffoldTestData(
        n = defaultN,
        proportionSecondShot = proportionSecondShot
    )

    # Target
    expect_equal(
        nrow(DatabaseConnector::querySql(testData$sqliteConnection, "SELECT * FROM temp.target")),
        n * (1+proportionSecondShot)
    )

    # Comparator
    expect_equal(
        nrow(DatabaseConnector::querySql(testData$sqliteConnection, "SELECT * FROM temp.comparator")),
        n
    )

    # Strata -- should match unique combinations from cohort
    expect_equal(
        nrow(DatabaseConnector::querySql(testData$sqliteConnection, "SELECT * FROM temp.strata")),
        nrow(testData$data$strata)
    )

    # Matched Strata
    expect_equal(
        nrow(DatabaseConnector::querySql(testData$sqliteConnection, "SELECT * FROM temp.matched_strata")),
        nrow(testData$data$matchedStrata)
    )



    # Test rest of tables
})

# TODO:
# Test correct results with default parameters
# Then may need to refactor some to make it make sense with non-default params
