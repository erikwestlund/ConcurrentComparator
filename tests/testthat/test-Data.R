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

test_that("getDbConcurrentComparatorData yields data objects with the expected number of records", {

   # Constant across tests
    n <- 10000
    proportionSecondShot <- 1
    studyStartDate <- '2020-12-18'
    studyEndDate <- '2021-06-30'

    # Varies across tests -- accords to protocol
    scenarios <- list(
        list(
            timeAtRiskStartDays = 0,
            timeAtRiskEndDays = 7,
            washoutPeriodDays = 22
        ),
        list(
            timeAtRiskStartDays = 1,
            timeAtRiskEndDays = 21,
            washoutPeriodDays = 22
        ),
        list(
            timeAtRiskStartDays = 1,
            timeAtRiskEndDays = 28,
            washoutPeriodDays = 29
        )
    )

    for(scenario in scenarios) {
        # Get test data
        testData <- scaffoldTestData(
            n = n,
            proportionSecondShot = proportionSecondShot,
            studyStartDate = studyStartDate,
            studyEndDate = studyEndDate,
            timeAtRiskStartDays = scenario$timeAtRiskStartDays,
            timeAtRiskEndDays = scenario$timeAtRiskEndDays,
            washoutPeriodDays = scenario$washoutPeriodDays
        )

        # Strata
        expect_equal(
            nrow(testData$ccData$strata %>% collect()),
            nrow(testData$sourceData$strata)
        )

        # Matched Cohort
        expect_equal(
            nrow(testData$ccData$matchedCohort %>% collect()),
            nrow(testData$sourceData$matchedCohort)
        )

        # Outcomes
        expect_equal(
            nrow(testData$ccData$allOutcomes %>% collect()),
            nrow(testData$sourceData$allOutcomes)
        )
    }
})

# TODO:
# Test correct results with default parameters
# Then may need to refactor some to make it make sense with non-default params
