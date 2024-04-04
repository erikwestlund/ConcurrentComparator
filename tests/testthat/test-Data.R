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

test_that("getDbConcurrentComparatorData yields data objects with the expected number of records in common scenarios.", {

   # Constant across tests
    n <- 10000
    proportionSecondShot <- 1
    studyStartDate <- '2020-12-18'
    studyEndDate <- '2021-06-30'

    # Varies across tests -- accords to EUMAEUS protocol
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

test_that("Two people who get shot washout period days apart end up in same Strata.", {
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
        testData <- scaffoldN2TestData(
            timeAtRiskStartDays = scenario$timeAtRiskStartDays,
            timeAtRiskEndDays = scenario$timeAtRiskEndDays,
            washoutPeriodDays = scenario$washoutPeriodDays,
            comparatorShotDaysBefore = scenario$washoutPeriodDays
        )

        expect_equal(
            testData$ccData$matchedCohort %>% collect() %>% filter(subjectId == 1) %>% pull(strataId),
            testData$ccData$matchedCohort %>% collect() %>% filter(subjectId == 2) %>% pull(strataId)
        )
    }
})


test_that("Outcome on day of shot does not show up when daysToStart > 0", {
    testData <- scaffoldN2TestData(
            timeAtRiskStartDays = 1,
            timeAtRiskEndDays = 7,
            washoutPeriodDays = 22,
            outcomes = list(
                list(outcomeId = 668, daysAfterFirstShot = 0)
            )
        )

    expect_equal(
        testData$ccData$allOutcomes %>% collect() %>% filter(subjectId == 1) %>% nrow(),
        0
    )
})


test_that("Outcome on day after shot shows up when daysToStart = 1", {
    testData <- scaffoldN2TestData(
            timeAtRiskStartDays = 1,
            timeAtRiskEndDays = 7,
            washoutPeriodDays = 22,
            outcomes = list(
                list(outcomeId = 668, daysAfterFirstShot = 1)
            )
        )

    expect_equal(
        testData$ccData$allOutcomes %>% collect() %>% filter(subjectId == 1) %>% nrow(),
        1
    )
})


# TODO:
# Test correct results with default parameters
# Then may need to refactor some to make it make sense with non-default params
