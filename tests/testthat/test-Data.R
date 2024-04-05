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
        testData <- generateTestData(
            n = 10000,
            proportionSecondShot = 1,
            studyStartDate = '2020-12-18',
            studyEndDate = '2021-06-30',
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

test_that("Two people who get shot washout period days apart from each other end up in same Strata.", {
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
        testData <- generateN2TestData(
            timeAtRiskStartDays = scenario$timeAtRiskStartDays,
            timeAtRiskEndDays = scenario$timeAtRiskEndDays,
            washoutPeriodDays = scenario$washoutPeriodDays,
            comparatorShot1DaysBeforeLastTargetShot = scenario$washoutPeriodDays
        )

        expect_equal(
            testData$ccData$matchedCohort %>% collect() %>% filter(subjectId == 1) %>% pull(strataId),
            testData$ccData$matchedCohort %>% collect() %>% filter(subjectId == 2) %>% pull(strataId)
        )
    }
})

test_that("Two people who get shot > washout period days apart from each other end up in different Strata.", {
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
        testData <- generateN2TestData(
            timeAtRiskStartDays = scenario$timeAtRiskStartDays,
            timeAtRiskEndDays = scenario$timeAtRiskEndDays,
            washoutPeriodDays = scenario$washoutPeriodDays,
            comparatorShot1DaysBeforeLastTargetShot = scenario$washoutPeriodDays + 5
        )

        # Empty because no strata match found so matchedCohort has no records.
        expect_equal(
            testData$ccData$matchedCohort %>% collect() %>% nrow(),
            0
        )
    }
})

test_that("Observed target outcome counts when day after shot counts when daysToStart == 1", {
    testData <- generateN2TestData(
            timeAtRiskStartDays = 1,
            timeAtRiskEndDays = 7,
            washoutPeriodDays = 22,
            targetOutcomes = list(
                list(outcomeId = 668, daysAfterFirstCohortEntry = 1)
            )
        )

    expect_equal(
        testData$ccData$allOutcomes %>% collect() %>% filter(subjectId == 1) %>% nrow(),
        1
    )
})

test_that("Observed comparator outcome counts when day after shot counts when daysToStart == 1", {
    testData <- generateN2TestData(
            timeAtRiskStartDays = 1,
            timeAtRiskEndDays = 7,
            washoutPeriodDays = 22,
            comparatorOutcomes = list(
                list(outcomeId = 668, daysAfterFirstCohortEntry = 1)
            )
        )

    expect_equal(
        testData$ccData$allOutcomes %>% collect() %>% filter(subjectId == 1) %>% nrow(),
        1
    )
})

test_that("Observed outcome on day of shot does not count when daysToStart > 0", {
    testData <- generateN2TestData(
            timeAtRiskStartDays = 1,
            timeAtRiskEndDays = 7,
            washoutPeriodDays = 22,
            outcomes = list(
                list(outcomeId = 668, daysAfterFirstCohortEntry = 0)
            )
        )

    expect_equal(
        testData$ccData$allOutcomes %>% collect() %>% filter(subjectId == 1) %>% nrow(),
        0
    )
})

test_that("Outcome on day 7 after shot counts when time at risk ends on day 7", {
    testData <- generateN2TestData(
            timeAtRiskStartDays = 1,
            timeAtRiskEndDays = 7,
            washoutPeriodDays = 22,
            outcomes = list(
                list(outcomeId = 668, daysAfterFirstCohortEntry = 7)
            )
        )

    expect_equal(
        testData$ccData$allOutcomes %>% collect() %>% filter(subjectId == 1) %>% nrow(),
        1
    )
})

test_that("Outcome on day 8 after shot does not count when time at risk ends on day 7", {
    testData <- generateN2TestData(
            timeAtRiskStartDays = 1,
            timeAtRiskEndDays = 7,
            washoutPeriodDays = 22,
            outcomes = list(
                list(outcomeId = 668, daysAfterFirstCohortEntry = 8)
            )
        )

    expect_equal(
        testData$ccData$allOutcomes %>% collect() %>% filter(subjectId == 1) %>% nrow(),
        0
    )
})

test_that("Outcome on day 23 after shot counts as comparator outcome.", {
    testData <- generateN2TestData(
            timeAtRiskStartDays = 1,
            timeAtRiskEndDays = 7,
            washoutPeriodDays = 22,
            outcomes = list(
                list(outcomeId = 668, daysAfterFirstCohortEntry = 5)
            )
        )

    testData$ccData$matchedCohort %>% collect()
    testData$ccData$allOutcomes %>% collect()

    # Note --
    expect_equal(
        testData$ccData$allOutcomes %>% collect() %>% filter(subjectId == 1) %>% nrow(),
        0
    )
})



# TODO:
# Test correct results with default parameters
# Then may need to refactor some to make it make sense with non-default params
