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
        # "spark",
        "snowflake",
        "synapse",
        "duckdb"
    )

    expect_no_error(translateCohortExtractionSql)

    for (dbms in supportedDbms) {
        translateCohortExtractionSql(
            dbms,
            'cdmDatabaseSchema',
            'cohortDatabaseSchema',
            'cohortTable',
            'timeAtRiskEnd',
            'washoutTime',
            'targetId'
            )
    }

})

test_that("CohortExtraction.sql yields target table with correct number of rows", {
    writeMatchedCohortsToScratchDatabase(sqliteConnection,
                                         dbms,
                                         cdmDatabaseSchema,
                                         cdmDatabaseSchema,
                                         cohortTable,
                                         defaultRiskWindowAfterTargetExposureDays,
                                         defaultWashoutPeriodDays,
                                         defaultTargetId)

    # Counts rows
    targetTable <- DatabaseConnector::querySql(sqliteConnection, "SELECT * FROM temp.target")

    expect_equal(
        nrow(targetTable),
        defaultN * (1+defaultPercentSecondShot)

    )
})
