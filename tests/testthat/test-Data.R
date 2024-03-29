library(RSQLite)
library(tibble)


# Setup defaults ----------------------------------------------------------

dbFile <- "testDb.sqlite"
dbms <- "sqlite"
cdmDatabaseSchema <- "main"
personsTable <- "person"
cohortTable <- "cohort"
observationPeriodTable <- "observation_period"

defaultN <- 1000
defaultTargetId <- 666
defaultOutcomeIds <- c(668)
defaultPercentSecondShot <- 1
defaultRiskWindowAfterTargetExposureDays <- 21
defaultWashoutPeriodDays <- 22
defaultHighRiskGroupDefinition <- list()
defaultMatchHighRiskCriteria <- 'all'
defaultLowRiskTargetIncidence <- 0.01
defaultLowRiskComparatorIncidence <- 0.01
defaultHighRiskTargetIncidence <- 0.01
defaultHighRiskComparatorIncidence <- 0.01
defaultStudyStartDate <- '2020-12-18'
defaultStudyEndDate <- '2021-06-30'
defaultTargetDaysEligibleBeforeStudyEndDate <- as.integer(as.Date(defaultStudyEndDate) - as.Date(defaultStudyStartDate))

set.seed(176400)



# Helper Functions --------------------------------------------------------

createEmptySQLiteDb <- function(dbFile) {
    con <- dbConnect(RSQLite::SQLite(), dbFile)
    dbDisconnect(con)
}

createPersonsTable <- function(persons, schema, personsTable, dbFile) {
    con <- RSQLite::dbConnect(RSQLite::SQLite(), dbFile)
    RSQLite::dbWriteTable(con, Id(schema = schema, table = personsTable), persons, overwrite = TRUE)
    RSQLite::dbDisconnect(con)
}

createCohortTable <- function(cohort, schema, cohortTable, dbFile) {
    con <- RSQLite::dbConnect(RSQLite::SQLite(), dbFile)
    RSQLite::dbWriteTable(con, Id(schema = schema, table = cohortTable), cohort, overwrite = TRUE)
    RSQLite::dbDisconnect(con)
}


createObservationPeriodTable <- function(observationPeriods, schema, observationPeriodTable, dbFile) {
    con <- RSQLite::dbConnect(RSQLite::SQLite(), dbFile)
    RSQLite::dbWriteTable(con, Id(schema = schema, table = observationPeriodTable), observationPeriods, overwrite = TRUE)
    RSQLite::dbDisconnect(con)
}



# Scaffolding -------------------------------------------------------------

createEmptySQLiteDb(dbFile)

defaultPersons <- generateSimulatedPersonsData(defaultN)
createPersonsTable(defaultPersons, cdmDatabaseSchema, personsTable, dbFile)

defaultCohort <- generateSimulatedCohortTable(
    persons = defaultPersons,
    targetId = defaultTargetId,
    outcomeIds = defaultOutcomeIds,
    proportionSecondShot = defaultPercentSecondShot,
    riskWindowAfterTargetExposureDays = defaultRiskWindowAfterTargetExposureDays,
    washoutPeriodDays = defaultWashoutPeriodDays,
    highRiskGroupDefinition = defaultHighRiskGroupDefinition,
    matchHighRiskCriteria = defaultMatchHighRiskCriteria,
    lowRiskTargetIncidence = defaultLowRiskTargetIncidence,
    lowRiskComparatorIncidence = defaultLowRiskComparatorIncidence,
    highRiskTargetIncidence = defaultHighRiskTargetIncidence,
    highRiskComparatorIncidence = defaultHighRiskComparatorIncidence,
    studyEndDate = defaultStudyEndDate,
    targetDaysEligibleBeforeStudyEndDate = defaultTargetDaysEligibleBeforeStudyEndDate
)
createCohortTable(defaultCohort, cdmDatabaseSchema, cohortTable, dbFile)

observationPeriods <- generateSimulatedObservationPeriodTable(
    cohort = defaultCohort,
    riskWindowAfterTargetExposureDays = defaultRiskWindowAfterTargetExposureDays
)
createObservationPeriodTable(observationPeriods, cdmDatabaseSchema, observationPeriodTable, dbFile)

# Scratch -----------------------------------------------------------------

sqliteConnectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = dbms,
    server = dbFile
)
sqliteConnection <- DatabaseConnector::connect(sqliteConnectionDetails)

test_that("CohortExtraction.sql yields target table with correct number of rows ", {
    # This runs CohortExtraction.sql on the SQLite database.
    writeMatchedCohortsToScratchDatabase(sqliteConnection,
                                         dbms,
                                         cdmDatabaseSchema,
                                         cdmDatabaseSchema,
                                         cohortTable,
                                         defaultRiskWindowAfterTargetExposureDays,
                                         defaultWashoutPeriodDays,
                                         defaultTargetId)

    targetTable <- DatabaseConnector::querySql(sqliteConnection, "SELECT * FROM temp.target")

    expect_equal(
        nrow(targetTable),
        defaultN * (1+defaultPercentSecondShot) + # Target
        defaultN + # Comparator
        defaultN * defaultHighRiskComparatorIncidence + # High risk comparator
        defaultN * defaultHighRiskTargetIncidence + # High risk target
        defaultN * defaultLowRiskTargetIncidence + # Low risk target
        defaultN * defaultLowRiskComparatorIncidence # Low risk comparator

    )
})

#' Tests:

withr::defer(unlink(dbFile), teardown_env())
