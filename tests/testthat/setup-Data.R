library(RSQLite)
library(tibble)


# Setup testing defaults ------------------------------------------------------

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

# Note: For the testing suite to work correctly with SQLite, we need to convert
# the dates to unix epochs. This is because the SQLite translation layer
# assumes unix epochs for dates.

defaultPersons <- generateSimulatedPersonsData(defaultN) %>%
    mutate(
        birth_datetime = as.numeric(as.POSIXct(birth_datetime, format="%Y-%m-%d")),
    )
createPersonsTable(defaultPersons, cdmDatabaseSchema, personsTable, dbFile)

# Create the cohort and derived observation period tables before mutating dates
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
observationPeriods <- generateSimulatedObservationPeriodTable(defaultCohort, defaultWashoutPeriodDays)

# Now, mutate the dates and store in SQLite
defaultCohort <- defaultCohort %>%
    mutate(
        cohort_start_date = as.numeric(as.POSIXct(cohort_start_date, format="%Y-%m-%d")),
        cohort_end_date = as.numeric(as.POSIXct(cohort_end_date, format="%Y-%m-%d"))
    )

observationPeriods <- observationPeriods %>%
    mutate(
        observation_period_start_date = as.numeric(as.POSIXct(observation_period_start_date, format="%Y-%m-%d")),
        observation_period_end_date = as.numeric(as.POSIXct(observation_period_end_date, format="%Y-%m-%d"))
    )

# Store these dataframes as tables
createCohortTable(defaultCohort, cdmDatabaseSchema, cohortTable, dbFile)
createObservationPeriodTable(observationPeriods, cdmDatabaseSchema, observationPeriodTable, dbFile)


# Scratch -----------------------------------------------------------------

sqliteConnectionDetails <- DatabaseConnector::createConnectionDetails(
    dbms = dbms,
    server = dbFile
)
sqliteConnection <- DatabaseConnector::connect(sqliteConnectionDetails)


writeMatchedCohortsToScratchDatabase(sqliteConnection,
                                     dbms,
                                     cdmDatabaseSchema,
                                     cdmDatabaseSchema,
                                     cohortTable,
                                     defaultRiskWindowAfterTargetExposureDays,
                                     defaultWashoutPeriodDays,
                                     defaultTargetId)
t <- DatabaseConnector::querySql(sqliteConnection, "SELECT * FROM temp.comparator")


# Teardown ----------------------------------------------------------------

withr::defer(unlink(dbFile), teardown_env())

