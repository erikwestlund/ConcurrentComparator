library(ConcurrentComparator)
library(RSQLite)

set.seed(1234)

# We will simulate a study using an SQLite database.
dbFile <- "simDb.sqlite"
cdmDatabaseSchema <- "main"
cohortDatabaseSchema <- "main"
cohortTable <- "cohort"

# Set population parameters:
n <- 2000000
targetId <- 667
outcomeIds <- c(668)
controlIds <- c(74816)
lowRiskTargetIncidence <- .01
lowRiskComparatorIncidence <- .01
highRiskTargetIncidence <- .04
highRiskComparatorIncidence <- .04
proportionSecondShot <- 0.65
timeAtRiskEndDays <- 21
washoutPeriodDays <- 22
highRiskGroupDefinition <- list(
    list(
        'var' = 'age_group_id',
        'operator' = 'in',
        'val' = 13:20 # 65-100 years old
    ),
    list(
        'var' = 'race_concept_id',
        'operator' = '=',
        'val' = 8516 # Black
    ),
    list(
        'var' = 'cohort_start_date',
        'operator' = 'between',
        'val' = c(as.Date('2021-04-01'), as.Date('2021-06-30'))
    )
)
matchHighRiskCriteria <- 'all'
studyStartDate <- '2020-12-18'
studyEndDate <- '2021-06-30'
targetDaysEligibleBeforeStudyEndDate <- as.integer(as.Date(studyEndDate) - as.Date(studyStartDate))

# Generate simulated persons, cohort, and observation periods data.
persons <- generateSimulatedPersonsData(n)

cohort <- generateSimulatedCohortTable(
  persons = persons,
  targetId = targetId,
  outcomeIds = outcomeIds,
  proportionSecondShot = proportionSecondShot,
  timeAtRiskEndDays = timeAtRiskEndDays,
  washoutPeriodDays = washoutPeriodDays,
  highRiskGroupDefinition = highRiskGroupDefinition,
  matchHighRiskCriteria = matchHighRiskCriteria,
  lowRiskTargetIncidence = lowRiskTargetIncidence,
  lowRiskComparatorIncidence = lowRiskComparatorIncidence,
  highRiskTargetIncidence = highRiskTargetIncidence,
  highRiskComparatorIncidence = highRiskComparatorIncidence,
  studyEndDate = studyEndDate,
  targetDaysEligibleBeforeStudyEndDate = targetDaysEligibleBeforeStudyEndDate
)

observationPeriods <- generateSimulatedObservationPeriodTable(cohort, washoutPeriodDays)

# Creating a SQLite database & connection to store the data and run the analysis requires transforming some dates to
# unix timestamps to work correclty with the Concurrent Comparator cohort extraction procedure.
sqlitePerson <- persons %>% mutate(
      birth_datetime = as.numeric(as.POSIXct(birth_datetime), format = "%Y-%m-%d")
  )

sqliteCohort <- cohort %>% mutate(
  cohort_start_date = as.numeric(as.POSIXct(cohort_start_date), format = "%Y-%m-%d"),
  cohort_end_date = as.numeric(as.POSIXct(cohort_end_date), format = "%Y-%m-%d")
)

sqliteObservationPeriod <- observationPeriods %>% mutate(
  observation_period_start_date = as.numeric(as.POSIXct(observation_period_start_date), format = "%Y-%m-%d"),
  observation_period_end_date = as.numeric(as.POSIXct(observation_period_end_date), format = "%Y-%m-%d")
)

# Create the SQlite database/connection and load the data.
conn <- DatabaseConnector::createConnectionDetails(
  dbms = "sqlite",
  server = dbFile
)
sqliteConnection <- dbConnect(RSQLite::SQLite(), dbFile)

RSQLite::dbWriteTable(sqliteConnection, "person", sqlitePerson, overwrite = TRUE)
RSQLite::dbWriteTable(sqliteConnection, "cohort", sqliteCohort, overwrite = TRUE)
RSQLite::dbWriteTable(sqliteConnection, "observation_period", sqliteObservationPeriod, overwrite = TRUE)

# Single CC Analysis
ccData <- getDbConcurrentComparatorData(connectionDetails = conn,
                                        cdmDatabaseSchema = cdmDatabaseSchema,
                                        targetId = 667,
                                        outcomeIds = 668,
                                        studyEndDate = "2021-06-30",
                                        exposureDatabaseSchema = cohortDatabaseSchema,
                                        exposureTable = cohortTable,
                                        outcomeDatabaseSchema = cohortDatabaseSchema,
                                        outcomeTable = cohortTable,
                                        timeAtRiskStart = 1,
                                        timeAtRiskEnd = 21,
                                        washoutTime = 22,
                                        testing = TRUE)

saveConcurrentComparatorData(ccData, "t667_o668.zip")
ccData <- loadConcurrentComparatorData("t667_o668.zip")

population <- createStudyPopulation(ccData, outcomeId = 668)

fit <- fitOutcomeModel(population = population)

fit

# Cleanup
RSQlite::dbDisconnect(conn)
file.remove(dbFile)
