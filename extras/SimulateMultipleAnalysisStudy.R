library(ConcurrentComparator)
library(RSQLite)

set.seed(1234)

# We will simulate a study using an SQLite database.
dbFile <- "simDb.sqlite"
cdmDatabaseSchema <- "main"
cohortDatabaseSchema <- "main"
cohortTable <- "cohort"
conditionEraTable <- "condition_era"

# Set population parameters:
n <- 1000000
targetId <- 667
outcomeIds <- c(668)
controlIds <- c(74816)
lowRiskTargetIncidence <- .01
lowRiskComparatorIncidence <- .01
highRiskTargetIncidence <- .04
highRiskComparatorIncidence <- .04
controlIncidence <- .01
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

# Generate simulated persons, cohort, and observation periods data for our target outcome.
targetPersons <- generateSimulatedPersonsData(n)

targetCohort <- generateSimulatedCohortTable(
  persons = targetPersons,
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

targetObservationPeriods <- generateSimulatedObservationPeriodTable(targetCohort, washoutPeriodDays)
targetConditionEras <- generateSimulatedConditionEraTable(targetCohort, outcomeIds)
targetConceptAncestors <- generateSimulatedConceptAncestorTable(targetConditionEras)

# Generate simulated persons, cohort, and observation periods data for our control outcome.
controlPersons <- generateSimulatedPersonsData(n)

controlCohort <- generateSimulatedCohortTable(
  persons = controlPersons,
  targetId = targetId,
  outcomeIds = controlIds,
  proportionSecondShot = proportionSecondShot,
  timeAtRiskEndDays = timeAtRiskEndDays,
  washoutPeriodDays = washoutPeriodDays,
  highRiskGroupDefinition = highRiskGroupDefinition,
  matchHighRiskCriteria = matchHighRiskCriteria,
  lowRiskTargetIncidence = controlIncidence,
  lowRiskComparatorIncidence = controlIncidence,
  highRiskTargetIncidence = controlIncidence,
  highRiskComparatorIncidence = controlIncidence,
  studyEndDate = studyEndDate,
  targetDaysEligibleBeforeStudyEndDate = targetDaysEligibleBeforeStudyEndDate
)

controlOobservationPeriods <- generateSimulatedObservationPeriodTable(controlCohort, washoutPeriodDays)
controlConditionEras <- generateSimulatedConditionEraTable(controlCohort, controlIds)
controlConceptAncestors <- generateSimulatedConceptAncestorTable(controlConditionEras)

# Stack the dataframes from the target and control cohorts.
persons <- rbind(targetPersons, controlPersons)
cohort <- rbind(targetCohort, controlCohort)
observationPeriods <- rbind(targetObservationPeriods, controlOobservationPeriods)
conditionEras <- rbind(targetConditionEras, controlConditionEras)
conceptAncestors <- rbind(targetConceptAncestors, controlConceptAncestors) %>% distinct()

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

sqliteConditionEra <- conditionEras %>% mutate(
  condition_era_start_date = as.numeric(as.POSIXct(condition_era_start_date), format = "%Y-%m-%d"),
  condition_era_end_date = as.numeric(as.POSIXct(condition_era_end_date), format = "%Y-%m-%d")
)

sqliteConceptAncestor <- conceptAncestors

# Create the SQlite database/connection and load the data.
conn <- DatabaseConnector::createConnectionDetails(
  dbms = "sqlite",
  server = dbFile
)
sqliteConnection <- dbConnect(RSQLite::SQLite(), dbFile)

RSQLite::dbWriteTable(sqliteConnection, "person", sqlitePerson, overwrite = TRUE)
RSQLite::dbWriteTable(sqliteConnection, "cohort", sqliteCohort, overwrite = TRUE)
RSQLite::dbWriteTable(sqliteConnection, "observation_period", sqliteObservationPeriod, overwrite = TRUE)
RSQLite::dbWriteTable(sqliteConnection, "condition_era", sqliteConditionEra, overwrite = TRUE)
RSQLite::dbWriteTable(sqliteConnection, "concept_ancestor", sqliteConceptAncestor, overwrite = TRUE)


# Multiple CC Analysis
analysisList <- list(
    createConcurrentComparatorAnalysis(analysisId = 1,
                                       studyEndDate = "2021-06-30",
                                       timeAtRiskStart = 1,
                                       timeAtRiskEnd = 21,
                                       washoutTime = 22),
    createConcurrentComparatorAnalysis(analysisId = 2,
                                       studyEndDate = "2021-06-30",
                                       timeAtRiskStart = 0,
                                       timeAtRiskEnd = 7,
                                       washoutTime = 36)
)

outputFolder <- "./ConcurrentComparatorOutput"
results <- runConcurrentComparatorAnalyses(connectionDetails = conn,
                                           cdmDatabaseSchema = cdmDatabaseSchema,
                                           exposureDatabaseSchema = cohortDatabaseSchema,
                                           exposureTable = cohortTable,
                                           outcomeDatabaseSchema = cohortDatabaseSchema,
                                           outcomeTable = cohortTable,
                                           outputFolder = outputFolder,
                                           analysisList = analysisList,
                                           targetIds = c(667),
                                           outcomeIds = c(668),
                                           controlIds = c(74816))



# Cleanup
RSQlite::dbDisconnect(conn)
file.remove(dbFile)
