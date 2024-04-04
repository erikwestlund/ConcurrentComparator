# Copyright 2023 Observational Health Data Sciences and Informatics
#
# This file is part of ConcurrentComparator
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Concurrent Comparator Data
#'
#' @description
#' `ConcurrentComparatorData` is an S4 class that inherits from [Andromeda][Andromeda::Andromeda]. It contains
#' information on the cohorts, their outcomes, and baseline covariates. Information about multiple outcomes can be
#' captured at once for efficiency reasons.
#'
#' A `ConcurrentComparatorData` is typically created using [getDbConcurrentComparatorData()], can only be saved using
#' [saveConcurrentComparatorData()], and loaded using [loadConcurrentComparatorData()].
#'
#' @name ConcurrentComparatorData-class
#' @aliases ConcurrentComparatorData
NULL

#' ConcurrentComparatorData class.
#'
#' @export
#' @import Andromeda
setClass("ConcurrentComparatorData", contains = "Andromeda")


#' Get the concurrent comparator data from the server
#'
#' @description
#' This function executes a large set of SQL statements against the database in OMOP CDM format to
#' extract the data needed to perform the analysis.
#'
#' @details
#' TODO
#'
#' @param connectionDetails            An R object of type `connectionDetails` created using the
#'                                     [DatabaseConnector::createConnectionDetails()] function.
#' @param cdmDatabaseSchema            The name of the database schema that contains the OMOP CDM
#'                                     instance. Requires read permissions to this database. On SQL
#'                                     Server, this should specify both the database and the schema,
#'                                     so for example 'cdm_instance.dbo'.
#' @param tempEmulationSchema          Some database platforms like Oracle and Impala do not truly support temp tables.
#'.                                    To emulate temp tables, provide a schema with write privileges where temp tables
#'                                     can be created.
#' @param targetId                     A unique identifier to define the target cohort. targetId is
#'                                     used to select the COHORT_DEFINITION_ID in the cohort-like table.
#' @param comparatorId                 A unique identifier to define the comparator cohort. comparatorId
#'                                     is used to select the COHORT_DEFINITION_ID in the cohort-like
#'                                     table. TODO UPDATE
#' @param overwriteComparators         Allow regeneration of comparators if comparatorId already exists
#'                                     TODO what is the table name?
#' @param outcomeIds                   A list of cohort IDs used to define outcomes.
#'
#' @param studyStartDate               A calendar date specifying the minimum date that a cohort index
#'                                     date can appear. Date format is 'yyyymmdd'.
#' @param studyEndDate                 A calendar date specifying the maximum date that a cohort index
#'                                     date can appear. Date format is 'yyyymmdd'. Important: the study
#'                                     end data is also used to truncate risk windows, meaning no
#'                                     outcomes beyond the study end date will be considered.
#' @param exposureDatabaseSchema       The name of the database schema that is the location where the
#'                                     exposure data used to define the exposure cohorts is available.
#' @param exposureTable                The tablename that contains the exposure cohorts has the
#'                                     format of a COHORT table: COHORT_DEFINITION_ID, SUBJECT_ID,
#'                                     COHORT_START_DATE, COHORT_END_DATE.
#' @param outcomeDatabaseSchema        The name of the database schema that is the location where the
#'                                     data used to define the outcome cohorts is available.
#' @param outcomeTable                 The tablename that contains the outcome cohorts has the format of a COHORT table:
#'                                     COHORT_DEFINITION_ID, SUBJECT_ID, COHORT_START_DATE, COHORT_END_DATE.
#' @param timeAtRiskStart              The time-at-risk start in days after subject index date.
#' @param timeAtRiskEnd                The time-at-risk end in days after subject index date.
#' @param washoutTime                  Washout time in days between target and comparator periods
#'
#' @return
#' A [ConcurrentComparatorData] object.
#'
#' @export

getDbConcurrentComparatorData <- function(connectionDetails,
                                          cdmDatabaseSchema,
                                          tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                          targetId = 1,
                                          outcomeIds,
                                          overwriteComparators = FALSE,
                                          studyStartDate = "",
                                          studyEndDate = "",
                                          exposureDatabaseSchema = cdmDatabaseSchema,
                                          exposureTable,
                                          outcomeDatabaseSchema = cdmDatabaseSchema,
                                          outcomeTable,
                                          timeAtRiskStart,
                                          timeAtRiskEnd,
                                          washoutTime = timeAtRiskEnd + 1,
                                          intermediateFileNameStem = NULL) {

    errorMessages <- checkmate::makeAssertCollection()
    checkmate::assertClass(connectionDetails, "ConnectionDetails", add = errorMessages)
    checkmate::assertCharacter(cdmDatabaseSchema, len = 1, add = errorMessages)
    checkmate::assertCharacter(tempEmulationSchema, len = 1, null.ok = TRUE, add = errorMessages)
    checkmate::assertIntegerish(targetId, add = errorMessages)
    checkmate::assertIntegerish(outcomeIds, add = errorMessages)
    checkmate::assertCharacter(studyStartDate, len = 1, add = errorMessages)
    checkmate::assertCharacter(studyEndDate, len = 1, add = errorMessages)
    checkmate::assertCharacter(exposureDatabaseSchema, len = 1, add = errorMessages)
    checkmate::assertCharacter(exposureTable, len = 1, add = errorMessages)
    checkmate::assertCharacter(outcomeDatabaseSchema, len = 1, add = errorMessages)
    checkmate::assertCharacter(outcomeTable, len = 1, add = errorMessages)
    checkmate::assertInt(timeAtRiskStart, lower = 0, add = errorMessages)
    checkmate::assertInt(timeAtRiskEnd, lower = timeAtRiskStart, add = errorMessages)
    checkmate::assertInt(washoutTime, lower = 1, add = errorMessages)
    checkmate::assertLogical(overwriteComparators, len = 1, add = errorMessages) # TODO Remove
    checkmate::reportAssertions(collection = errorMessages)

    # checkmate::assertInt(outcomeIds, add = errorMessages) # TODO generalize for multiple outcomes

    start <- Sys.time()
    connection <- initializeDatabaseConnection(connectionDetails)

    ParallelLogger::logInfo("Creating matched cohorts in database for targetId ", targetId, ".")
    writeMatchedCohortsToScratchDatabase(
      connection,
      connectionDetails$dbms,
      cdmDatabaseSchema,
      exposureDatabaseSchema,
      exposureTable,
      timeAtRiskEnd,
      washoutTime,
      targetId
    )

    ParallelLogger::logInfo("Initializing ConcurrentComparator data object on local system.")
    concurrentComparatorData <- initializeConcurrentComparatorData()

    ParallelLogger::logInfo("Extracting strata and matched cohort data.")
    concurrentComparatorData <- writeStrataToConncurentComparatorData(concurrentComparatorData, connection, connectionDetails$dbms, targetId)
    concurrentComparatorData <- writeMatchedCohortToConcurrentComparatorData(concurrentComparatorData, connection, connectionDetails$dbms, targetId)

    ParallelLogger::logInfo("Removing subjects with 0 time-at-risk.")
    matchedCohortDiagnostics <- getMatchedCohortDiagnostics(concurrentComparatorData$matchedCohort)
    concurrentComparatorData <- removeMatchedCohortEpisodesWithZeroTimeAtRisk(concurrentComparatorData)

    ParallelLogger::logInfo("Extracting outcomes (IDs: ", paste0(outcomeIds, collapse = ","), ").")
    concurrentComparatorData <- writeOutcomesToConcurrentComparatorData(
      concurrentComparatorData,
      connection,
      connectionDetails$dbms,
      outcomeDatabaseSchema,
      cdmDatabaseSchema,
      outcomeTable,
      outcomeIds,
      targetId,
      timeAtRiskStart,
      timeAtRiskEnd
    )

    concurrentComparatorData <- saveIntermediateFile(concurrentComparatorData, intermediateFileNameStem, 1)

    ParallelLogger::logInfo("Truncating to study-end-date (if specified).")
    concurrentComparatorData$allOutcomes <- getTruncatedOutcomeData(
      concurrentComparatorData$matchedCohort,
      concurrentComparatorData$allOutcomes,
      studyStartDate,
      studyEndDate
    )

    concurrentComparatorData <- saveIntermediateFile(concurrentComparatorData, intermediateFileNameStem, 2)

    # Attach metadata and summary statistics to concurrent comparator data.
    concurrentComparatorData <- writeMetaDataToConcurrentComparatorData(
        concurrentComparatorData,
        targetId,
        outcomeIds,
        matchedCohortDiagnostics
    )

    delta <- Sys.time() - start
    message("Getting ConcurrentComparator data from server took ", signif(delta, 3), " ", attr(delta, "units"))

    DatabaseConnector::disconnect(connection)

    return(concurrentComparatorData)
}

# #' @export
# getAnalyticConcurrentComparatorData <- function(concurrentComparatorData, outcomeId) {
#
#     errorMessages <- checkmate::makeAssertCollection()
#     checkmate::assertClass(concurrentComparatorData, "ConcurrentComparatorData",
#                            add = errorMessages)
#     checkmate::assertInt(outcomeId)
#
#     outcomeMatchedCohort <- concurrentComparatorData$matchedCohort %>%
#         left_join(concurrentComparatorData$allOutcomes %>% filter(outcomeId == !!outcomeId) %>%
#                       select(exposureId, subjectId, strataId, cohortStartDate, y, outcomeStartDate, daysToEvent),
#                   by = c("exposureId", "subjectId", "strataId", "cohortStartDate")
#         ) %>%
#         mutate(y = ifelse(is.na(y), 0, y))
#
#     # Summary statistics
#     outcomeStatistics <- outcomeMatchedCohort %>% group_by(exposureId) %>%
#         summarise(entries = n(),
#                   subjects = n_distinct(subjectId),
#                   outcomes = sum(y),
#                   kPtYrs = sum(timeAtRisk / 365.25 / 1000)) %>%
#         as_tibble()
#
#     attr(outcomeMatchedCohort, "metaData") <- list(
#         targetId = attr(concurrentComparatorData, "metaData")$targetId,
#         outcomeId = outcomeId,
#         outcomeStatistics = outcomeStatistics
#     )
#
#     return(outcomeMatchedCohort)
# }


#' Save the concurrent comparator data to file
#'
#' @description
#' Saves an object of type [ConcurrentComparatorData] to a file.
#'
#' @template ConcurrentComparatorData
#' @param file               The name of the file where the data will be written. If the file already
#'                           exists it will be overwritten.
#'
#' @return
#' Returns no output.
#'
#' @export
saveConcurrentComparatorData <- function(concurrentComparatorData, file) {
    errorMessages <- checkmate::makeAssertCollection()
    checkmate::assertClass(concurrentComparatorData, "ConcurrentComparatorData",
                           add = errorMessages)
    checkmate::assertCharacter(file, len = 1, add = errorMessages)
    checkmate::reportAssertions(collection = errorMessages)

    Andromeda::saveAndromeda(concurrentComparatorData, file)
}

#' Load the concurrent comparator data from a file
#'
#' @description
#' Loads an object of type [ConcurrentComparatorData] from a file in the file system.
#'
#' @param file       The name of the file containing the data.
#'
#' @return
#' An object of class [ConcurrentComparatorData].
#'
#' @export
loadConcurrentComparatorData <- function(file) {
    errorMessages <- checkmate::makeAssertCollection()
    checkmate::assertCharacter(file, len = 1, add = errorMessages)
    checkmate::reportAssertions(collection = errorMessages)
    if (!file.exists(file)) {
        stop("Cannot find file ", file)
    }
    if (file.info(file)$isdir) {
        stop(file, " is a folder, but should be a file")
    }
    ConcurrentComparatorData <- Andromeda::loadAndromeda(file)
    class(ConcurrentComparatorData) <- "ConcurrentComparatorData"
    attr(class(ConcurrentComparatorData), "package") <- "ConcurrentComparator"
    return(ConcurrentComparatorData)
}

#' TODO:
initializeDatabaseConnection <- function(connectionDetails) {
    connection <- DatabaseConnector::connect(connectionDetails)
    # withr::defer(DatabaseConnector::disconnect(connection))
    return(connection)
}

#' TODO: document
#' @export
translateCohortExtractionSql <- function(dbms,
                                         cdmDatabaseSchema,
                                         cohortDatabaseSchema,
                                         cohortTable,
                                         timeAtRiskEnd,
                                         washoutTime,
                                         targetId) {

    sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "CohortExtraction.sql",
                                             packageName = "ConcurrentComparator",
                                             dbms = dbms,
                                             cdm_database_schema = cdmDatabaseSchema,
                                             cohort_database_schema = cohortDatabaseSchema,
                                             cohort_table = cohortTable,
                                             time_at_risk = timeAtRiskEnd,
                                             delta_time = washoutTime,
                                             cohort_ids = c(targetId),
                                             warnOnMissingParameters = TRUE)

    sql <- patchSql(sql, dbms)

    return(sql)
}

#' TODO: document once working
#' @export
writeMatchedCohortsToScratchDatabase <- function(connection,
                                                 dbms,
                                                 cdmDatabaseSchema,
                                                 exposureDatabaseSchema,
                                                 exposureTable,
                                                 timeAtRiskEnd,
                                                 washoutTime,
                                                 targetId) {
    sql <- translateCohortExtractionSql(dbms,
                                        cdmDatabaseSchema,
                                        exposureDatabaseSchema,
                                        exposureTable,
                                        timeAtRiskEnd,
                                        washoutTime,
                                        targetId)

    DatabaseConnector::executeSql(connection = connection, sql = sql)
}

#' TODO: doc
initializeConcurrentComparatorData <- function() {
    concurrentComparatorData <- Andromeda::andromeda()
    return(concurrentComparatorData)
}


#' TODO: doc
#' Takes andromeda object and extracts strata to local system and returns it
writeStrataToConncurentComparatorData <- function(concurrentComparatorData, connection, dbms, targetId) {
    sql <- SqlRender::render(
      "SELECT * FROM #strata WHERE cohort_definition_id = @target_id",
      target_id = targetId
    )
    sql <- SqlRender::translate(sql, dbms)

    DatabaseConnector::querySqlToAndromeda(connection = connection,
                                           sql = sql,
                                           andromeda = concurrentComparatorData,
                                           andromedaTableName = "strata",
                                           snakeCaseToCamelCase = TRUE)

    return(concurrentComparatorData)
}

#' TODO: doc
#' Takes andromeda object and extracts matched to local system and returns it
writeMatchedCohortToConcurrentComparatorData <- function(concurrentComparatorData, connection, dbms, targetId) {
    sql <- SqlRender::render(
    "SELECT exposure_id,
           strata_id,
           subject_id,
           cohort_start_date,
           DATEDIFF(DAY, cohort_start_date, cohort_end_date) AS time_at_risk
    FROM #matched_cohort
    WHERE cohort_definition_id = @target_id",
    target_id = targetId
    )
    sql <- SqlRender::translate(sql, dbms)

    DatabaseConnector::querySqlToAndromeda(connection = connection,
                                           sql = sql,
                                           andromeda = concurrentComparatorData,
                                           andromedaTableName = "matchedCohort",
                                           snakeCaseToCamelCase = TRUE)

    return(concurrentComparatorData)
}

#' TODO: doc
removeMatchedCohortEpisodesWithZeroTimeAtRisk <- function(concurrentComparatorData) {
    concurrentComparatorData$matchedCohort <- concurrentComparatorData$matchedCohort %>%
                                  collect() %>%
                                  filter(timeAtRisk != 0.0)

    return(concurrentComparatorData)
}

#' TODO: doc
getMatchedCohortDiagnostics <- function(matchedCohort) {
    zeroT <- length(matchedCohort %>% filter(exposureId == 1,
                                                       timeAtRisk == 0) %>% distinct(subjectId))
    zeroC <- length(matchedCohort %>% filter(exposureId == 0,
                                                       timeAtRisk == 0) %>% distinct(subjectId))

    return(list(zeroT = zeroT,
                zeroC = zeroC))
}

#' TODO: doc
writeOutcomesToConcurrentComparatorData <- function(
  concurrentComparatorData,
  connection,
  dbms,
  outcomeDatabaseSchema,
  cdmDatabaseSchema,
  outcomeTable,
  outcomeIds,
  targetId,
  timeAtRiskStart,
  timeAtRiskEnd
) {
    sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "GetOutcomes.sql",
                                                 packageName = "ConcurrentComparator",
                                                 dbms = dbms,
                                                 outcome_database_schema = outcomeDatabaseSchema,
                                                 cdm_database_schema = cdmDatabaseSchema,
                                                 outcome_table = outcomeTable,
                                                 outcome_ids = c(outcomeIds),
                                                 exposure_ids = targetId,
                                                 days_from_obs_start = timeAtRiskStart,
                                                 days_to_obs_end = timeAtRiskEnd,
                                                 warnOnMissingParameters = TRUE)

    sql <- patchSql(sql, dbms)

    DatabaseConnector::querySqlToAndromeda(connection = connection,
                                           sql = sql,
                                           andromeda = concurrentComparatorData,
                                           andromedaTableName = "allOutcomes",
                                           snakeCaseToCamelCase = TRUE)

    return(concurrentComparatorData)
}

#' TODO: doc
saveIntermediateFile <- function(concurrentComparatorData, intermediateFileNameStem, suffix = 1) {
  if (!is.null(intermediateFileNameStem)) {
    fileName <- paste0(intermediateFileNameStem, "_", suffix, ".zip")
    Andromeda::saveAndromeda(concurrentComparatorData, fileName = fileName)
    ParallelLogger::logInfo("Matched cohorts saved to: ", fileName)
    concurrentComparatorData <- Andromeda::loadAndromeda(fileName = fileName)
  }
  return(concurrentComparatorData)
}

#' TODO: doc
getTruncatedOutcomeData <- function(matchedCohort, outcomes, cohortStartDate, studyEndDate = "") {
  if (studyEndDate != "") {
        matchedCohort <- matchedCohort %>%
            collect() %>%
            mutate(
                restoredCohortStartDate = ifelse(inherits(cohortStartDate, "Date"), cohortStartDate, Andromeda::restoreDate(cohortStartDate)),
                truncate = restoredCohortStartDate > as.Date(studyEndDate)
            ) %>%
            filter(truncate == 0) %>%
            select(-truncate, -restoredCohortStartDate)

        outcomes <- outcomes %>%
            collect() %>%
            mutate(
                restoredCohortStartDate = ifelse(inherits(cohortStartDate, "Date"), cohortStartDate, Andromeda::restoreDate(cohortStartDate)),
                truncate = restoredCohortStartDate > as.Date(studyEndDate)
            ) %>%
            filter(truncate == 0) %>%
            select(-truncate, -restoredCohortStartDate) %>%
            mutate(y = 1)
    } else {
        outcomes <- outcomes %>% mutate(y = 1)
    }

  return(outcomes)
}

#' TODO: doc
writeMetaDataToConcurrentComparatorData <- function(
    concurrentComparatorData,
    targetId,
    outcomeIds,
    matchedCohortDiagnostics
) {
    cohortStatistics <- concurrentComparatorData$matchedCohort %>%
        collect() %>%
        group_by(exposureId) %>%
        summarise(entries = n(),
                  subjects = n_distinct(subjectId),
                  kPtYrs = sum(timeAtRisk / 365.25 / 1000)) %>%
        as_tibble()

    attr(concurrentComparatorData, "metaData") <- list(
        targetId = targetId,
        outcomeIds = outcomeIds,
        attrition = c(matchedCohortDiagnostics$zeroT, matchedCohortDiagnostics$zeroC),
        cohortStatistics = cohortStatistics
    )

    class(concurrentComparatorData) <- "ConcurrentComparatorData"
    attr(class(concurrentComparatorData), "package") <- "ConcurrentComparator"

    return(concurrentComparatorData)
}

#' TODO: doc
#' Used only to translate sqlite
patchSql <- function(sql, dbms) {
    if (dbms == "sqlite") {
        # SQLite does not support floor; round instead
        sql <- gsub("floor\\(", "round\\(", sql)

        # SQLite does not support extract(year from VAR); use strftime('%Y', DATETIME(VAR, 'unixepoch', (0)||' days')) instead
        sql <- gsub("extract\\(year from (.*?)\\)", "strftime('%Y', DATETIME(\\1, 'unixepoch'))", sql)
    }

    return(sql)
}
