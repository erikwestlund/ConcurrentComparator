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

#' @export
aggregateConcurrentComparatorResults <- function(results,
                                                  outputFolder = "./ConcurrentComparatorOutput") {

    table <- do.call(rbind,
            lapply(results, function(fileName) {
                tmp <- readRDS(fileName)

                list(
                    analysisId = tmp$analysisId,
                    targetId = tmp$targetId,
                    outcomeId = tmp$outcomeId,
                    estimate = tmp$treatmentEstimate$logRr,
                    control = tmp$controlValue
                )
            }))

    return(table)
}

#' @export
runConcurrentComparatorAnalyses <- function(connectionDetails,
                                            cdmDatabaseSchema,
                                            tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                            exposureDatabaseSchema = cdmDatabaseSchema,
                                            exposureTable,
                                            outcomeDatabaseSchema = cdmDatabaseSchema,
                                            outcomeTable,
                                            outputFolder = "./ConcurrentComparatorOutput",
                                            analysisList,
                                            targetIds,
                                            outcomeIds,
                                            controlIds,
                                            cdmVersion = "5",
                                            testing = FALSE) {

    # Note that this value gets updated in the `fitOutcomeModelAndSaveResults` function, which is called by
    # the `runAndSaveAnalysisToResults`. It is updated using the `<<-` operator.
    # This is a side effect that is necessary to collect the results of the analysis, which makes this code easier to
    # write and understand without extra parameters and control structures.
    results <- NULL

    outputFolder <- normalizePath(outputFolder, mustWork = FALSE)
    if (!file.exists(outputFolder)) {
        dir.create(outputFolder)
    }

    lapply(analysisList, function(analysis) {
        ParallelLogger::logInfo("Starting analysis ", analysis$analysisId)

        lapply(targetIds, function(targetId) {

            fileStem <- file.path(outputFolder,
                                  paste0("a", analysis$analysisId, "_",
                                         "t", targetId, "_"))

            if (length(outcomeIds) > 0) {
                runAndSaveAnalysisToResults(
                    results = results, # Note that results is updated in this function using the `<<-` operator
                    analysis = analysis,
                    filenamePrefix = "o",
                    fitControlValue = NA,
                    outcomeTable = outcomeTable,
                    timeAtRiskStart = analysis$timeAtRiskStart,
                    timeAtRiskEnd = analysis$timeAtRiskEnd,
                    washoutTime = analysis$washoutTime,
                    studyEndDate = analysis$studyEndDate,
                    fileStem = fileStem,
                    connectionDetails = connectionDetails,
                    cdmDatabaseSchema = cdmDatabaseSchema,
                    targetId = targetId,
                    outcomeIds = outcomeIds,
                    exposureDatabaseSchema = exposureDatabaseSchema,
                    exposureTable = exposureTable,
                    outcomeDatabaseSchema = outcomeDatabaseSchema,
                    testing = testing
                )
            }

            if (length(controlIds) > 0) {
                warning("deprecated use of `controlIds`.  use `CohortGenerator::generateNegativeControlOutcomeCohorts()`")

                runAndSaveAnalysisToResults(
                    results = results,
                    analysis = analysis,
                    filenamePrefix = "c",
                    fitControlValue = 0,
                    outcomeTable = "condition_era",
                    timeAtRiskStart = analysis$timeAtRiskStart,
                    timeAtRiskEnd = analysis$timeAtRiskEnd,
                    washoutTime = analysis$washoutTime,
                    studyEndDate = analysis$studyEndDate,
                    fileStem = fileStem,
                    connectionDetails = connectionDetails,
                    cdmDatabaseSchema = cdmDatabaseSchema,
                    targetId = targetId,
                    outcomeIds = outcomeIds,
                    exposureDatabaseSchema = exposureDatabaseSchema,
                    exposureTable = exposureTable,
                    outcomeDatabaseSchema = outcomeDatabaseSchema,
                    testing = testing
                )
            }
        })
    })

    return(results)
}

getAndSaveCcData <- function(
    fileStem,
    filenamePrefix,
    connectionDetails,
    cdmDatabaseSchema,
    targetId,
    outcomeIds,
    studyEndDate,
    exposureDatabaseSchema,
    exposureTable,
    outcomeDatabaseSchema,
    outcomeTable,
    timeAtRiskStart,
    timeAtRiskEnd,
    washoutTime,
    testing = FALSE
) {
    fileName <- paste0(fileStem, paste0(filenamePrefix, ".zip")) #

    if (!file.exists(fileName)) {
        ccData <- getDbConcurrentComparatorData(
            connectionDetails = connectionDetails,
            cdmDatabaseSchema = cdmDatabaseSchema,
            targetId = targetId,
            outcomeIds = outcomeIds,
            studyEndDate = studyEndDate,
            exposureDatabaseSchema = exposureDatabaseSchema,
            exposureTable = exposureTable,
            outcomeDatabaseSchema = outcomeDatabaseSchema,
            outcomeTable = outcomeTable,
            timeAtRiskStart = timeAtRiskStart,
            timeAtRiskEnd = timeAtRiskEnd,
            washoutTime = washoutTime,
            testing = testing)
        saveAndromeda(ccData, fileName)
    }

    ccData <- loadConcurrentComparatorData(fileName)

    return(list(
        fileName = fileName,
        data = ccData
    ))
}

fitOutcomeModelAndSaveResults <- function(
    results,
    outcomeIds,
    analysis,
    data,
    fitControlValue,
    filenamePrefix,
    fileStem
) {
    lapply(outcomeIds, function(outcomeId) {
        fileName <- paste0(fileStem, filenamePrefix, outcomeId, ".Rds")
        if (!file.exists(fileName)) {

            population = createStudyPopulation(data, outcomeId = outcomeId)

            fit <- fitOutcomeModel(population = population)
            fit$analysisId <- analysis$analysisId
            fit$controlValue <- fitControlValue

            saveRDS(fit, fileName)
        }

        results <<- c(results, fileName)
    })
}

runAndSaveAnalysisToResults <- function(
    results,
    analysis,
    fitControlValue,
    fileStem,
    filenamePrefix,
    connectionDetails,
    cdmDatabaseSchema,
    targetId,
    outcomeIds,
    studyEndDate,
    exposureDatabaseSchema,
    exposureTable,
    outcomeDatabaseSchema,
    outcomeTable,
    timeAtRiskStart,
    timeAtRiskEnd,
    washoutTime,
    testing = FALSE
) {
    ccDataOutput <- getAndSaveCcData(
       fileStem = fileStem,
       filenamePrefix = filenamePrefix,
       connectionDetails = connectionDetails,
       cdmDatabaseSchema = cdmDatabaseSchema,
       targetId = targetId,
       outcomeIds = outcomeIds,
       studyEndDate = analysis$studyEndDate,
       exposureDatabaseSchema = exposureDatabaseSchema,
       exposureTable = exposureTable,
       outcomeDatabaseSchema = outcomeDatabaseSchema,
       outcomeTable = outcomeTable,
       timeAtRiskStart = analysis$timeAtRiskStart,
       timeAtRiskEnd = analysis$timeAtRiskEnd,
       washoutTime = analysis$washoutTime,
       testing = testing
    )

    fitOutcomeModelAndSaveResults(
        results = results,
        outcomeIds = outcomeIds,
        analysis = analysis,
        data = ccDataOutput$data,
        fitControlValue = fitControlValue,
        filenamePrefix = filenamePrefix,
        fileStem = fileStem
    )

    close(ccDataOutput$data)
}
