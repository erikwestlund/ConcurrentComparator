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

                ccDataOutput <- getAndSaveCcData(
                   fileStem = fileStem,
                   filenamePrefix = "o",
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

                lapply(outcomeIds, function(outcomeId) {

                    fileName <- paste0(fileStem, "o", outcomeId, ".Rds")
                    if (!file.exists(fileName)) {

                        population = createStudyPopulation(ccDataOutput$data,
                                                           outcomeId = outcomeId)

                        fit <- fitOutcomeModel(population = population)
                        fit$analysisId <- analysis$analysisId
                        fit$controlValue <- NA

                        saveRDS(fit, fileName)
                    }

                    results <<- c(results, fileName)
                })

                close(ccDataOutput$data)
            }

            # TODO Remove code duplication (slight differences marked with X)

            if (length(controlIds) > 0) { # X

                warning("deprecated use of `controlIds`.  use `CohortGenerator::generateNegativeControlOutcomeCohorts()`")

                ccDataOutput <- getAndSaveCcData(
                   fileStem = fileStem,
                   filenamePrefix = "c",
                   connectionDetails = connectionDetails,
                   cdmDatabaseSchema = cdmDatabaseSchema,
                   targetId = targetId,
                   outcomeIds = controlIds,
                   studyEndDate = analysis$studyEndDate,
                   exposureDatabaseSchema = exposureDatabaseSchema,
                   exposureTable = exposureTable,
                   outcomeDatabaseSchema = cdmDatabaseSchema,
                   outcomeTable = "condition_era",
                   timeAtRiskStart = analysis$timeAtRiskStart,
                   timeAtRiskEnd = analysis$timeAtRiskEnd,
                   washoutTime = analysis$washoutTime,
                   testing = testing
                )

                lapply(controlIds, function(outcomeId) { # X

                    fileName <- paste0(fileStem, "c", outcomeId, ".Rds") # X
                    if (!file.exists(fileName)) {

                        population = createStudyPopulation(ccDataOutput$data,
                                                           outcomeId = outcomeId)

                        fit <- fitOutcomeModel(population = population)
                        fit$analysisId <- analysis$analysisId
                        fit$controlValue <- 0

                        saveRDS(fit, fileName)
                    }

                    results <<- c(results, fileName)
                })

                close(ccDataOutput$data)
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

getAndSaveOutcomeResults <- function(

) {
    # TODO
}
