# Copyright 2024 Observational Health Data Sciences and Informatics
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


#' Generate simulated population data for target and comparator suitable for analysis.
#'
#' @param targetId                              The ID of the target cohort.
#' @param outcomeIds                            The IDs of the outcomes.
#' @param n                                     The number of subjects in the simulated study population.
#' @param proportionSecondShot                  The proportion of subjects to receive a  second shot.
#' @param riskWindowAfterTargetExposureDays     The number of days after target exposure to consider subject at risk.
#' @param washoutPeriodDays                     The number of days after entering target cohort to consider subject
#'                                              '"safe" and eligible for comparator cohort.
#' @param highRiskGroupDefinition               A list to pass to the `filterPersonsAgainstHighRiskGroupDefinition()`
#'                                              defining which subjects are considered high risk.
#' @param matchHighRiskCriteria                 A value of `any` (has any high risk characteristic) or `all` (has all
#'                                              defined high risk characteristics) to
#'                                              define how to match high risk subjects.
#' @param lowRiskComparatorIncidence            The incidence rate of the outcome in the low risk comparator group
#'                                              (those who do not match the high risk criteria).
#' @param lowRiskTargetIncidence                The incidence rate of the outcome in the low risk target group (those
#'                                              who do not match the high risk criteria).
#' @param highRiskComparatorIncidence           The incidence rate of the outcome in the high risk comparator group
#'                                              (those who match the high risk criteria).
#' @param highRiskTargetIncidence               The incidence rate of the outcome in the high risk target group (those
#'                                              who match the high risk criteria).
#' @param studyStartDate                        The start date of the study in the format 'YYYY-MM-DD'.
#' @param studyEndDate                          The end date of the study in the format 'YYYY-MM-DD'.
#'
#' @return
#' A `tibble` containing the simulated study population data with the following columns:
#' exposureId strataId subjectId cohortStartDate timeAtRisk     y daysToEvent
#' - `exposureId`: 1 == target; 0 == comparator
#' - `strataId`: The stratum of the entry.
#' - `subjectId`: The subjectId of the person from the simulated `persons` table.
#' - `cohortStartDate`: The date the subject entered the cohort in the format 'YYYY-MM-DD'.
#' - `timeAtRisk`: The number of days in the risk window.
#' - `y`: The outcome status (1 = outcome occurred; 0 = outcome did not occur).
#' - `daysToEvent`: The number of days from cohortStartDate on which outcome occurs; NA if no outcome occured.
#'
#' @export
generateSimulatedPopulationAnalysisData <- function(
        targetId,
        outcomeIds,
        n = 100000,
        proportionSecondShot = 0.65,
        riskWindowAfterTargetExposureDays = 21,
        washoutPeriodDays = 22,
        highRiskGroupDefinition = list(),
        matchHighRiskCriteria = 'any',
        lowRiskComparatorIncidence = 0.0001,
        lowRiskTargetIncidence = 0.0001,
        highRiskComparatorIncidence = 0.0001,
        highRiskTargetIncidence = 0.0001,
        studyStartDate = '2020-12-18',
        studyEndDate = '2021-06-30'
){
    errorMessages <- checkmate::makeAssertCollection()
    checkmate::assertIntegerish(targetId, add = errorMessages)
    checkmate::assertVector(outcomeIds, add = errorMessages)
    checkmate::assertIntegerish(n, lower = 1, add = errorMessages)
    checkmate::assertNumeric(riskWindowAfterTargetExposureDays, lower = 1, add = errorMessages)
    checkmate::assertNumeric(washoutPeriodDays, lower = 1, add = errorMessages)
    checkmate::assertList(highRiskGroupDefinition, add = errorMessages)
    checkmate::assertChoice(matchHighRiskCriteria, choices = c('any', 'all'), add = errorMessages)
    checkmate::assertNumeric(lowRiskComparatorIncidence, lower = 0, upper = 1, add = errorMessages)
    checkmate::assertNumeric(lowRiskTargetIncidence, lower = 0, upper = 1, add = errorMessages)
    checkmate::assertNumeric(highRiskComparatorIncidence, lower = 0, upper = 1, add = errorMessages)
    checkmate::assertNumeric(highRiskTargetIncidence, lower = 0, upper = 1, add = errorMessages)
    checkmate::assertCharacter(studyStartDate, pattern = '^\\d{4}-\\d{2}-\\d{2}$', add = errorMessages)
    checkmate::assertCharacter(studyEndDate, pattern = '^\\d{4}-\\d{2}-\\d{2}$', add = errorMessages)
    checkmate::reportAssertions(collection = errorMessages)

    persons <- generateSimulatedPersonsData(n)

    cohort <- generateSimulatedCohortTable(
        persons = persons,
        targetId = targetId,
        outcomeIds = outcomeIds,
        proportionSecondShot = proportionSecondShot,
        riskWindowAfterTargetExposureDays = riskWindowAfterTargetExposureDays,
        washoutPeriodDays = washoutPeriodDays,
        highRiskGroupDefinition = highRiskGroupDefinition,
        matchHighRiskCriteria = matchHighRiskCriteria,
        lowRiskTargetIncidence = lowRiskTargetIncidence,
        lowRiskComparatorIncidence = lowRiskComparatorIncidence,
        highRiskTargetIncidence = highRiskTargetIncidence,
        highRiskComparatorIncidence = highRiskComparatorIncidence,
        studyEndDate = studyEndDate,
        targetDaysEligibleBeforeStudyEndDate = as.integer(as.Date(studyEndDate) - as.Date(studyStartDate))
    )

    targetCohort <- generateTargetCohortFromSimulatedCohortTable(
        cohort,
        targetCohortId = 666,
        riskWindowAfterTargetExposureDays = 21
    )

    comparatorCohort <- generateComparatorCohortFromSimulatedTargetCohortTable(
        targetCohort,
        washoutPeriodDays = 22,
        riskWindowAfterTargetExposureDays = 21
    )

    strata <- generateStrataFromCohort(targetCohort, persons)

    matchedStrata <- generateMatchedStrataForComparatorCohort(strata, comparatorCohort, persons)

    matchedCohort <- generateMatchedCohort(targetCohort, comparatorCohort, matchedStrata, persons)

    allOutcomes <- getOutcomesForMatchedCohort(
        cohort,
        matchedCohort,
        targetId = 666,
        outcomeIds = c(668),
        daysFromObsStart = 1,
        daysToObsEnd = 21
    )

    createSimulatedStudyPopulation(
        matchedCohort,
        allOutcomes,
        outcomeIds = c(668)
    )
}


#' Create a Cyclops-ready tibble for analysis using the `fitOutcomeModel` function.
#'
#' @param matchedCohort A tibble containing the matched cohort table generated by the `generateMatchedCohort` function.
#' @param allOutcomes   A tibble containing the outcomes for the matched cohort generated by the
#'                      `getOutcomesForMatchedCohort` function.
#' @param outcomeIds    A vector of outcome IDs to include in the study population.
#'
#' @return
#' A `tibble` containing the simulated study population data with the following columns:
#' - `exposureId`: The ID of the exposure cohort.
#' - `strataId`: The ID of the strata.
#' - `subjectId`: The ID of the subject.
#' - `cohortStartDate`: The start date of the target/comparator cohort.
#' - `timeAtRisk`: The time at risk in days for the subject.
#' - `y`: The outcome indicator (1 = outcome occurred, 0 = outcome did not occur).
#' - `daysToEvent`: The number of days to the outcome event from the cohort start date.
createSimulatedStudyPopulation <- function(matchedCohort, allOutcomes, outcomeIds) {
    outcomeMatchedCohort <- matchedCohort %>%
        select(exposureId, strataId, subjectId, cohortStartDate, timeAtRisk) %>%
        left_join(
            allOutcomes %>% filter(outcomeId %in% !!outcomeIds) %>%
                select(strataId, subjectId, daysToEvent, y),
            by = c("subjectId", "strataId")
        ) %>% mutate(
            y = ifelse(!is.na(y), 1, 0),
        ) %>%
        select(exposureId, strataId, subjectId, cohortStartDate, timeAtRisk, y, daysToEvent)
}

#' Get the adverse event outcomes for a matched cohort from the cohort table.
#'
#' @details
#' This function takes matched cohort tibble and finds all outcomes matching the specified `outcomeIds` that
#' occur within the risk window for each record. The risk window is defined using the `daysFromObsStart` and
#' `daysToObsEnd` parameters. If specified, outcomes are truncated by excluding records that occur after
#' `studyEndDate.`
#'
#' @param cohort            A tibble containing the cohort table generated by the `generateSimulatedCohortTable`
#'                          function.
#' @param matchedCohort     A tibble containing the matched cohort table generated by the `generateMatchedCohort`
#'                          function.
#' @param targetId          The target cohort definition ID.
#' @param outcomeIds        A vector of outcome cohort definition IDs.
#' @param daysFromObsStart  The number of days after the target cohort start date to begin the risk window for adverse
#'                          events.
#' @param daysToObsEnd      The number of days after the target cohort start date to end the risk window for adverse
#'                          events.
#' @param studyEndDate      The date to use as the end of the study period. If specified, outcomes that occur after this
#'                          date are excluded.
#'
#' @return
#' A tibble with the following columns:
#' - `subjectId`: The subject ID.
#' - `strataId`: The strata ID.
#' - `outcomeId`: The outcome cohort definition ID.
#' - `exposureId`: The exposure (i.e., target) cohort definition ID.
#' - `cohortStartDate`: The start date of the cohort.
#' - `daysToEvent`: The number of days from the cohort start date to the outcome event.
#' - `y`: A binary indicator of whether the outcome event occurred within the risk window.
getOutcomesForMatchedCohort <- function(
        cohort,
        matchedCohort,
        targetId,
        outcomeIds,
        daysFromObsStart = 1,
        daysToObsEnd = 21,
        studyEndDate = ""
) {
    outcomesCohortData <- cohort %>%
        filter(cohort_definition_id %in% outcomeIds) %>%
        rename(
            cohortDefinitionId = cohort_definition_id,
            subjectId = subject_id,
            cohortStartDate = cohort_start_date,
            cohortEndDate = cohort_end_date
        )

    matchedCohortData <- matchedCohort %>% filter(as.Date(cohortEndDate) - as.Date(cohortStartDate) > 0)

    if(studyEndDate != "") {
        outcomesCohortData <- outcomesCohortData %>%
            filter(as.Date(cohortStartDate) <= as.Date(studyEndDate))

        matchedCohortData <- matchedCohortData %>%
            filter(as.Date(cohortStartDate) <= as.Date(studyEndDate))
    }

    tempDb <- RSQLite::dbConnect(RSQLite::SQLite(), ":memory:")
    for (table in c('matchedCohortData', 'outcomesCohortData')) {
        RSQLite::dbWriteTable(
            tempDb,
            table,
            get(table),
            overwrite = TRUE
        )
    }

    outcomeIdsString <- paste(outcomeIds, collapse = ",")
    RSQLite::dbGetQuery(
        tempDb,
        glue::glue("
        SELECT DISTINCT (matchedCohort.subjectId),
            matchedCohort.strataId,
            matchedCohort.exposureId,
          outcomeCohort.cohortDefinitionId AS outcomeId,
          matchedCohort.cohortStartDate,
          matchedCohort.cohortEndDate,
          outcomeCohort.cohortStartDate as outcomeStartDate,
          outcomeCohort.cohortEndDate as outcomeEndDate,
          outcomeCohort.cohortStartDate - matchedCohort.cohortStartDate AS daysToEvent
        FROM matchedCohortData matchedCohort
        INNER JOIN outcomesCohortData outcomeCohort
          ON matchedCohort.subjectId = outcomeCohort.subjectId
        WHERE outcomeCohort.cohortDefinitionId IN ({outcomeIdsString})
            AND matchedCohort.cohortDefinitionId IN ({targetId})
            AND outcomeCohort.cohortStartDate - matchedCohort.cohortStartDate >= {daysFromObsStart}
          AND outcomeCohort.cohortStartDate - matchedCohort.cohortStartDate <= {daysToObsEnd}
        ")
    ) %>% mutate(
        y = 1
    ) %>% select(
        subjectId,
        strataId,
        outcomeId,
        exposureId,
        cohortStartDate,
        daysToEvent,
        outcomeStartDate,
        y
    ) %>%
        tibble()
}


#' Generates a person-level dataset including both the target and comparator cohort that can be used for analysis.
#'
#' @param targetCohort      A target cohort tibble generated by `generateTargetCohortFromSimulatedCohortTable` function.
#' @param comparatorCohort  A comparator cohort tibble generated by
#'                          `generateComparatorCohortFromSimulatedTargetCohortTable` function.
#' @param matchedStrata     A matched strata tibble generated by `generateMatchedStrataForComparatorCohort` function.
#' @param persons           A persons tibble generated by `generateSimulatedPersonsData` function.
#'
#' @return
#' A tibble with the following columns:
#' - `cohortDefinitionId`: The cohort definition ID for the cohort record.
#' - `exposureId`: Whether the record is from the target at-risk cohort. (1) or comparator safe cohort (0).
#' - `strataId`: The strata ID for the cohort record.
#' - `subjectId`: The subject ID for the cohort record.
#' - `cohortStartDate`: The cohort start date for the cohort record.
#' - `cohortEndDate`: The cohort end date for the cohort record.
#' - `ageGroupId`: The age group ID for the cohort.
#' - `genderConceptId`: The gender concept ID for the cohort.
#' - `raceConceptId`: The race concept ID.
#' - `ethnicityConceptId`: The ethnicity concept ID.
generateMatchedCohort <- function(targetCohort, comparatorCohort, matchedStrata, persons) {
    bind_rows(
        getPersonLevelDataFromCohortAndStrata(targetCohort, 'target', matchedStrata, persons),
        getPersonLevelDataFromCohortAndStrata(comparatorCohort, 'comparator', matchedStrata, persons)
    )  %>% rename(
        cohortDefinitionId = cohort_definition_id,
        exposureId = exposure_id,
        strataId = strata_id,
        subjectId = subject_id,
        cohortStartDate = cohort_start_date,
        cohortEndDate = cohort_end_date,
        ageGroupId = age_group_id,
        genderConceptId = gender_concept_id,
        raceConceptId = race_concept_id,
        ethnicityConceptId = ethnicity_concept_id
    ) %>%
        mutate(timeAtRisk = as.numeric(cohortEndDate - cohortStartDate))
}

#' Gets person-level dataset that contains only those cohort records that belong to the matched strata.
#'
#' @details
#' This function takes a cohort, exposureId, strata tibble, and persons tibble and returns a person-level
#' dataset that can be used for analysis.
#'
#' @param cohort     A target cohort tibble generated by `generateTargetCohortFromSimulatedCohortTable` function or
#'                   a comparator cohort tibble generated by `generateComparatorCohortFromSimulatedTargetCohortTable`
#'                   function.
#' @param exposureId The exposure ID for the cohort.
#' @param strata     A matched strata tibble generated by `generateMatchedStrataForComparatorCohort` function.
#' @param persons    A persons tibble generated by `generateSimulatedPersonsData` function.
#'
#' @return
#' A tibble with the following columns:
#' - `cohort_definition_id`: The cohort definition ID.
#' - `exposure_id`: Whether the record is from the target cohort (1) or comparator cohort (0).
#' - `strata_id`: The strata ID for the cohort record.
#' - `subject_id`: The subject ID for the cohort record.
#' - `cohort_start_date`: The cohort start date.
#' - `cohort_end_date`: The cohort end date.
#' - `age_group_id`: The age group ID for the record
#' - `gender_concept_id`: The gender concept ID for the record.
#' - `race_concept_id`: The race concept ID for the record.
#' - `ethnicity_concept_id`: The ethnicity concept ID for the record.
getPersonLevelDataFromCohortAndStrata <- function(cohort, cohortType, strata, persons) {
    errorMessages <- checkmate::makeAssertCollection()
    checkmate::assertDataFrame(cohort, add = errorMessages)
    checkmate::assertDataFrame(strata, add = errorMessages)
    checkmate::assertDataFrame(persons, add = errorMessages)
    checkmate::assertChoice(cohortType, c('target', 'comparator'), add = errorMessages)
    checkmate::reportAssertions(errorMessages)

    cohort %>%
        inner_join(persons, by = c("subject_id" = "person_id")) %>%
        mutate(age_group_id = getAgeGroupIdForReferenceDateAndDateOfBirth(cohort_start_date, birth_datetime)) %>%
        select(cohort_definition_id, subject_id, cohort_start_date, cohort_end_date, age_group_id, gender_concept_id,
               race_concept_id, ethnicity_concept_id) %>%
        inner_join(strata, by = c("cohort_definition_id", "cohort_start_date", "age_group_id", "gender_concept_id",
                                  "race_concept_id", "ethnicity_concept_id")) %>%
        mutate(
            exposure_id = ifelse(cohortType == 'target', 1, 0),
        ) %>%
        select(cohort_definition_id, exposure_id, strata_id, subject_id, cohort_start_date, cohort_end_date,
               age_group_id, gender_concept_id, race_concept_id, ethnicity_concept_id)
}


#' Gets unique strata from a cohort and persond tibble.
#'
#' @param cohort    A cohort tibble with cohort_definition_id, cohort_start_date
#' @param persons   A persons tibble generated by `generateSimulatedPersonsData` function.
#'
#' @return
#' A tibble with the following columns:
#' - cohort_definition_id: The ID of the cohort definition.
#' - strata_id: The ID of the strata, which is unique to each combination of cohort_definition_id,
#'              cohort_start_date, age_group_id, gender_concept_id, race_concept_id, and ethnicity_concept_id
#'              in the cohort tibble.
#' - cohort_start_date: The start date of the cohort.
#' - age_group_id: The age group ID as generated by the `getAgeGroupIdForReferenceDateAndDateOfBirth` function
#' - gender_concept_id: The OMOP gender concept ID.
#' - race_concept_id: The OMOP race concept ID.
#' - ethnicity_concept_id: The OMOP ethnicity concept ID.
generateStrataFromCohort <- function(cohort, persons) {
    cohort %>%
        inner_join(
            persons,
            by = c("subject_id" = "person_id")
        ) %>%
        mutate(
            age_group_id = getAgeGroupIdForReferenceDateAndDateOfBirth(cohort_start_date, birth_datetime)
        ) %>%
        select(cohort_definition_id, cohort_start_date, age_group_id, gender_concept_id, race_concept_id,
               ethnicity_concept_id) %>%
        distinct(cohort_definition_id, cohort_start_date, age_group_id, gender_concept_id, race_concept_id,
                 ethnicity_concept_id) %>%
        mutate(
            strata_id = rownames(.)
        ) %>%
        select(cohort_definition_id, strata_id, cohort_start_date, age_group_id, gender_concept_id, race_concept_id,
               ethnicity_concept_id) %>%
        arrange(cohort_start_date, age_group_id, gender_concept_id, race_concept_id, ethnicity_concept_id)
}

#' Generate matched strata from existing strata and a comparator cohort.
#'
#' @details
#' This gets all the strata that exist in a comparator cohort and matches them to the strata
#' in the provided strata tibble. This has effect of remove strata from the provided strata
#' that do not exist in the comparator cohort.
#'
#' @param strata            A strata tibble generated by `generateStrataFromCohort` function.
#' @param comparatorCohort  A comparator cohort tibble generated by
#'                          `generateComparatorCohortFromSimulatedTargetCohortTable` function.
#' @param persons           A persons tibble generated by `generateSimulatedPersonsData` function.
#'
#' @return
#' A tibble with the following columns:
#' - cohort_definition_id: The ID of the cohort definition.
#' - strata_id: The ID of the strata, which is unique to each combination of cohort_definition_id,
#'              cohort_start_date, age_group_id, gender_concept_id, race_concept_id, and ethnicity_concept_id
#'              in the cohort tibble.
#' - `cohort_start_date`: The start date of the cohort.
#' - `age_group_id`: The age group ID as generated by the `getAgeGroupIdForReferenceDateAndDateOfBirth` function
#' - `gender_concept_id`: The OMOP gender concept ID.
#' - `race_concept_id`: The OMOP race concept ID.
#' - `ethnicity_concept_id`: The OMOP ethnicity concept ID.
generateMatchedStrataForComparatorCohort <- function(strata, comparatorCohort, persons) {
    generateStrataFromCohort(comparatorCohort, persons) %>%
        select(-strata_id) %>%
        inner_join(
            strata,
            by = c("cohort_definition_id", "cohort_start_date", "age_group_id", "gender_concept_id", "race_concept_id",
                   "ethnicity_concept_id")
        ) %>%
        select(cohort_definition_id, strata_id, cohort_start_date, age_group_id, gender_concept_id, race_concept_id,
               ethnicity_concept_id)
}

#' Gets an age group ID from a reference date and date of birth.
#'
#' @param referenceDate     The date to use as the reference date for calculating age.
#' @param dateOfBirth       The date of birth of the person.
#'
#' @return
#' A numeric value corresponding to the bin number a person would fall in based on their age, with bins of 5 years
#' starting at birth.
getAgeGroupIdForReferenceDateAndDateOfBirth <- function(referenceDate, dateOfBirth) {
    cut(as.numeric(difftime(as.Date(referenceDate), as.Date(dateOfBirth), units = 'days')) / 365.25, seq(0, 100, 5),
        labels=FALSE)
}

#' Generate a target cohort from a simulated cohort table in the same format as
#' created by the definition encoded in CohortExtraction.sql.
#'
#' @param cohort                            A tibble generated by the `generateSimulatedCohortTable` function.
#' @param targetCohortId                    The ID of the target cohort definition.
#' @param riskWindowAfterTargetExposureDays The number of days after the target exposure to include in the risk window.
#'
#' @return
#' A tibble with the following columns:
#' - `cohort_definition_id`: The ID of the target cohort definition.
#' - `subject_id`: The person sequence ID of the subject.
#' - `cohort_start_date`: The date the cohort starts (i.e. the start of the risk window)
#' - `cohort_end_date`: The date the cohort ends (i.e., the end of the risk window)
generateTargetCohortFromSimulatedCohortTable <- function(
        cohort,
        targetCohortId,
        riskWindowAfterTargetExposureDays = 21
) {
    errorMessages <- checkmate::makeAssertCollection()
    checkmate::assertDataFrame(cohort, add = errorMessages)
    checkmate::assertVector(targetCohortId, add = errorMessages)
    checkmate::assertIntegerish(riskWindowAfterTargetExposureDays, add = errorMessages)
    checkmate::reportAssertions(collection = errorMessages)

    # Note: With real data, we would need to account for the observation date and end
    # set the cohort end date to that window if patient was no longer under observation.
    cohort %>%
        filter(cohort_definition_id == targetCohortId) %>%
        mutate(
            cohort_end_date = as.Date(cohort_start_date) + riskWindowAfterTargetExposureDays
        )
}

#' Generate a comparator cohort from a simulated target cohort table in the same format as
#' created by the definition encoded in CohortExtraction.sql.
#'
#' @details
#' The Comparator Cohort is generated by taking the latest entry for each person in the target cohort
#' and shifting the cohort start date by the washout period and the cohort end date by the risk window.
#'
#' @param targetCohort                       A tibble generated by the `generateTargetCohortFromSimulatedCohortTable`
#'                                           function.
#' @param riskWindowAfterTargetExposureDays  The number of days after the target exposure to include in the risk window.
#' @param washoutPeriodDays                  The number of days to shift the cohort start date by.
#'
#' @return
#' A tibble with the following columns:
#' - `cohort_definition_id`: The ID of the target cohort definition.
#' - `subject_id`: The person sequence ID of the subject.
#' - `cohort_start_date`: The date the cohort starts (i.e. the start of the safe window)
#' - `cohort_end_date`: The date the cohort ends (i.e., the end of the safe window)
generateComparatorCohortFromSimulatedTargetCohortTable <- function(
        targetCohort,
        riskWindowAfterTargetExposureDays = 21,
        washoutPeriodDays = 22
) {
    errorMessages <- checkmate::makeAssertCollection()
    checkmate::assertDataFrame(targetCohort, add = errorMessages)
    checkmate::assertIntegerish(riskWindowAfterTargetExposureDays, add = errorMessages)
    checkmate::reportAssertions(collection = errorMessages)

    getLatestCohortEntryFromEachPersonInTargetCohort(targetCohort) %>%
        mutate(
            cohort_start_date = as.Date(cohort_start_date) + washoutPeriodDays,
            cohort_end_date = cohort_start_date + riskWindowAfterTargetExposureDays
        )
}



#' Prints a tabulation of each covariate in the simulated persons data.
#'
#' @param persons         A tibble generated by the `generateSimulatedPersonsData` function.
#' @param studyEndDate    The date the study ended in YYYY-MM-DD format.
#'
#' @return
#' Void.
summarizeSimulatedTestData <- function(persons, studyEndDate = '2021-06-01') {
    data <- persons %>%
        mutate(age_group_id = getAgeGroupIdForReferenceDateAndDateOfBirth(studyEndDate, birth_datetime))

    for(c in c('age_group_id', 'gender_concept_id', 'race_concept_id', 'ethnicity_concept_id')) {
        data %>% group_by(across(all_of(c))) %>%
            summarise(
                n = n(),
                percent = n() / nrow(data)
            ) %>%
            arrange(desc(c)) %>%
            print(n = 21)
    }
}

#' Filters a persons tibble against a high risk group definition.
#'
#' @param persons                   A tibble generated by the `generateSimulatedPersonsData` function.
#' @param exposureCohort            A cohort against which to apply the risk criteria. This cohort is left-joined with
#'                                  persons.
#' @param highRiskGroupDefinition   A list of high risk group definitions. Each definition is a list with the following
#'                                  elements:
#'                                  - `var`: The variable to filter on. Must be one of `age_group_id`,
#'                                           `gender_concept_id`,
#'                                    `race_concept_id`, `ethnicity_concept_id`, `cohort_start_date`, or
#'                                     `cohort_end_date`
#'                                  - `operator`: The operator to use for the filter. Must be one of `=`, `==`, `<`,
#'                                                 `<=`, `>`, `>=`, `in`, or `between`
#'
#'                                    Note: between requires a 2-item vector and checks the value is between, inclusive
#'                                    of endpoints.
#'                                  - `val`: The value to filter on. If `operator` is `in`, this should be a vector of
#'                                     values.
#' @param forCriteria               A value of `any` or `all` to determine whether risk group must meet all (`all`)
#'                                  criteria or any one of the criteria (`any`).
#'
#' @return
#' A vector of unique person IDs that match the provided group definition.
filterPersonsAgainstHighRiskGroupDefinition <- function(persons, exposureCohort, highRiskGroupDefinition,
                                                        forCriteria = 'all') {
    if(length(highRiskGroupDefinition) == 0) return(c())

    data <- persons %>%
        left_join(exposureCohort, by = c('person_id' = 'subject_id')) %>%
        mutate(
            age_group_id = getAgeGroupIdForReferenceDateAndDateOfBirth(cohort_start_date, birth_datetime)
        )

    highRiskPersonIds <- c()

    for(i in 1:length(highRiskGroupDefinition)) {
        item <- highRiskGroupDefinition[[i]]

        errorMessages <- checkmate::makeAssertCollection()
        checkmate::assertChoice(item$operator, c('=', '==', '<', '<=', '>', '>=', 'in', 'between'), add = errorMessages)
        checkmate::assertChoice(item$var, c("age_group_id", "gender_concept_id", "race_concept_id",
                                            "ethnicity_concept_id", "cohort_start_date", "cohort_end_date"),
                                add = errorMessages)
        if(item$operator == 'in') checkmate::assertVector(item$val, add = errorMessages)
        if(item$operator == 'between') checkmate::assertVector(item$val, len = 2, add = errorMessages)
        checkmate::assertChoice(forCriteria, c('all', 'any'), add = errorMessages)
        checkmate::reportAssertions(collection = errorMessages)

        if(item$operator %in% c("=", "==")) {
            iData <- data %>%
                filter(!!sym(item$var) == item$val)
        } else if(item$operator == '<') {
            iData <- data %>%
                filter(!!sym(item$var) < item$val)
        } else if(item$operator == '<=') {
            iData <- data %>%
                filter(!!sym(item$var) <= item$val)
        } else if(item$operator == '>') {
            iData <- data %>%
                filter(!!sym(item$var) > item$val)
        } else if(item$operator == '>=') {
            iData <- data %>%
                filter(!!sym(item$var) >= item$val)
        } else if(item$operator == 'in') {
            iData <- data %>%
                filter(!!sym(item$var) %in% item$val)
        } else if(item$operator == 'between') {
            iData <- data %>%
                filter(
                    !!sym(item$var) >= item$val[1],
                    !!sym(item$var) <= item$val[2],
                )
        }

        matchingPersonIds <- iData %>% pull(person_id) %>% unique()

        if(forCriteria == 'any' || (forCriteria == 'all' && length(highRiskPersonIds) == 0)) {
            highRiskPersonIds <- union(highRiskPersonIds, matchingPersonIds) %>% unique()
        } else  {
            highRiskPersonIds <- intersect(highRiskPersonIds, matchingPersonIds) %>% unique()
        }
    }


    highRiskPersonIds
}

#' Generate outcomes for members of a target cohort.
#'
#' @param targetCohort                      A tibble generated by the `getLatestCohortEntryFromEachPersonInTargetCohort`
#'                                          function.
#' @param includePersonIds                  A vector of person IDs to include in the outcomes cohort. If empty, all
#'                                          persons in the target cohort will be included.
#' @param outcomeId                         The ID of the outcome cohort definition. TODO: Multiple outcomes?
#' @param outcomeRate                       The proportion of persons in the comparator cohort who will experience the
#'                                          outcome.
#' @param riskWindowAfterTargetExposureDays The number of days after the target exposure that the adverse event outcome
#'                                          can occur.
#'
#' @return
#' A tibble containing the outcome cohort with the the following columns:
#' - `cohort_definition_id`
#' - `subject_id`
#' - `cohort_start_date`
#' - `cohort_end_date`
generateOutcomesCohortFromTargetCohort <- function(
        targetCohort,
        includePersonIds = c(),
        outcomeId,
        outcomeRate,
        riskWindowAfterTargetExposureDays
) {
    data <- targetCohort

    if(length(includePersonIds) > 0) {
        data <- data %>%
            filter(subject_id %in% includePersonIds)
    }

    data %>%
        mutate(
            days_to_event = sample(1:(riskWindowAfterTargetExposureDays - 1), nrow(data), replace = TRUE)
        ) %>%
        sample_n(ceiling(nrow(data) * outcomeRate)) %>%
        mutate(
            cohort_definition_id = outcomeId,
            cohort_start_date = cohort_start_date + days_to_event, # Ensure start date is inside of the risk period
            cohort_end_date = cohort_start_date + 1
        ) %>%
        select(-days_to_event)
}

#' Generate outcomes for members of a comparator cohort using the last exposure of a target cohort.
#'
#' @details
#' This takes the last exposure of the target cohort and generates outcomes for the comparator cohort. The outcome
#' occurrence date will be a random day between `washoutPeriodDays` days after the date of the target cohort start date
#' (beginning of safe window) and `riskWindowAfterTargetExposureDays` days after the beginning of the safe window.
#'
#'
#' @param comparatorCohort                  A tibble generated by the `getLatestCohortEntryFromEachPersonInTargetCohort`
#'                                          function.
#' @param includePersonIds                  A vector of person IDs to include in the outcomes cohort. If empty, all
#'                                          persons in the
#'                                          comparator cohort will be included.
#' @param outcomeId                         The ID of the outcome cohort definition. TODO: Multiple outcomes?
#' @param outcomeRate                       The proportion of persons in the comparator cohort who will experience the
#'                                          outcome.
#' @param washoutPeriodDays                 The number of days after entering the target cohort until the person is
#'                                          considered no longer
#'                                          at risk of adverse events caused by exposure to the vaccine.
#' @param riskWindowAfterTargetExposureDays The number of days after entering the target cohort when a person is
#'                                          considered at risk of adverse events caused by exposure to the vaccine.
#'
#' @return
#' A tibble containing the outcome cohort with the the following columns:
#' - `cohort_definition_id`: The ID of the outcome cohort definition.
#' - `subject_id`: The ID of the subject.
#' - `cohort_start_date`: The start date of the outcome cohort.
#' - `cohort_end_date`: The end date of the outcome cohort.
generateOutcomesCohortForComparatorCohortFromLastExposureInTargetCohort <- function(
        targetCohortLastExposuresOnly,
        includePersonIds = c(),
        outcomeId,
        outcomeRate,
        washoutPeriodDays,
        riskWindowAfterTargetExposureDays
) {
    data <- targetCohortLastExposuresOnly

    if(length(includePersonIds) > 0) {
        data <- data %>%
            filter(subject_id %in% includePersonIds)
    }

    data %>%
        mutate(
            days_to_event = sample(1:(riskWindowAfterTargetExposureDays - 1), nrow(data), replace = TRUE)
        ) %>%
        sample_n(ceiling(nrow(data) * outcomeRate)) %>%
        mutate(
            cohort_definition_id = outcomeId,
            cohort_start_date = cohort_start_date + washoutPeriodDays + days_to_event,
            cohort_end_date = cohort_start_date + 1
        ) %>%
        select(-days_to_event)
}


#' Generate a target cohort from a persons tibble.
#'
#' @param persons                               A tibble generated by the `generateSimulatedPersonsData` function.
#' @param targetId                              The ID of the target cohort definition.
#' @param washoutPeriodDays                     The number of days after entering the target cohort until the person is
#'                                              considered no longer
#'                                              at risk of adverse events caused by exposure to the vaccine.
#' @param studyEndDate                          The date the study ended in YYYY-MM-DD format.
#' @param targetDaysEligibleBeforeStudyEndDate  The number of days before the study end date that a person is eligible
#'                                              to enter the target cohort.
#' @param proportionSecondShot                  The proportion of people in the target cohort who receive a second shot.
#'
#' @return
#' A tibble with the following columns:
#' - `cohort_definition_id`
#' - `subject_id`
#' - `cohort_start_date`
#' - `cohort_end_date`
generateTargetCohortFromPersons <- function(
        persons,
        targetId,
        washoutPeriodDays,
        studyEndDate,
        targetDaysEligibleBeforeStudyEndDate,
        proportionSecondShot,
        daysBetweenShots = 21
) {
    n <- nrow(persons)

    matrix(
        nrow = n,
        ncol = 4,
        dimnames = list(
            c(),
            c('cohort_definition_id', 'subject_id', 'cohort_start_date', 'cohort_end_date')
        )
    ) %>%
        as_tibble() %>%
        mutate(
            cohort_definition_id = targetId,
            subject_id = persons$person_id,
            cohort_start_date = sample(
                seq(
                    as.Date(studyEndDate) - targetDaysEligibleBeforeStudyEndDate,
                    as.Date(studyEndDate) - washoutPeriodDays,
                    by = 'day'
                ),
                n,
                replace = TRUE
            ),
            cohort_end_date = cohort_start_date + 1
        ) %>%
        bind_rows(
            filter(., cohort_definition_id == targetId) %>%
                sample_n(n * proportionSecondShot) %>%
                mutate(
                    cohort_start_date = cohort_end_date + daysBetweenShots,
                    cohort_end_date = cohort_start_date + 1
                )
        )
}

#' Gets the latest cohort entry from each person in the target cohort.
#'
#' @param targetCohort A tibble generated by the `generateTargetCohortFromPersons` function.
#'
#' @return
#' A tibble with the following columns:
#' - `cohort_definition_id`
#' - `subject_id`
#' - `cohort_start_date`
#' - `cohort_end_date`
getLatestCohortEntryFromEachPersonInTargetCohort <- function(targetCohort) {
    targetCohort %>%
        arrange(subject_id, cohort_start_date) %>%
        group_by(subject_id) %>%
        filter(row_number() == n()) %>%
        ungroup()
}

#' Generates a simulated cohort table in the format produced by the
#' `generateCohortSet` function in the OHDSI CohortGenerator package.
#'
#' @details
#' This function generates a simulated cohort table. The table includes records for each person in the
#' `persons` that are eligible for the target cohort. The target cohort is defined as the set of
#' people who received the vaccine. People are eligible to be in the target cohort up to two times. For example,
#' once for their first shot in a vaccine series and once for their second shot. The cohort table also includes
#' adverse event outcomes for a subset of the target cohort and its corresponding comparator cohort.
#' The comparator cohort is constructed of members of the target cohort in the period of time
#' after the washout period concludes after a target cohort member receives the vaccine. There are no records for
#' the comparator cohort in the generated table since it does not correspond to any observed treatment or outcome, but
#' rather a window of time when members of the target cohort are no longer considered potentially at a higher risk for
#' experiencing adverse events from the vaccine. This functions allows for the specification of a low-risk and high-risk
#' group of people who are eligible for the target cohort. Adverse event outcomes are generated in proportion to the
#' provided parameters for the low- and high-risk groups during both the post-vaccine risk period and the safe period
#' after the washout window concludes.
#'
#'
#' @param persons                                   A tibble with a format similar to the `persons` table in an OHDSI
#'                                                  CDM instance.
#'                                                  The tibble must include the following columns: `person_id`,
#'                                                  `birth_datetime`,
#'                                                  `gender_concept_id`, `race_concept_id`, and `ethnicity_concept_id`.
#' @param targetId                                  The ID of the target cohort that received the vaccine. This ID
#'                                                  corresponds to the ID of
#'                                                  the cohort in the JSON cohort definition file.
#' @param outcomeIds                                A vector of IDs of the adverse event outcomes that are being
#'                                                  studied. Each ID corresponds to the ID of
#'                                                  the cohort in the JSON cohort definition file.
#'                                                  TODO: Support multiple outcomes
#' @param proportionSecondShot                      The proportion of the target cohort that received a second shot of
#'                                                  the vaccine.
#' @param riskWindowAfterTargetExposureDays         The number of days after the target cohort received the vaccine that
#'                                                  a person is considered
#'                                                  at risk of the outcome.
#' @param washoutPeriodDays                         The number of days after the target cohort received the vaccine
#'                                                  after which they are considered not to be at risk of the adverse
#'                                                  event outcome being caused by the vaccine exposure.
#' @param highRiskGroupDefinition                   A list of items that define the high risk group. Each item is a list
#'                                                  with the following elements:
#'                                                  - var: The name of the variable in the `persons` that is used to
#'                                                         define the high risk group.
#'                                                         For example: "race_concept_id"
#'                                                  - operator: The operator to use to define the high risk group. This
#'                                                    can be one of the following:
#'                                                              "in": The variable must be in a list of values.
#'                                                              "between": The variable must be between two values,
#'                                                                         inclusive of the endpoints.
#'                                                              "=="
#'                                                              ">="
#'                                                              >
#'                                                              <=
#'                                                              <
#'                                                  - val: The value to use to define the high risk group
#'                                                         For example, "8515" for "Asian"
#' @param lowRiskTargetIncidence                    The proportion of the low risk group to generate adverse events in
#'                                                  the risk period.
#' @param lowRiskComparatorIncidence                The proportion of the low risk group to generate adverse events in
#'                                                  the safe period.
#' @param highRiskTargetIncidence                   The proportion of the high risk group to generate adverse events in
#'                                                  the risk period.
#' @param highRiskComparatorIncidence               The proportion of the high risk group to generate adverse events in
#'                                                  the safe period.
#' @param studyEndDate                              The date on which the study ends.
#' @param targetDaysEligibleBeforeStudyEndDate      The number of days before the study end date that a person is
#'                                                  eligible to be in the target cohort. For example, if the study end
#'                                                  date is 2021-06-30 and this value is 500, then the target cohort
#'                                                  can include people who received the vaccine between 2020-02-11 and
#'                                                  2021-06-30.
#'
#' @return
#' A tibble with the following columns:
#'
#' - cohort_definition_id:   The ID of the cohort for which the row is a member. The ID
#'                           corresponds to the ID of the cohort in the JSON cohort
#'                           definition file.
#' - subject_id:             The ID of the person who is a member of the cohort. This
#'                           corresponds to the person_id table in the `persons` table of
#'                           an OHDSI CDM instance
#' - cohort_start_date:      The date on which the person became a member of the
#'                           cohort (in the format YYYY-MM-DD)
#' - cohort_end_date:        The date on which the person ceased to be a member of the
#'                           cohort (in the format YYYY-MM-DD)
#' @export
generateSimulatedCohortTable <- function(
        persons,
        targetId = 666,
        outcomeIds = c(668),
        proportionSecondShot = 0.65,
        riskWindowAfterTargetExposureDays = 21,
        washoutPeriodDays = 22,
        highRiskGroupDefinition = c(), # By default, there is no high risk group
        matchHighRiskCriteria = 'all', # Match all
        lowRiskTargetIncidence = 0.0001,
        lowRiskComparatorIncidence = 0.0001,
        highRiskTargetIncidence = 0,
        highRiskComparatorIncidence = 0,
        studyEndDate = '2021-06-30',
        targetDaysEligibleBeforeStudyEndDate = 500
) {
    errorMessages <- checkmate::makeAssertCollection()
    checkmate::assertIntegerish(targetId, len = 1, add = errorMessages)
    checkmate::assertSubset(targetId, c(666, 667), add = errorMessages)
    checkmate::assertIntegerish(outcomeIds, add = errorMessages)
    checkmate::assertDouble(proportionSecondShot, len = 1, add = errorMessages)
    checkmate::assertIntegerish(riskWindowAfterTargetExposureDays, len = 1, add = errorMessages)
    if(length(highRiskGroupDefinition) > 0) checkmate::assertList(highRiskGroupDefinition, add = errorMessages)
    checkmate::assertDouble(highRiskTargetIncidence, len = 1, add = errorMessages)
    checkmate::assertDouble(lowRiskTargetIncidence, len = 1, add = errorMessages)
    checkmate::assertDouble(highRiskComparatorIncidence, len = 1, add = errorMessages)
    checkmate::assertDouble(lowRiskComparatorIncidence, len = 1, add = errorMessages)
    checkmate::assertCharacter(studyEndDate, len = 1, add = errorMessages)
    checkmate::reportAssertions(collection = errorMessages)

    targetCohort <- generateTargetCohortFromPersons(
        persons,
        targetId,
        washoutPeriodDays,
        studyEndDate,
        targetDaysEligibleBeforeStudyEndDate,
        proportionSecondShot
    )

    highRiskSubjectIds <- filterPersonsAgainstHighRiskGroupDefinition(persons, targetCohort, highRiskGroupDefinition,
                                                                      matchHighRiskCriteria)

    lowRiskSubjectIds <- persons %>%
        filter(!person_id %in% highRiskSubjectIds) %>%
        pull(person_id)

    # Generate outcomes from the target group in proportion to the specified
    # risk. Separately generate outcomes from the low risk group and the high
    # risk group.
    lowRiskOutcomeCohortFromTargetCohort <- generateOutcomesCohortFromTargetCohort(
        targetCohort,
        includePersonIds = lowRiskSubjectIds,
        outcomeId = outcomeIds[1],
        outcomeRate = lowRiskTargetIncidence,
        riskWindowAfterTargetExposureDays
    )

    highRiskOutcomeCohortFromTargetCohort <- generateOutcomesCohortFromTargetCohort(
        targetCohort,
        includePersonIds = highRiskSubjectIds,
        outcomeId = outcomeIds[1],
        outcomeRate = highRiskTargetIncidence,
        riskWindowAfterTargetExposureDays
    )

    # In the safe period, you first get only subject ids from target cohort
    # from subjects' last shot. This should be length of persons.
    # Then generate outcomes at the specific risks for the low and high risk
    # groups.
    targetGroupIncludingOnlyLastCohortEntryPerPerson <- getLatestCohortEntryFromEachPersonInTargetCohort(targetCohort)

    lowRiskOutcomeCohortFromComparatorCohort <- generateOutcomesCohortForComparatorCohortFromLastExposureInTargetCohort(
        targetGroupIncludingOnlyLastCohortEntryPerPerson,
        includePersonIds = lowRiskSubjectIds,
        outcomeId = outcomeIds[1],
        outcomeRate = lowRiskComparatorIncidence,
        washoutPeriodDays,
        riskWindowAfterTargetExposureDays
    )

    highRiskOutcomeCohortFromComparatorCohort <-
      generateOutcomesCohortForComparatorCohortFromLastExposureInTargetCohort(
        targetGroupIncludingOnlyLastCohortEntryPerPerson,
        includePersonIds = highRiskSubjectIds,
        outcomeId = outcomeIds[1],
        outcomeRate = highRiskComparatorIncidence,
        washoutPeriodDays,
        riskWindowAfterTargetExposureDays
    )

    targetCohort %>%
        bind_rows(
            lowRiskOutcomeCohortFromTargetCohort,
            highRiskOutcomeCohortFromTargetCohort,
            lowRiskOutcomeCohortFromComparatorCohort,
            highRiskOutcomeCohortFromComparatorCohort
        )
}


#' Generate a simulated persons table consistent with the OMOP CDM.
#'
#' @param n                     The number of persons to generate.
#' @param studyEndDate        The date the study ended in YYYY-MM-DD format.
#'
#' @return
#' A tibble with the following columns:
#'
#' - `person_id`: A unique identifier for each person.
#' - `birth_datetime`: The date of birth of the person in YYYY-MM-DD format.
#' - `gender_concept_id`: The OMOP CDM gender concept ID.
#' - `race_concept_id`: The OMOP CDM race concept ID.
#' - `ethnicity_concept_id`: The OMOP CDM ethnicity concept ID.
generateSimulatedPersonsData <- function(n = 1000, studyEndDate = '2021-06-01') {
    matrix(
        nrow = n,
        ncol = 6,
        dimnames = list(
            c(),
            c('person_id', 'birth_datetime', 'year_of_birth', 'gender_concept_id', 'race_concept_id',
              'ethnicity_concept_id')
        )
    ) %>%
        as_tibble() %>%
        mutate(
            person_id = 1:n,
            birth_datetime = getRandomDatesOfBirthFromAgeCategories(getRandomAgeCategories(n), studyEndDate),
            year_of_birth = as.numeric(format(birth_datetime, '%Y')),
            gender_concept_id = getRandomGenderCategories(n),
            race_concept_id = getRandomRaceCategories(n),
            ethnicity_concept_id = getRandomEthnicityCategories(n)
        )
}



#' Get random birth dates from age categories.
#'
#' @param ageCategories     A vector of age category IDs.
#' @param studyEndDate      The date the study ended in YYYY-MM-DD format.
#'
#' @return
#' A vector of dates of birth in YYYY-MM-DD format consistent with the provided
#' study start date.
getAgeGroupCategories <- function() {
    data <- tribble(
        ~age_group_id, ~name,
    )

    for (i in 1:20) {
        data <- data %>%
            add_row(
                age_group_id = i,
                name = paste0(
                    i * 5 - 5, "-", i * 5 - 1, " years"
                )
            )
    }

    ageProbabilities <- tribble(
        ~age_group_id, ~prob,
        1, .056, # 0-4
        2, .064, # 5-9
        3, .064, # 10-14
        4, .064, # 15-19
        5, .063, # 20-24
        6, .066, # 25-29
        7, .066, # 30-34
        8, .066, # 35-39
        9, .066, # 40-44
        10, .062, # 45-49
        11, .062, # 50-54
        12, .065, # 55-59
        13, .065, # 60-64
        14, .05, # 65-69
        15, .05, # 70-74
        16, .025, # 75-79
        17, .025, # 80-84
        18, .01, # 85-89
        19, .01, # 90-94
        20, .001, # 95-99
    )

    data %>%
        left_join(ageProbabilities, by = "age_group_id")
}

#' Get test vocabulary concepts using OMOP vocabulary.
#'
#' @return
#' A tibble with columns concept_id, name, domain, and probability.
#'
#' - concept_id: The unique identifier for the concept.
#' - name: The name of the concept.
#' - domain: The domain of the concept.
#' - prob: The probability of the concept, conditional on the domain.
#'
#' Probabilities roughly match the 2020 US Census data, with some exceptions.
#' Race, for example, will not sum to 1.0 because the Census allows for
#' respondents to select more than one response category.
getRandomAgeCategories <- function(n = 1) {
    ageGroupCategories <- getAgeGroupCategories()
    sample(ageGroupCategories$age_group_id, n, prob = ageGroupCategories$prob, replace = TRUE)
}

#' Get random dates of birth from a set of age categories.
#'
#' @param n     The number of categories to return.
#'
#' @return
#' A vector of random birthdays in YYYY-MM-DD format.
getRandomDatesOfBirthFromAgeCategories <- function(ageCategories, studyEndDate = '2021-06-01') {
    n <- length(ageCategories)

    datesOfBirth <- as.POSIXlt(as.Date(rep(studyEndDate, n)))
    datesOfBirth$year <- datesOfBirth$year - (ageCategories * 5 - sample(1:5, n, replace=TRUE))
    daysOfYear <- sample(1:365, n, replace=TRUE)
    datesOfBirth$mon <- ceiling(daysOfYear/31) - 1
    datesOfBirth$mday <- daysOfYear %% 31

    if_else(
        as.Date(datesOfBirth) >= as.Date(studyEndDate) - 31,
        rep(as.Date(studyEndDate) - 31, n),
        as.Date(datesOfBirth)
    )
}

#' Get random age categories in five-year bins, roughly representative of the US
#' population in 2020.
#'
#' @param n     The number of categories to return.
#'
#' @return
#' A vector of age category IDs.
getRandomEthnicityCategories <- function(n = 1) {
    ethnicityCategories <- getTestVocabConcepts() %>%
        filter(domain == "Ethnicity")

    sample(ethnicityCategories$concept_id, n, prob = ethnicityCategories$prob, replace=TRUE)
}


#' Get age group categories used to stratify the analysis population.
#'
#' @return
#' A tibble with the following columns:
#'
#' - age_group_id: The unique identifier for the age group.
#' - name: The name of the age group.
#' - prob: The probability of the age group.
#'
#' Probabilities roughly match the 2020 US Census data age reported in Table 2
#' in the following report, with some rounding and adjustment for bins that
#' do not align with five-year age groups used in this package.
#'
#' https://www2.census.gov/library/publications/decennial/2020/census-briefs/c2020br-06.pdf
getRandomGenderCategories <- function(n = 1) {
    genderCategories <- getTestVocabConcepts() %>%
        filter(domain == "Gender")

    sample(genderCategories$concept_id, n, prob = genderCategories$prob, replace = TRUE)
}

#' Get random race categories representative of the US population in 2020.
#'
#' @param n     The number of categories to return.
#'
#' @return
#' A vector of OMOP CDM race category concept IDs.
getRandomRaceCategories <- function(n = 1) {
    raceCategories <- getTestVocabConcepts() %>%
        filter(domain == "Race")

    sample(raceCategories$concept_id, n, prob = raceCategories$prob, replace = TRUE)
}

#' Get random ethnicity categories representative of the US population in 2020.
#'
#' @param n     The number of categories to return.
#'
#' @return
#' A vector of OMOP CDM ethnicity category concept IDs.
getTestVocabConcepts <- function() {
    tribble(
        ~concept_id, ~name, ~domain, ~prob,
        8532, "Female", "Gender", .509,
        8507, "Male", "Gender", .491,

        8657, "American Indian or Alaska Native", "Race", .013,
        8515, "Asian", "Race", .063,
        8516, "Black or African American", "Race", .136,
        8557, "Native Hawaiian or Other Pacific Islander", "Race", .003,
        8527, "White", "Race", .755,

        38003563, "Hispanic or Latino", "Ethnicity", .191,
        38003564, "Not Hispanic or Latino", "Ethnicity", .809,
    )
}
