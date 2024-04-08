test_that("Outcome model recovers null effect when no incidence difference between groups in all scenarios.", {

  n <- 300000
  targetIncidence <- 0.1
  comparatorIncidence <- 0.1
  lowRiskTargetIncidence <- targetIncidence
  highRiskTargetIncidence <- targetIncidence
  lowRiskComparatorIncidence <- comparatorIncidence
  highRiskComparatorIncidence <- comparatorIncidence

  riskRatioExpectation <- targetIncidence/comparatorIncidence
  riskRatioTolerance <- 0.1 # after exponentiation

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
     testData <- generateTestData(
            n =  n,
            proportionSecondShot = 0.65,
            studyStartDate = '2020-12-18',
            studyEndDate = '2021-06-30',
            timeAtRiskStartDays = scenario$timeAtRiskStartDays,
            timeAtRiskEndDays = scenario$timeAtRiskEndDays,
            washoutPeriodDays = scenario$washoutPeriodDays,
            lowRiskTargetIncidence = lowRiskTargetIncidence,
            lowRiskComparatorIncidence = lowRiskComparatorIncidence,
            highRiskTargetIncidence = highRiskTargetIncidence,
            highRiskComparatorIncidence = highRiskComparatorIncidence
        )

     population <- createStudyPopulation(testData$ccData, outcomeId = 668)
     fit <- fitOutcomeModel(population = population)
     exponentiatedExposureCoefficient <- unname(exp(fit$coefficients[1]))

     expect_true(
         all.equal(exponentiatedExposureCoefficient, riskRatioExpectation, tolerance = riskRatioTolerance),
         info = paste0("[Expect Null] Scenario: startDays=", scenario$timeAtRiskStartDays, ", endDays=", scenario$timeAtRiskEndDays, ", washout=", scenario$washoutPeriodDays)
     )
  }
})

test_that("Outcome model recovers incidence between groups in all scenarios.", {

    n <- 300000
    targetIncidence <- .2
    comparatorIncidence <- 0.1
    lowRiskTargetIncidence <- targetIncidence
    highRiskTargetIncidence <- targetIncidence
    lowRiskComparatorIncidence <- comparatorIncidence
    highRiskComparatorIncidence <- comparatorIncidence

    riskRatioExpectation <- targetIncidence/comparatorIncidence
    riskRatioTolerance <- 0.25 # after exponentiation

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
        testData <- generateTestData(
            n =  n,
            proportionSecondShot = 0.65,
            studyStartDate = '2020-12-18',
            studyEndDate = '2021-06-30',
            timeAtRiskStartDays = scenario$timeAtRiskStartDays,
            timeAtRiskEndDays = scenario$timeAtRiskEndDays,
            washoutPeriodDays = scenario$washoutPeriodDays,
            lowRiskTargetIncidence = lowRiskTargetIncidence,
            lowRiskComparatorIncidence = lowRiskComparatorIncidence,
            highRiskTargetIncidence = highRiskTargetIncidence,
            highRiskComparatorIncidence = highRiskComparatorIncidence
        )

        population <- createStudyPopulation(testData$ccData, outcomeId = 668)
        fit <- fitOutcomeModel(population = population)
        exponentiatedExposureCoefficient <- unname(exp(fit$coefficients[1]))

        expect_true(
            all.equal(exponentiatedExposureCoefficient, riskRatioExpectation, tolerance = riskRatioTolerance),
            info = paste0("[Expect ", round(riskRatioExpectation) , "] Scenario: startDays=", scenario$timeAtRiskStartDays, ", endDays=", scenario$timeAtRiskEndDays, ", washout=", scenario$washoutPeriodDays)
        )
    }
})


test_that("Outcome model recovers null effect when baseline incidence in target vs. comparator are equal, but high risk groups have higher incidence.", {

    n <- 300000
    lowRiskTargetIncidence <- .01
    highRiskTargetIncidence <- .04
    lowRiskComparatorIncidence <- .01
    highRiskComparatorIncidence <- .04

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

    riskRatioExpectation <- lowRiskTargetIncidence/lowRiskComparatorIncidence
    riskRatioTolerance <- 0.25 # after exponentiation

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
        testData <- generateTestData(
            n =  n,
            proportionSecondShot = 0.65,
            studyStartDate = '2020-12-18',
            studyEndDate = '2021-06-30',
            timeAtRiskStartDays = scenario$timeAtRiskStartDays,
            timeAtRiskEndDays = scenario$timeAtRiskEndDays,
            washoutPeriodDays = scenario$washoutPeriodDays,
            lowRiskTargetIncidence = lowRiskTargetIncidence,
            lowRiskComparatorIncidence = lowRiskComparatorIncidence,
            highRiskTargetIncidence = highRiskTargetIncidence,
            highRiskComparatorIncidence = highRiskComparatorIncidence,
            matchHighRiskCriteria = 'all',
            highRiskGroupDefinition = highRiskGroupDefinition
        )

        population <- createStudyPopulation(testData$ccData, outcomeId = 668)
        fit <- fitOutcomeModel(population = population)
        exponentiatedExposureCoefficient <- unname(exp(fit$coefficients[1]))

        expect_true(
            all.equal(exponentiatedExposureCoefficient, riskRatioExpectation, tolerance = riskRatioTolerance),
            info = paste0("[Expect ", round(riskRatioExpectation) , "] Scenario: startDays=", scenario$timeAtRiskStartDays, ", endDays=", scenario$timeAtRiskEndDays, ", washout=", scenario$washoutPeriodDays)
        )
    }
})

