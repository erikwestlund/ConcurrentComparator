library(ConcurrentComparator)

set.seed(1234)

# Set population parameters:
n <- 300000
targetId <- 666
outcomeIds <- c(668)
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
matchHighRiskCriteria <- 'any'
studyStartDate <- '2020-12-18'
studyEndDate <- '2021-06-30'

# Generate simulated analysis-ready population data.
population <- generateSimulatedPopulationAnalysisData(
    n = n,
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
    studyStartDate = studyStartDate,
    studyEndDate = studyEndDate
)

# Run outcome model once:
fit <- fitOutcomeModel(population = population)

print(paste0("Simulated risk ratio: ", lowRiskTargetIncidence/lowRiskComparatorIncidence, ":1"))
print("Estimated risk ratios:")
print(exp(fit$coefficients))
print(exp(fit$treatmentEstimate))


# Simulate study multiple times with multiple population sizes using
# set incidences across all simulations.
comparatorIncidence <- 0.0001
targetIncidence <- 0.00015
for(n in c(100000, 250000, 500000, 750000, 1000000, 1500000, 2000000, 3000000)) {
    population <- generateSimulatedPopulationAnalysisData(
        n = n,
        targetId = targetId,
        outcomeIds = outcomeIds,
        proportionSecondShot = proportionSecondShot,
        timeAtRiskEndDays = timeAtRiskEndDays,
        washoutPeriodDays = washoutPeriodDays,
        highRiskGroupDefinition = highRiskGroupDefinition,
        lowRiskTargetIncidence = targetIncidence,
        lowRiskComparatorIncidence = comparatorIncidence,
        highRiskTargetIncidence = targetIncidence,
        highRiskComparatorIncidence = comparatorIncidence,
        studyStartDate = studyStartDate,
        studyEndDate = studyEndDate
    )

    fit <- fitOutcomeModel(population = population)

    print(paste0("N: ", n))
    print(paste0("Simulated risk ratio: ", targetIncidence/comparatorIncidence, ":1"))
    print("Estimated risk ratios:")
    print(exp(fit$coefficients))
    print(exp(fit$treatmentEstimate))
}
