library(ConcurrentComparator)

set.seed(123)

# Set population parameters:
n <- 1000000
targetId <- 666
outcomeIds <- c(668)
lowRiskTargetIncidence <- .0001
lowRiskComparatorIncidence <- .0001
highRiskTargetIncidence <- .0001
highRiskComparatorIncidence <- .0001
proportionSecondShot <- 0.65
riskWindowAfterTargetExposureDays <- 21
washoutPeriodDays <- 22
highRiskGroupDefinition <- list(
    list(
        'var' = 'age_group_id',
        'operator' = 'in',
        'val' = 14:20
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
    riskWindowAfterTargetExposureDays = riskWindowAfterTargetExposureDays,
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
        riskWindowAfterTargetExposureDays = riskWindowAfterTargetExposureDays,
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
