library(dplyr)
library(dbplyr)
library(survival)
library(odbc)
library(DBI)
library(ggplot2)
con <- DBI::dbConnect(odbc:::odbc(), "databricks")
# DBI::dbDisconnect(con) run at end of session

# Read in and format data for CHD cases

personal_info_CHD_cases <- tbl(con, in_catalog("hive_metastore", "dsa_391419_j3w9t_collab", "ccu068_cases_matched_personal_info_post_vaccination")) %>% 
                            select(NHS_number, ethnicity, sex, DOB, gp_practice)
severity_CHD_cases <- tbl(con, in_catalog("hive_metastore", "dsa_391419_j3w9t_collab", "ccu068_cases_matched_severity_post_vaccination")) %>%
                            select(NHS_number, severity_score, pulmonary_hypertension)
CHD_cases_covid <- tbl(con, in_catalog("hive_metastore", "dsa_391419_j3w9t_collab", "ccu068_cases_matched_covid_post_vaccination"))

smoking_CHD_cases <- tbl(con, in_catalog("hive_metastore", "dsa_391419_j3w9t_collab", "ccu068_cases_matched_smoking_post_vaccination")) %>%
                          select(NHS_number, smoking_status)
CHD_cases <- personal_info_CHD_cases %>% 
                inner_join(CHD_cases_covid, by = 'NHS_number') %>% 
                inner_join(severity_CHD_cases, by = 'NHS_number') %>% 
                inner_join(smoking_CHD_cases, by = 'NHS_number') %>% 
                mutate(case_control = 'case') %>% 
                collect()

# Read in and format data for controls

personal_info_controls <- tbl(con, in_catalog("hive_metastore", "dsa_391419_j3w9t_collab", "ccu068_controls_matched_personal_info_post_vaccination")) %>% 
  select(NHS_number, ethnicity, sex, DOB, gp_practice)

controls_covid <- tbl(con, in_catalog("hive_metastore", "dsa_391419_j3w9t_collab", "ccu068_controls_matched_covid_post_vaccination"))
controls_smoking <- tbl(con, in_catalog("hive_metastore", "dsa_391419_j3w9t_collab", "ccu068_controls_matched_smoking_post_vaccination")) 

CHD_controls <- personal_info_controls %>% 
  inner_join(controls_covid, by = 'NHS_number') %>% 
  inner_join(controls_smoking, by = 'NHS_number') %>% 
  mutate(case_control = 'control') %>% 
  mutate(severity_score = 'control') %>% 
  mutate(pulmonary_hypertension = 'control') %>% 
  collect()

covid_outcomes <- CHD_cases %>% union(CHD_controls) %>%
  mutate(hospitalised_with_first_covid_infection = as.numeric(hospitalised_with_first_covid_infection == 'Yes')) %>% 
  mutate(death_from_first_covid_infection = as.numeric(death_from_first_covid_infection == 'Yes')) %>% 
  mutate(DOB = cut(2021 - as.numeric(format(DOB, "%Y")), breaks = 26))

# Get percentage hospitalised and death for cases and controls

total_controls = nrow(CHD_controls)
total_cases = nrow(CHD_cases)
hosp_percent_cases = covid_outcomes %>% filter(case_control == 'case') %>% group_by(hospitalised_with_first_covid_infection) %>% summarise(100*n()/total_cases)
death_percent_cases = covid_outcomes %>% filter(case_control == 'case') %>% group_by(death_from_first_covid_infection) %>% summarise(100*n()/total_cases)
hosp_percent_controls = covid_outcomes %>% filter(case_control == 'control') %>% group_by(hospitalised_with_first_covid_infection) %>% summarise(100*n()/total_controls)
death_percent_controls = covid_outcomes %>% filter(case_control == 'control') %>% group_by(death_from_first_covid_infection) %>% summarise(100*n()/total_controls)

# Fit conditional logistic regression model #

fit_hosp <- clogit(hospitalised_with_first_covid_infection ~ case_control + smoking_status  + strata(DOB, sex, ethnicity, gp_practice), data = covid_outcomes)
fit_death <- clogit(death_from_first_covid_infection ~ case_control + smoking_status + strata(DOB, sex, ethnicity, gp_practice), data = covid_outcomes)
summary(fit_hosp)
round(cbind('OR' = exp(coef(fit_hosp)), exp(confint(fit_hosp))), 3)
pval <- summary(fit_death)$coefficients[ ,"Pr(>|z|)"]

summary(fit_death)
round(cbind('OR' = exp(coef(fit_death)), exp(confint(fit_death))), 3)

### Look at the effect of predictors of severe COVID-19 outcomes in cases ###

case_only_outcomes = CHD_cases %>% 
  mutate(covid_infection = as.numeric(covid_infection == 'Yes')) %>% 
  mutate(hospitalised_with_first_covid_infection = as.numeric(hospitalised_with_first_covid_infection == 'Yes')) %>% 
  mutate(death_from_first_covid_infection = as.numeric(death_from_first_covid_infection == 'Yes')) %>% 
  mutate(DOB = ifelse(as.numeric(format(DOB, "%Y")) > 1970, '<50', '>50')) %>% 
  mutate(ethnicity = ifelse(ethnicity %in% c('A', 'B', 'C'), 'White', ethnicity)) %>% 
  mutate(ethnicity = ifelse(ethnicity %in% c('Z'), 'Unknown', ethnicity)) %>% 
  mutate(ethnicity = ifelse(ethnicity %in% c('White', 'Unknown'), ethnicity, 'Non-white')) %>% 
  mutate(smoking_status = ifelse(smoking_status %in% c('current_smoker', 'former_smoker'), 'current/former', smoking_status))

case_only_outcomes$smoking_status <- relevel(as.factor(case_only_outcomes$smoking_status), ref = 'never_smoked')
case_only_outcomes$ethnicity <- relevel(as.factor(case_only_outcomes$ethnicity), ref = 'White')
case_only_outcomes$severity_score <- relevel(as.factor(case_only_outcomes$severity_score), ref = 'Mild')

# 'smoking status' as a predictor can be switched to severity, pulmonary hypertension, or any of matching variables #
fit_hosp <- glm(hospitalised_with_first_covid_infection ~ severity_score + smoking_status + ethnicity + DOB + sex, data = case_only_outcomes, family = 'binomial')
fit_death <- glm(death_from_first_covid_infection ~ pulmonary_hypertension + smoking_status +  ethnicity + DOB + sex, data = case_only_outcomes, family = 'binomial')
summary(fit_hosp)
round(cbind('OR' = exp(coef(fit_hosp)), exp(confint(fit_hosp))), 3)
summary(fit_death)
round(cbind('OR' = exp(coef(fit_death)), exp(confint(fit_death))), 3)

# Exclude PH cases

case_only_outcomes_no_PH <- case_only_outcomes %>% filter(pulmonary_hypertension == 'No')


