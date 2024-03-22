library(survival)
library(dplyr)
library(SAILDBUtils)


chd_case_info<- read.csv('ccu068_matched_cases_personal_info_post_vaccination.csv') %>% select(-X) %>% mutate(case_control = 'case')
control_info <- read.csv('ccu068_matched_controls_personal_info_post_vaccination.csv') %>% select(-X) %>% mutate(case_control = 'control')

database_connection <- SAILConnect()
sail_db_query_control <- 'SELECT * FROM SAILWWMCCV.CCU068_CHD_CONTROLS_COVID_POST_VACCINATION'
sail_db_control_covid <- runSQL(database_connection, sail_db_query_control) 
sail_db_query_case <- 'SELECT * FROM SAILWWMCCV.CCU068_CHD_CASE_COVID_POST_VACCINATION'
sail_db_case_covid <- runSQL(database_connection, sail_db_query_case)

covid_outcomes <- chd_case_info %>% inner_join(sail_db_case_covid, by = 'ID') %>% 
                  union_all(control_info %>% inner_join(sail_db_control_covid, by = 'ID')) %>% 
                  mutate(case_control = as.numeric(case_control == 'case')) %>% 
                  mutate(HOSPITALISED_WITH_FIRST_COVID_INFECTION = as.numeric(HOSPITALISED_WITH_FIRST_COVID_INFECTION == 'Yes')) %>% 
                  mutate(DEATH_FROM_FIRST_INFECTION = as.numeric(DEATH_FROM_FIRST_INFECTION == 'Yes'))

fit_hosp <- clogit(HOSPITALISED_WITH_FIRST_COVID_INFECTION ~ case_control + strata(DOB, GP_PRACTICE, ETHNICITY, GENDER), data = covid_outcomes)
fit_death <- clogit(DEATH_FROM_FIRST_INFECTION ~ case_control + strata(DOB, GP_PRACTICE, ETHNICITY, GENDER), data = covid_outcomes)
summary(fit_hosp)
summary(fit_death)

percent_hosp_cases = covid_outcomes %>% filter(case_control == 1) %>% group_by(HOSPITALISED_WITH_FIRST_COVID_INFECTION) %>% summarise(100*n()/nrow(chd_case_info))
percent_hosp_controls = covid_outcomes %>% filter(case_control == 0) %>% group_by(HOSPITALISED_WITH_FIRST_COVID_INFECTION) %>% summarise(100*n()/nrow(control_info))

percent_death_cases = covid_outcomes %>% filter(case_control == 1) %>% group_by(DEATH_FROM_FIRST_INFECTION) %>% summarise(100*n()/nrow(chd_case_info))
percent_death_controls = covid_outcomes %>% filter(case_control == 0) %>% group_by(DEATH_FROM_FIRST_INFECTION) %>% summarise(100*n()/nrow(control_info))
