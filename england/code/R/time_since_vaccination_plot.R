library(dplyr)
library(dbplyr)
library(survival)
library(odbc)
library(DBI)
library(ggplot2)
library(tidyr)
library(viridis)
con <- DBI::dbConnect(odbc:::odbc(), "databricks")

# Read in and format data for CHD cases

personal_info_CHD_cases <- tbl(con, in_catalog("hive_metastore", "dsa_391419_j3w9t_collab", "ccu068_cases_matched_personal_info_post_vaccination")) %>% 
  select(NHS_number, ethnicity, sex, DOB, gp_practice)
severity_CHD_cases <- tbl(con, in_catalog("hive_metastore", "dsa_391419_j3w9t_collab", "ccu068_cases_matched_severity_post_vaccination")) %>%
  select(NHS_number, severity_score, pulmonary_hypertension)
CHD_cases_covid <- tbl(con, in_catalog("hive_metastore", "dsa_391419_j3w9t_collab", "ccu068_cases_matched_covid_post_vaccination"))

CHD_cases <- personal_info_CHD_cases %>% 
  inner_join(CHD_cases_covid, by = 'NHS_number') %>% 
  inner_join(severity_CHD_cases, by = 'NHS_number') %>% 
  mutate(case_control = 'Case') %>% 
  collect()

# Read in and format data for controls

personal_info_CHD_controls <- tbl(con, in_catalog("hive_metastore", "dsa_391419_j3w9t_collab", "ccu068_controls_matched_personal_info_post_vaccination")) %>% 
  select(NHS_number, ethnicity, sex, DOB, gp_practice)
CHD_controls_covid <- tbl(con, in_catalog("hive_metastore", "dsa_391419_j3w9t_collab", "ccu068_controls_matched_covid_post_vaccination"))

CHD_controls <- personal_info_CHD_controls %>%
  inner_join(CHD_controls_covid, by = 'NHS_number') %>% 
  mutate(case_control = 'Control') %>% 
  mutate(severity_score = 'Control') %>% 
  mutate(pulmonary_hypertension = 'Control') %>% 
  collect()

# Combine CHD case and control records into single table

covid_outcomes <- CHD_cases %>% union(CHD_controls) %>%
  mutate(hospitalised_with_first_covid_infection = as.numeric(hospitalised_with_first_covid_infection == 'Yes')) %>% 
  mutate(death_from_first_covid_infection = as.numeric(death_from_first_covid_infection == 'Yes')) %>% 
  mutate(DOB = ifelse(as.numeric(format(DOB, "%Y")) < 1971, '>50', '<50'))

# Group cases and controls by days since vaccination and plot COVID hospitalisation rate

outcomes_days <- covid_outcomes %>% 
  mutate(days_since_vaccination = cut(days_since_vaccination, breaks = c(0,50,100,150,444))) %>% 
  group_by(case_control, days_since_vaccination)%>% 
  summarise(percent_hospitalised = 100*sum(hospitalised_with_first_covid_infection)/n())

reformatted_hosp_rate <- outcomes_days %>% 
  mutate(days_since_vaccination = ifelse(days_since_vaccination == '(0,50]', '0-50', days_since_vaccination))%>% 
  mutate(days_since_vaccination = ifelse(days_since_vaccination == '2', '50-100', days_since_vaccination)) %>% 
  mutate(days_since_vaccination = ifelse(days_since_vaccination == '3', '100-150', days_since_vaccination)) %>% 
  mutate(days_since_vaccination = ifelse(days_since_vaccination == '4', '150+', days_since_vaccination))

reformatted_hosp_rate$days_since_vaccination = factor(reformatted_hosp_rate$days_since_vaccination, levels = c('0-50', '50-100', '100-150', '150+'))

ggplot(reformatted_hosp_rate, aes(x = days_since_vaccination, y = percent_hospitalised, colour = case_control)) +
  geom_line(aes(group = case_control), size=1.3) +
  xlab('Time since most recent vaccination (days)') +
  ylab('Hospitalised with first COVID-19 infection (%)') +
  scale_colour_manual(values =c('#3b528b', '#5ec962')) + 
  theme_minimal() +
  guides(colour=guide_legend(title = 'Group')) +
  ylim(0,0.7)

# Group cases and controls by days since vaccination and plot COVID fatality rate

outcomes_days_death <- covid_outcomes %>% 
  mutate(days_since_vaccination = cut(days_since_vaccination, breaks = c(0,50,100, 150, 442))) %>% 
  group_by(case_control, days_since_vaccination) %>% 
  summarise(percent_death = 100*sum(death_from_first_covid_infection)/n())

reformatted_death_rate <- outcomes_days_death %>% 
  mutate(days_since_vaccination = ifelse(days_since_vaccination == '(0,50]', '0-50', days_since_vaccination))%>% 
  mutate(days_since_vaccination = ifelse(days_since_vaccination == '2', '50-100', days_since_vaccination)) %>% 
  mutate(days_since_vaccination = ifelse(days_since_vaccination == '3', '100-150', days_since_vaccination)) %>% 
  mutate(days_since_vaccination = ifelse(days_since_vaccination == '4', '150+', days_since_vaccination))

reformatted_death_rate$days_since_vaccination = factor(reformatted_death_rate$days_since_vaccination, levels = c('0-50', '50-100', '100-150', '150+'))

ggplot(reformatted_death_rate, aes(x =  days_since_vaccination, y = percent_death, colour = case_control)) +
  geom_line(aes(group = case_control), size=1.3) +
  xlab('Time since most recent vaccination (days)') +
  ylab('Death from first COVID-19 infection (%)') +
  scale_colour_manual(values =c('#3b528b', '#5ec962')) + 
  theme_minimal() +
  guides(colour=guide_legend(title = 'Group')) +
  ylim(0,0.85)

# Plot hospitalisation and death rates on a single graph

hospitalisation_death_rate = reformatted_hosp_rate %>% mutate(Outcome = 'Hospitalisation') %>% 
                              mutate(percent = percent_hospitalised) %>% select(-percent_hospitalised) %>% 
                              union(reformatted_death_rate %>%  mutate(Outcome =  'Death') %>% 
                              mutate(percent = percent_death) %>% select(-percent_death))

ggplot(hospitalisation_death_rate, aes(x =  days_since_vaccination, y = percent, colour = case_control, linetype = Outcome)) +
  geom_line(aes(group = interaction(Outcome, case_control)), linewidth = 0.9) +
  xlab('Time since most recent vaccination (days)') +
  ylab('Percent with outcome') +
  scale_colour_manual(values =c('#3b528b', '#5ec962')) + 
  theme_minimal() +
  guides(colour=guide_legend(title = 'Group')) +
  ylim(0,0.85)
