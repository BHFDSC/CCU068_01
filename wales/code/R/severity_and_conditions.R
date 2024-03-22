library(SAILDBUtils)
library(dplyr)
library(tidyr)
library(ggplot2)
library(stringr)
library(forcats)

database_connection <- SAILConnect()
query_CHD_records <- 'SELECT * FROM SAILWWMCCV.CCU068_CASE_CHD_RECORDS_UNMATCHED_PRE_VACCINATION'
query_case_all_records <- 'SELECT * FROM SAILWWMCCV.CCU068_CHD_CASES_VALID_RECORDS_UNMATCHED_PRE_VACCINATION'               
query_inclusion_codes <- 'SELECT * FROM SAILWWMCCV.CCU068_CHD_INCLUSION_CODES'

CHD_records <- runSQL(database_connection, query_CHD_records)
all_records <- runSQL(database_connection, query_case_all_records)
inclusion_codes <- runSQL(database_connection, query_inclusion_codes)

matched_cases <- read.csv('pre-vaccination/ccu068_matched_cases_personal_info_pre_vaccination.csv') %>% select(ID)
matched_CHD_records <- CHD_records %>% inner_join(matched_cases, by = 'ID')

# Find conditions for each person and counts of each condition

total_cases <- nrow(matched_CHD_records %>% select(ID) %>% distinct())
CHD_case_conditions <- matched_CHD_records %>% inner_join(inclusion_codes, by = c('CODE','CODE_TYPE' ))
                      
condition_counts <- CHD_case_conditions %>%
                    mutate(PHENOTYPE_ABBREVIATION = ifelse(str_detect('^AS$|^AR$', as.character(PHENOTYPE_ABBREVIATION)), 'BAV', as.character(PHENOTYPE_ABBREVIATION)))%>%
                    select(ID, PHENOTYPE_ABBREVIATION) %>% 
                    distinct()%>% 
                    group_by(PHENOTYPE_ABBREVIATION) %>% 
                    summarise(PERCENT_WITH_CODE = round(100*n()/total_cases, 3)) %>% 
                    arrange(desc(PERCENT_WITH_CODE))
condition_counts <- condition_counts %>% 
                    right_join(inclusion_codes %>% select(PHENOTYPE_ABBREVIATION) %>% distinct(), by = 'PHENOTYPE_ABBREVIATION') %>% 
                    mutate(PERCENT_WITH_CODE = replace_na(PERCENT_WITH_CODE, 0)) %>% 
                    filter(!PHENOTYPE_ABBREVIATION %in% c('AR', 'AS'))


# Assign severity classification to each CHD case

# Find people with cyanosis or PH

CHD_cases_cyanosis <- matched_CHD_records %>% select(ID) %>% 
                      inner_join(all_records %>% select(CODE, ID) %>%  distinct(), by = 'ID') %>% 
                      select(ID, CODE) %>% 
                      filter(CODE %in% c('XE1KK')) %>% 
                      mutate(PHENOTYPE_ABBREVIATION = 'CY') %>% 
                      mutate(ESC_COMPLEXITY_CLASSIFICATION = 'Severe') %>% 
                      select(-CODE)


CHD_cases_PH_code <- matched_CHD_records %>% select(ID) %>% 
  inner_join(all_records %>% select(ID, CODE, CODE_TYPE) %>% distinct(), by = 'ID') %>%
  select(ID, CODE, CODE_TYPE) %>% 
  filter((CODE %in% c('I272', 'I278', 'I279')) & (CODE_TYPE == 'ICD10')) %>% 
  mutate(PHENOTYPE_ABBREVIATION = 'PH') %>% 
  mutate(ESC_COMPLEXITY_CLASSIFICATION = 'Severe') %>% 
  select(-CODE, -CODE_TYPE)

PH_causing_conditions = c('ASD', 'VSD', 'AVSD', 'TOF', 'PDA', 'HLH', 'HRH', 'CAT', 'CAVV', 'DILV', 'DIV', 'DIRV', 'DOLV', 'DORV', 'DOV', 'SV', 'TGA', 'TAPVC', 'MS', 'BAV', 'SVAS', 'SAS', 'PVEM', 'IAA', 'SD', 'AR', 'AS', 'AVD')

CHD_cases_shunt <- matched_CHD_records %>%
  inner_join(CHD_case_conditions %>% select(ID, PHENOTYPE_ABBREVIATION) %>% distinct(), by = 'ID') %>% 
  filter(PHENOTYPE_ABBREVIATION %in% PH_causing_conditions) %>% 
  select('ID') %>% distinct()

CHD_cases_PH <- CHD_cases_PH_code %>% inner_join(CHD_cases_shunt, by = 'ID')


# Find people with septal defects and other associated abnormalities

CHD_cases_SD <- CHD_case_conditions %>% select(ID, PHENOTYPE_ABBREVIATION) %>% distinct() %>% 
  filter(PHENOTYPE_ABBREVIATION %in% c('ASD', 'VSD', 'AVSD', 'SD')) %>% 
  select('ID') %>% distinct()

CHD_cases_SD_associated_abnormalities <-  CHD_case_conditions %>% select(ID, PHENOTYPE_ABBREVIATION) %>% distinct() %>% 
  inner_join(CHD_cases_SD, by = 'ID') %>% 
  filter(!PHENOTYPE_ABBREVIATION %in%  c('ASD', 'VSD', 'AVSD', 'SD')) %>% 
  select(ID, PHENOTYPE_ABBREVIATION) %>% 
  mutate(ESC_COMPLEXITY_CLASSIFICATION = 'Moderate')

# Combine CHD records with PH/septal defect additional records

CHD_case_conditions_final <- CHD_case_conditions %>% select(PHENOTYPE_ABBREVIATION, ESC_COMPLEXITY_CLASSIFICATION, ID) %>% 
  union(CHD_cases_SD_associated_abnormalities) %>% 
  union(CHD_cases_cyanosis) %>% 
  union(CHD_cases_PH) %>% 
  distinct()

# Assign severity of most severe phenotype

severity = c(1,2,3)
names(severity) = c('Severe', 'Moderate', 'Mild')

CHD_case_severity <- CHD_case_conditions_final %>% 
  group_by(ID) %>% 
  arrange(severity[ESC_COMPLEXITY_CLASSIFICATION]) %>% 
  mutate(row = row_number()) %>% 
  filter(row == 1) %>% 
  select(ID, ESC_COMPLEXITY_CLASSIFICATION) %>% 
  left_join(CHD_cases_PH %>% select(ID) %>% mutate(PH = 'Yes'), by = 'ID') %>% 
  mutate(PH = replace_na(PH, 'No')) %>% 
  distinct()

CHD_case_severity %>% group_by(ESC_COMPLEXITY_CLASSIFICATION) %>% 
  summarise(100*n()/nrow(CHD_case_severity))


write.csv(CHD_case_severity, 'ccu068_matched_cases_severity_pre_vaccination.csv')

CHD_case_conditions_final <- CHD_case_conditions_final %>% select(ID, PHENOTYPE_ABBREVIATION, ESC_COMPLEXITY_CLASSIFICATION) %>% 
  distinct() %>% mutate(PHENOTYPE_ABBREVIATION = ifelse(str_detect('^AS$|^AR$', as.character(PHENOTYPE_ABBREVIATION)), 'BAV', as.character(PHENOTYPE_ABBREVIATION)))

write.csv(CHD_case_conditions_final, 'ccu068_matched_cases_phenotypes_pre_vaccination.csv')

# Demographic charts

# Make bar chart of conditions by age
condition_age <- CHD_case_conditions %>% select(PHENOTYPE_ABBREVIATION, DOB, ID) %>% 
                  mutate(DOB = (as.numeric(format(as.Date(DOB), '%Y')))) %>% 
                  mutate(Age = ifelse(2021-DOB >50, '>50', '<50') )

condition_age_plot <- condition_age %>% 
                        mutate(PHENOTYPE_ABBREVIATION = ifelse(str_detect('^AS$|^AR$', as.character(PHENOTYPE_ABBREVIATION)), 'BAV', as.character(PHENOTYPE_ABBREVIATION))) %>% 
                        select(ID, PHENOTYPE_ABBREVIATION, Age) %>% distinct() %>% 
                        group_by(Age, PHENOTYPE_ABBREVIATION) %>% 
                        summarise(count = n()) %>%
                        arrange(by_group = count) %>%  top_n(16) %>% 
                        mutate(count = round(count/5)*5) %>% 
                        filter(PHENOTYPE_ABBREVIATION != 'CHD')
top_16 <- condition_age %>% 
  mutate(PHENOTYPE_ABBREVIATION = ifelse(str_detect('^AS$|^AR$', as.character(PHENOTYPE_ABBREVIATION)), 'BAV', as.character(PHENOTYPE_ABBREVIATION))) %>% 
  select(ID, PHENOTYPE_ABBREVIATION, Age) %>% distinct() %>% 
  group_by(Age, PHENOTYPE_ABBREVIATION) %>% 
  summarise(count = n()) %>%
  arrange(by_group = count) %>%  top_n(16) %>% ungroup() %>% 
  select(PHENOTYPE_ABBREVIATION) %>% distinct()

condition_age_plot <- condition_age %>% 
  mutate(PHENOTYPE_ABBREVIATION = ifelse(str_detect('^AS$|^AR$', as.character(PHENOTYPE_ABBREVIATION)), 'BAV', as.character(PHENOTYPE_ABBREVIATION))) %>% 
  select(ID, PHENOTYPE_ABBREVIATION, Age) %>% distinct() %>% 
  filter(PHENOTYPE_ABBREVIATION != 'CHD') %>% 
  inner_join(top_16, by = 'PHENOTYPE_ABBREVIATION')

ggplot(condition_age_plot, aes(x = fct_rev(fct_infreq(PHENOTYPE_ABBREVIATION)), fill = Age)) +
                        geom_bar(position = 'stack') +
                        coord_flip() + scale_fill_manual(values = c('#20A387FF', '#404788FF')) +
                        xlab('Phenotype Abbreviation') + 
                        ylab('Number of patients with condition') + theme_minimal()

# Make plot of distribution of ages, grouped by sex

age_sex <- CHD_case_conditions %>% select(ID, DOB, GENDER) %>% 
            mutate(Age = (2021-as.numeric(format(DOB, '%Y')))) %>% 
            distinct() %>% 
            mutate(Sex =ifelse(GENDER == 1, 'Male', 'Female'))%>% 
            group_by(Age, Sex) %>% 
            summarise(number = round(n(), digits = -1)) %>% 
            filter(!is.na(Sex))

ggplot(age_sex, aes(x = Age, y =number, colour = Sex)) + geom_line(linewidth = 0.8) +
      theme_minimal() + 
      scale_colour_manual(values = c('#20A387FF', '#404788FF')) + 
      ylab('Number of CHD cases')

# Print demographic statistics

personal_info <-  read.csv('ccu068_cases_personal_info.csv') %>% mutate(DOB = as.character(DOB))
mean_age_case <-  personal_info %>% mutate(age = 2021-as.numeric(format(as.Date(DOB), '%Y'))) %>%  summarise(mean(age))

case_sex <- personal_info %>% group_by(GENDER) %>% summarise(100*n()/7021) 
case_ethnicity <- personal_info %>%
                  mutate(ETHNICITY = ifelse(ETHNICITY %in% c('A', 'B', 'C'), 'White', ETHNICITY)) %>%
                  mutate(ETHNICITY = ifelse(ETHNICITY %in% c('D', 'E', 'F', 'G'), 'Mixed', ETHNICITY)) %>% 
                  mutate(ETHNICITY = ifelse(ETHNICITY %in% c('H', 'J', 'K', 'L'), 'Asian', ETHNICITY)) %>% 
                  mutate(ETHNICITY = ifelse(ETHNICITY %in% c('M', 'N', 'P'), 'Black', ETHNICITY)) %>% 
                  mutate(ETHNICITY = ifelse(ETHNICITY %in% c('R', 'S'), 'Other', ETHNICITY)) %>% 
                  group_by(ETHNICITY) %>% summarise(100*n()/7021)



