library(SAILDBUtils)
library(dplyr)

database_connection <- SAILConnect()
sail_db_query_case <- 'SELECT * FROM SAILWWMCCV.CCU068_CHD_CASE_COVID_POST_VACCINATION'
sail_db_query_control <- 'SELECT * FROM SAILWWMCCV.CCU068_CHD_CONTROLS_COVID_POST_VACCINATION'
sail_db_case_covid <- runSQL(database_connection, sail_db_query_case)
sail_db_control_covid <- runSQL(database_connection, sail_db_query_control)

personal_info_cases <- read.csv('ccu068_cases_personal_info_post_vaccination.csv') %>% inner_join(sail_db_case_covid %>% select(ID, VACCINATED_PRE_INFECTION), by = 'ID')
personal_info_controls <- read.csv('ccu068_controls_personal_info_post_vaccination.csv') %>% inner_join(sail_db_control_covid %>% select(ID, VACCINATED_PRE_INFECTION), by = 'ID')

# Make matching ID - wider matching: ceiling((2021 - as.numeric(format(as.Date(DOB), '%Y')))/4)

personal_info_cases_matching <- unmatched_cases %>% mutate(matching_ID = paste(ETHNICITY, GP_PRACTICE, GENDER,ceiling((2021 - as.numeric(format(as.Date(DOB), '%Y')))/4), VACCINATED_PRE_INFECTION, sep = ''))
personal_info_controls_matching <- unmatched_controls %>% mutate(matching_ID = paste(ETHNICITY, GP_PRACTICE, GENDER,  ceiling((2021 - as.numeric(format(as.Date(DOB), '%Y')))/4), VACCINATED_PRE_INFECTION,  sep = ''))

#label rows - change max_case_number to case/control in function

grouped_cases <- personal_info_cases_matching %>% group_by(matching_ID) %>% mutate(row_number = row_number())
max_cases_per_group <-  personal_info_cases_matching %>% group_by(matching_ID) %>% mutate(max_case_number = n())


grouped_controls <- personal_info_controls_matching %>% group_by(matching_ID) %>% mutate(row_number = row_number())
max_controls_per_group <-  personal_info_controls_matching %>% group_by(matching_ID) %>% mutate(max_control_number = n())

# Match cases to controls

ratio = 2

# Split controls into groups of 4 with the same ID
max_control_groups <- max_controls_per_group %>% 
                      mutate(max_groups = floor(max_control_number/ratio)) %>% 
                      select(matching_ID, max_control_number, max_groups) %>% 
                      distinct()
grouped_controls_rounded <- max_control_groups %>%  
                      inner_join(grouped_controls, by = 'matching_ID') %>% 
                      filter(row_number <= max_control_number - (max_control_number%%ratio)) %>% 
                      select(-max_control_number)   

# Join cases and controls on matching ID and find case-control ratio
case_control_ratios <- max_control_groups %>% 
                       inner_join(max_cases_per_group, by = 'matching_ID') %>% 
                       mutate(max_controls_required = ratio * max_case_number) %>% 
                       mutate(max_cases_required = max_groups) %>% 
                       select(matching_ID, max_controls_required, max_cases_required) %>% 
                       distinct()

# Remove controls with row number above matching cases * ratio

matched_controls_temp_3 <- grouped_controls_rounded %>% 
                    inner_join(case_control_ratios, by = 'matching_ID') %>% 
                    filter(row_number <= max_controls_required) %>% 
                    distinct()

unmatched_controls <- grouped_controls %>% 
                      anti_join(matched_controls_temp_3, by = c('matching_ID', 'row_number')) %>% 
                      select(-matching_ID, - row_number) %>% 
                      distinct()
                      
# Remove cases with row number above matching controls/ratio

matched_cases_temp_3 <- grouped_cases %>% 
                 inner_join(case_control_ratios, by = 'matching_ID') %>% 
                 filter(row_number <= max_cases_required)
unmatched_cases <- grouped_cases %>% 
                   anti_join(matched_cases_temp_3, by = c('matching_ID', 'row_number'))

# Remove extra columns from cases and controls

matched_cases_temp_3 <- matched_cases_temp_3 %>% ungroup %>%  select('ID', 'GENDER', 'ETHNICITY', 'DOB', 'GP_PRACTICE', 'VACCINATED_PRE_INFECTION')
unmatched_cases <- unmatched_cases %>% ungroup %>%  select('ID', 'GENDER', 'ETHNICITY', 'DOB', 'GP_PRACTICE', 'VACCINATED_PRE_INFECTION')

matched_controls_temp_3 <- matched_controls_temp_3 %>% ungroup %>%  select('ID', 'GENDER', 'ETHNICITY', 'DOB', 'GP_PRACTICE', 'VACCINATED_PRE_INFECTION')
unmatched_controls <- unmatched_controls %>% ungroup %>% select('ID', 'GENDER', 'ETHNICITY', 'DOB', 'GP_PRACTICE', 'VACCINATED_PRE_INFECTION')

matched_cases <- matched_cases %>% union(matched_cases_temp) %>% union(matched_cases_temp_2) %>% union(matched_cases_temp_3)
matched_controls <- matched_controls %>% union(matched_controls_temp) %>% union(matched_controls_temp_2) %>% union(matched_controls_temp_3)

write.csv(matched_cases, 'ccu068_matched_cases_personal_info_post_vaccination.csv')
write.csv(matched_controls, 'ccu068_matched_controls_personal_info_post_vaccination.csv')
