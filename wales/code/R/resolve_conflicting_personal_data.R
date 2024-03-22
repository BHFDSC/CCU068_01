library(SAILDBUtils)
library(dplyr)
library(tidyr)

database_connection <- SAILConnect()
sail_db_query_case <- 'SELECT * FROM SAILWWMCCV.CCU068_CHD_CASES_VALID_RECORDS_UNMATCHED_PRE_VACCINATION'
sail_db_query_control <- 'SELECT * FROM SAILWWMCCV.CCU068_CHD_CONTROLS_VALID_RECORDS_UNMATCHED_PRE_VACCINATION'
sail_db_query_control_ethnicity <- 'SELECT * FROM SAILWWMCCV.CCU068_CHD_CONTROLS_VALID_RECORDS_UNMATCHED_PRE_VACCINATION_ethnicity'
sail_db_case <- runSQL(database_connection, sail_db_query_case)
sail_db_control <- runSQL(database_connection, sail_db_query_control)
sail_db_control_ethnicity <- runSQL(database_connection, sail_db_query_control_ethnicity) %>% mutate(ETHNICITY = as.character(ETHNICITY))

# State invalid values for each column

invalid_ethnicity_codes=c('Z',NA,0, '#',1,2,3,4,5,6,7,8,9,'12345')
invalid_gender_codes <- c(0,3, NA, 9)
invalid_GP_practices <- c('Z')
invalid_DOB <- c(NA)

# Remove people with date of birth after study window or before 1911

sail_db_case <- sail_db_case %>% filter(format(DOB, '%Y') >= 1912 & format(DOB, "%Y") < 2020)
sail_db_control <- sail_db_control %>% filter(format(DOB, '%Y') >= 1912 & format(DOB, "%Y") < 2020)

# Find most common or most recent entry for conflicting records #

resolve_conflicting_personal_data <- function(col_name, quoted_col, db, invalid_values){

  ids = db %>%
    select(ID, {{col_name}}, RECORD_DATE) %>%
    filter(!{{col_name}} %in% invalid_values) %>% 
    distinct()
  
  # Find most commonly recorded ethnicity for each person

  ids_grouped <- ids %>% group_by(ID, {{col_name}}) %>% summarise(count_col = n())
  
  ids_uncomplicated = ids_grouped %>% 
    group_by(ID) %>%
    filter(n() == 1) %>% 
    select(ID, {{col_name}})

  ids_grouped_conflicting <- ids_grouped %>% group_by(ID) %>% filter(n()>1)
  
  ids_part_resolved <- ids_grouped_conflicting %>% 
    mutate(max = max(count_col)) %>% 
    filter(count_col == max)

  ids_resolved_most_common <- ids_part_resolved %>% 
    group_by(ID) %>%
    filter(n() == 1) %>% 
    select(ID, {{col_name}})

  # Find most recently recorded ethnicity for ties
  
  ids_tie = ids_part_resolved %>% group_by(ID) %>% filter(n() > 1) %>% 
    inner_join(ids, by = c('ID', {{quoted_col}})) %>% 
    group_by(ID) %>% 
    mutate(most_recent = max(RECORD_DATE)) %>% 
    filter(RECORD_DATE == most_recent) %>% 
    select(ID, {{col_name}}) %>% 
    distinct(ID, .keep_all = TRUE)
  
  # Record Z for any people with no ethnicity recorded
  
  #as.Date('0000-01-01')
  ids_unknown <- db %>% select(ID) %>% anti_join(ids, by = 'ID') %>% distinct() %>% mutate(unknown_column = 'Z')
  colnames(ids_tie) = c('ID', 'unknown_column')
  colnames(ids_uncomplicated) = c('ID', 'unknown_column')
  colnames(ids_resolved_most_common) = c('ID', 'unknown_column')
  
  final_resolved <- ids_uncomplicated %>% union(ids_unknown) %>% union(ids_tie) %>% union(ids_resolved_most_common)
  colnames(final_resolved) = c('ID', quoted_col)
  return(final_resolved)
}

# Resolve personal info for cases

final_resolved_ethnicity_case = resolve_conflicting_personal_data(ETHNICITY, 'ETHNICITY', sail_db_case, invalid_ethnicity_codes)
final_resolved_gender_case = resolve_conflicting_personal_data(GENDER, 'GENDER', sail_db_case, invalid_gender_codes)
final_resolved_gp_case = resolve_conflicting_personal_data(GP_PRACTICE, 'GP_PRACTICE', sail_db_case, invalid_GP_practices)
final_resolved_DOB_case = resolve_conflicting_personal_data(DOB, 'DOB', sail_db_case, invalid_DOB)

resolved_ids_case <- final_resolved_ethnicity_case %>% 
                inner_join(final_resolved_DOB_case, by = 'ID') %>% 
                inner_join(final_resolved_gp_case, by = 'ID') %>% 
                inner_join(final_resolved_gender_case, by = 'ID')

write.csv(resolved_ids_case, 'ccu068_cases_personal_info_pre_vaccination.csv')

# Resolve personal info for controls

final_resolved_ethnicity_control = resolve_conflicting_personal_data(ETHNICITY, 'ETHNICITY', sail_db_control_ethnicity, invalid_ethnicity_codes)
final_resolved_gender_control = resolve_conflicting_personal_data(GENDER, 'GENDER', sail_db_control, invalid_gender_codes)
final_resolved_gp_control = resolve_conflicting_personal_data(GP_PRACTICE, 'GP_PRACTICE', sail_db_control, invalid_GP_practices)
final_resolved_DOB_control = resolve_conflicting_personal_data(DOB, 'DOB', sail_db_control, invalid_DOB)

resolved_ids_control <- final_resolved_gender_control %>% 
  inner_join(final_resolved_DOB_control, by = 'ID') %>% 
  inner_join(final_resolved_gp_control, by = 'ID') %>% 
  left_join(final_resolved_ethnicity_control, by = 'ID') %>% 
  mutate(ETHNICITY = replace_na(ETHNICITY, 'Z'))

write.csv(resolved_ids_control, 'ccu068_controls_personal_info_pre_vaccination.csv')
