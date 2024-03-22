library(dplyr)
library(dbplyr)
library(survival)
library(odbc)
library(DBI)
library(ggplot2)
library(tidyr)
library(stringr)
library(viridis)
con <- DBI::dbConnect(odbc:::odbc(), "databricks")

severity_CHD_cases <- tbl(con, in_catalog("hive_metastore", "dsa_391419_j3w9t_collab", "ccu068_cases_matched_severity_post_vaccination")) %>% 
                          collect()

total_cases <- nrow(severity_CHD_cases)
severity_percentages <- severity_CHD_cases %>% group_by(severity_score) %>% summarise(100*n()/total_cases) 
PH_percentage <- 100* nrow(severity_CHD_cases %>% filter(pulmonary_hypertension == 'Yes'))/total_cases

# Plot the 15 most common CHD phenotypes, grouped by age

conditions_CHD_cases <- tbl(con, in_catalog("hive_metastore", "dsa_391419_j3w9t_collab", "ccu068_cases_matched_conditions_post_vaccination")) %>% 
  mutate(phenotype_description = ifelse(phenotype_description %in% c('aortic valve disease', 'Aortic (valve) insufficiency or regurgitation', 'Aortic (valve) stenosis'), 'bicuspid aortic valve', phenotype_description)) %>% 
  collect()

conditions_CHD_cases <- conditions_CHD_cases %>% select(NHS_number, phenotype_description) %>% distinct()

condition_DOB <- conditions_CHD_cases %>% inner_join(personal_info_CHD_cases, by = 'NHS_number') %>% distinct()
condition_age <- condition_DOB %>% mutate(DOB = (as.numeric(format(DOB, "%Y")))) %>% 
                                    mutate(Age = ifelse(2021-DOB > 50, '>50', '<50'))
                                           
condition_age_plot <- condition_age %>% group_by(Age, phenotype_description) %>% summarise(count = n()) %>% 
                                     arrange(by_group=count) %>% top_n(16) %>% 
                                    mutate(phenotype_description = str_to_sentence(phenotype_description)) %>% 
                                    mutate(count = round(count/5) *5) %>% 
                                    filter(phenotype_description != 'General classification')

ggplot(condition_age_plot, aes(x = reorder(phenotype_description, count), y = count, fill = Age)) + geom_bar(position = 'stack', stat = 'identity') +
              coord_flip() + scale_fill_manual(values =c('#20A387FF', '#404788FF'))+
              xlab('Phenotype description') +
              ylab('Number of patients with condition') +theme_minimal()

# Plot the distribution of ages in the CHD group, grouped by sex

age_sex <- personal_info_CHD_cases %>% mutate(Age = 2021 - (as.numeric(format(DOB, "%Y")))) %>% 
                                      mutate(Sex = ifelse(sex == '1', 'Male', 'Female')) %>% 
                                      group_by(Age, Sex) %>% 
                                      summarise(number = round(n(), digits = -1))

ggplot(age_sex, aes(x = Age, y = number, colour = Sex)) + geom_line(size=0.8) +
                        theme_minimal()+
                        scale_colour_manual(values =c('#20A387FF', '#404788FF')) +
                        ylab('Number of CHD cases')

