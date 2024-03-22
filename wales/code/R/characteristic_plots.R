library(ggplot2)

# Read in tables of Welsh demographic data

conditions <- read.csv('~/Documents/Wales/post_vaccination_conditions_by_age.csv')
ages <- read.csv('~/Documents/Wales/post_vaccination_ages_by_sex.csv')
abbrev_descriptions <- read.csv('~/Documents/old_codelists/CHD_code_descriptions/abbrev_descriptions.csv') %>% 
  mutate(PHENOTYPE_ABBREVIATION = code) %>% select(-code) %>% 
  mutate(description = str_to_sentence(description))

conditions_england <- read.csv('~/Downloads/ccu068_post_vaccination_condition_counts/ccu068_post_vaccination_condition_counts.csv')

# Plot 20 most common CHD phenotypes in Welsh data

## Add phenotype descriptions to records 

conditions <- conditions %>% inner_join(abbrev_descriptions, by = 'PHENOTYPE_ABBREVIATION') %>% 
      mutate(description = ifelse(description == 'Tetralogy of fallot', 'Tetralogy of Fallot', description)) %>% 
      mutate(description = ifelse(description == 'Patent ductus arterious', 'Patent ductus arteriosus', description)) %>% 
      filter(!description %in% c('Ebstein anomaly', 'Hypoplastic right heart', 'Other anomalies of the thoracic aorta'))

## Order phenotypes by total count

condition_order = conditions %>% group_by(description) %>% summarise(count = sum(count)) %>% arrange(count)
condition_order <- condition_order$description
age_order <- c('>50', '<50')
conditions$description = factor(conditions$description, levels = condition_order)
conditions$Age <- factor(conditions$Age, levels = age_order)

# Barplot of number of patients with each condition, split by age

ggplot(conditions, aes(x = description, y = count, fill = Age)) +
  geom_bar(stat = 'identity') + coord_flip() + 
  scale_fill_manual(values = c('#404788FF', '#20A387FF')) + 
  theme_minimal() +
  xlab('Phenotype description') + 
  ylab('Number of patients')

#Plot 20 most common conditions in English data

conditions_england <- conditions_england %>% mutate(phenotype_description = ifelse(phenotype_description == 'Tetralogy of fallot', 'Tetralogy of Fallot', phenotype_description)) %>% 
  mutate(phenotype_description = ifelse(phenotype_description == 'Patent ductus arterious', 'Patent ductus arteriosus', phenotype_description)) %>% 
  filter(phenotype_description != 'Mitral regurgitation')
condition_order = conditions_england %>% group_by(phenotype_description) %>% summarise(count = sum(count)) %>% arrange(count)
condition_order <- condition_order$phenotype_description
age_order <- c('>50', '<50')

conditions_england$phenotype_description = factor(conditions_england$phenotype_description, levels = condition_order)
conditions_england$Age <- factor(conditions_england$Age, levels = age_order)

ggplot(conditions_england, aes(x = phenotype_description, y = count, fill = Age)) +
  geom_bar(stat = 'identity') + coord_flip() + 
  scale_fill_manual(values = c('#404788FF', '#20A387FF')) + 
  theme_minimal() + 
  ylab('Number of patients') + xlab(('Phenotype description'))


# Plot distribution of ages, grouped by sex

age<- as.data.frame(c(0:99))
sex <- as.data.frame(c(rep('Male', times = 100), rep('Female', times = 100)))
age_sex <- cbind(age, sex) 
colnames(age_sex) = c('Age', 'Sex')
ages <- age_sex %>% left_join(ages, by = c('Age', 'Sex')) %>% replace(is.na(.), 0)


ggplot(ages, aes(x= Age, y = number, colour = Sex)) + geom_line() + 
  scale_colour_manual(values = c('#20A387FF', '#404788FF')) + 
  theme_minimal() + 
  ylab('Number of patients')



