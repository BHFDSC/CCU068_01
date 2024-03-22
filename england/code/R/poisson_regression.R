library(rstudioapi)
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))

# Read in table of disease event counts and person-days

incidence_rates <- read.csv('myocarditis_incidence_age_sex.csv', header = TRUE) %>% mutate(sex = as.character(sex)) %>% 
  filter(event %in% c('Baseline', '2nd vaccine')) #, 'Infected pre-vaccination'

# Set reference level for event and group

incidence_rates$group <- relevel(as.factor(incidence_rates$group), ref = 'Control')
incidence_rates$event <- relevel(as.factor(incidence_rates$event), ref = 'Baseline')

# Fit poisson regression for incidence rate ratios for each disease

model_myo <- glm(myo_cases ~ group * event + age + sex, family = 'poisson', data = incidence_rates, offset = log(person_days))
model_peri <- glm(peri_cases ~ group * event + age + sex, family = 'poisson', data = incidence_rates, offset = log(person_days))
model_endo <- glm(endo_cases ~ group * event + age + sex, family = 'poisson', data = incidence_rates, offset = log(person_days))
model_mi <- glm(MI_cases ~ group * event + age + sex, family = 'poisson', data = incidence_rates, offset = log(person_days))
model_PE <- glm(PE_cases ~ group * event + age + sex, family = 'poisson', data = incidence_rates, offset = log(person_days))
model_VT <- glm(VT_cases + PE_cases ~ group * event + age + sex, family = 'poisson', data = incidence_rates, offset = log(person_days))
model_IS <- glm(IS_cases ~ group * event + age + sex, family = 'poisson', data = incidence_rates, offset = log(person_days))
model_HS <- glm(HS_cases ~ group * event + age + sex, family = 'poisson', data = incidence_rates, offset = log(person_days))


# Print out estimated incidence rate ratios 

incidence_rate_ratios_endo <- round(cbind('IRR' = exp(coef(model_endo)), exp(confint(model_endo)), 'p-val' = summary(model_endo)$coefficients[,4]),2)
incidence_rate_ratios_myo <- round(cbind('IRR' = exp(coef(model_myo)), exp(confint(model_myo)), 'p-val' = summary(model_myo)$coefficients[,4]),2)
incidence_rate_ratios_peri <- round(cbind('IRR' = exp(coef(model_peri)), exp(confint(model_peri)), 'p-val' = summary(model_peri)$coefficients[,4]),4)


# Fit poisson regression to estimate IRR after vaccination separately for cases and controls

model_myo_case <- glm(myo_cases ~ event + age + sex, family = 'poisson', data = incidence_rates %>% filter(group == 'CHD'), offset = log(person_days))
model_peri_case <- glm(peri_cases ~ event + age + sex, family = 'poisson', data = incidence_rates %>% filter(group == 'CHD'), offset = log(person_days))
model_endo_case <- glm(endo_cases ~ event + age + sex, family = 'poisson', data = incidence_rates %>% filter(group == 'CHD'), offset = log(person_days))
model_mi_case <- glm(MI_cases ~ event + age + sex, family = 'poisson', data = incidence_rates %>% filter(group == 'CHD'), offset = log(person_days))
model_PE_case <- glm(PE_cases ~ event + age + sex, family = 'poisson', data = incidence_rates %>% filter(group == 'CHD'), offset = log(person_days))
model_VT_case <- glm(VT_cases + PE_cases ~ event + age + sex, family = 'poisson', data = incidence_rates %>% filter(group == 'CHD'), offset = log(person_days))
model_IS_case <- glm(IS_cases ~ event + age + sex, family = 'poisson', data = incidence_rates %>% filter(group == 'CHD'), offset = log(person_days))
model_HS_case <- glm(HS_cases ~ event + age + sex, family = 'poisson', data = incidence_rates %>% filter(group == 'CHD'), offset = log(person_days))


model_myo_control <- glm(myo_cases ~ event + age + sex, family = 'poisson', data = incidence_rates %>% filter(group == 'Control'), offset = log(person_days))
model_peri_control <- glm(peri_cases ~ event + age + sex, family = 'poisson', data = incidence_rates %>% filter(group == 'Control'), offset = log(person_days))
model_endo_control <- glm(endo_cases ~ event + age + sex, family = 'poisson', data = incidence_rates %>% filter(group == 'Control'), offset = log(person_days))
model_mi_control <- glm(MI_cases ~ event + age + sex, family = 'poisson', data = incidence_rates %>% filter(group == 'Control'), offset = log(person_days))
model_PE_control <- glm(PE_cases ~ event + age + sex, family = 'poisson', data = incidence_rates %>% filter(group == 'Control'), offset = log(person_days))
model_VT_control <- glm(VT_cases + PE_cases ~ event + age + sex, family = 'poisson', data = incidence_rates %>% filter(group == 'Control'), offset = log(person_days))
model_IS_control <- glm(IS_cases ~ event + age + sex, family = 'poisson', data = incidence_rates %>% filter(group == 'Control'), offset = log(person_days))
model_HS_control <- glm(HS_cases ~ event + age + sex, family = 'poisson', data = incidence_rates %>% filter(group == 'Control'), offset = log(person_days))


# Read in table of regression coefficients

subgroups_vacc <- read.csv('~/Documents/paper/Results\ tables/IRRs_subgroup_vaccination.csv') %>% filter(!Disease %in% c('Haemorrhagic stroke', 'VT', 'Pulmonary embolism'))

subgroups_covid_post_vacc <- read.csv('~/Documents/paper/Results\ tables/IRRs_subgroup_infected_post_vacc.csv') %>% mutate(event = 'Vaccinated')
subgroups_covid_pre_vacc <- read.csv('~/Documents/paper/Results\ tables/IRRs_subgroup_covid_pre_vacc.csv') %>% mutate(event = 'Unvaccinated')
subgroups_covid <- subgroups_covid_post_vacc %>% union(subgroups_covid_pre_vacc) #%>% filter(!Disease %in% c('Myocarditis', 'Pericarditis'))

# Plot IRRs after vaccination for CHD cases and controls

ggplot(subgroups_vacc, aes(x = Group, y = IRR)) +
  geom_errorbar(aes(x= Group, ymin = Lower, ymax = Upper, width = 0.2)) +
  geom_point() +
  ylab('Incidence rate ratio (14 days post-vaccination vs baseline)') +
  facet_grid(.~Disease, scales = 'free_x', space = 'free_x') +
  scale_y_log10() +
  theme(panel.spacing.x = unit(1, "cm")) +
  geom_hline(aes(yintercept=1), linetype='dashed') +
  theme_bw()

# Plot IRRs after COVID for cases and controls

ggplot(subgroups_covid, aes(x = event, y = IRR, colour = Group)) +
  geom_errorbar(aes(x=event, ymin = Lower, ymax = Upper, width = 0.2)) +
  geom_point() +
  ylab('Incidence rate ratio (14 days post-infection vs baseline)') +
  facet_grid(.~Disease, scales = 'free_x', space = 'free_x') +
  scale_y_log10() +
  theme(panel.spacing.x = unit(1, "cm")) +
  geom_hline(aes(yintercept=1), linetype='dashed') +
  theme_bw() + 
  xlab('')


# Test for interaction 

mod1.add <- glm(VT_cases + PE_cases ~ group + event + age + sex, family = 'poisson', data = incidence_rates, offset = log(person_days))
mod1.int <- glm(VT_cases + PE_cases ~ group * event + age + sex, family = 'poisson', data = incidence_rates, offset = log(person_days))
anova(mod1.add, mod1.int, test = "Chisq")
