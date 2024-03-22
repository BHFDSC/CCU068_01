library(ggplot2)
library(readxl)
library(dplyr)
library(tidyverse)
library(viridis)
library(gridExtra)

# Read and reformat table of odds ratios for predictors of severe COVID outcomes

case_only_predictors <- read_excel('~/Documents/paper/Results\ tables/predictors_of_severe_covid.xlsx', sheet = 'Sheet3')
case_only_predictors <- case_only_predictors %>% mutate(comparison = ...1) %>% select(-...1) %>% 
                        filter(!grepl('^Eth', comparison)) %>% 
                        mutate(OR = as.numeric(OR)) %>% 
                        mutate(lower = as.numeric(lower)) %>% 
                        mutate(upper = as.numeric(upper))

case_only_predictors$Outcome = relevel(as.factor(case_only_predictors$Outcome), ref = 'Hospitalisation')
case_only_predictors_hosp <- case_only_predictors %>% filter(Outcome == 'Hospitalisation')
case_only_predictors_death <- case_only_predictors %>% filter(Outcome == 'Death')

# Forest plot for predictors of hospitalisation

p = ggplot(case_only_predictors_hosp, aes(x=factor(comparison), y = OR)) + geom_point() + 
  geom_errorbar(aes(x= factor(comparison), ymin = lower, ymax = upper, width = 0.2)) +
  coord_flip() + 
  ylab('Odds Ratio') + 
  xlab(NULL) + 
  theme_bw() +
  scale_y_continuous(trans = 'log10') +  
  geom_hline(aes(yintercept = 1), linetype = 'dashed') +
  #scale_color_manual(values = c('#5ec962', '#404788FF')) +
  theme(legend.position = 'bottom')


# Forest plot for predictors of death

q = ggplot(case_only_predictors_death, aes(x=factor(comparison), y = OR)) + geom_point() + 
  geom_errorbar(aes(x= factor(comparison), ymin = lower, ymax = upper, width = 0.2)) +
  coord_flip() + 
  ylab('Odds Ratio') + 
  xlab(NULL) + 
  theme_bw() +
  scale_y_continuous(trans = 'log10') +  
  geom_hline(aes(yintercept = 1), linetype = 'dashed') +
  #scale_color_manual(values = c('#5ec962', '#404788FF')) +
  theme(legend.position = 'bottom')
grid.arrange(p,q, ncol = 2)


# Forest plot for predictors of hospitalisation and death

ggplot(case_only_predictors, aes(x=factor(comparison), y = OR)) + geom_point() + 
  geom_errorbar(aes(x= factor(comparison), ymin = lower, ymax = upper, width = 0.2)) +
  coord_flip() + 
  ylab('Odds Ratio') + 
  xlab(NULL) + 
  theme_bw() +
  scale_y_continuous(trans = 'log10') +  
  geom_hline(aes(yintercept = 1), linetype = 'dashed') +
  #scale_color_manual(values = c('#5ec962', '#404788FF')) +
  theme(legend.position = 'bottom') +
  facet_wrap(~Outcome, scales = "free_x")
