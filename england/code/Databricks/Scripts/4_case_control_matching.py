# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from collections import Counter

%run '/Workspace/Repos/catriona.harrison@manchester.ac.uk/ccu068/functions/case_control_matching_functions.py'
dbutils.widgets.dropdown('window', 'pre_vaccination', ['pre_vaccination', 'post_vaccination'])

# COMMAND ----------

# Get chosen window (pre- or post- vaccination) end date from widget

window = dbutils.widgets.get('window')
window_end_dict = {'pre_vaccination': '12-08-2020', 'post_vaccination': '04-01-2022'}
window_end_date = window_end_dict[window]

# COMMAND ----------

# Read in CHD cases and controls

CHD_cases =  spark.table(f'dsa_391419_j3w9t_collab.ccu068_cases_resolved_{window}').distinct()
CHD_controls = spark.table(f'dsa_391419_j3w9t_collab.ccu068_controls_resolved_{window}').distinct()


# COMMAND ----------

# Match cases and controls with the same ethnicity, gp practice, sex, and year of birth with a ratio of four controls to one case
# 1. Give each case/control a matching ID by concatenating ethnicity, gp practice, sex, and year of birth columns
# 2. Remove cases with fewer than 4 exact matches in controls
# 3. Remove surplus controls

cases = (
    CHD_cases
        .withColumn('matching_ID', F.concat(F.col('gp_practice'), F.year(F.col('DOB')), F.col('ethnicity'), F.col('sex')))
)
controls = (
    CHD_controls
        .withColumn('matching_ID', F.concat(F.col('gp_practice'), F.year(F.col('DOB')), F.col('ethnicity'), F.col('sex')))
)
ratio = 4
matched_cases, matched_controls, unmatched_cases, unmatched_controls = match_cases_to_controls(cases, controls, ratio)

# COMMAND ----------

# Match remaining unmatched cases using a 4-year date window 
# 1. Convert date part of matching ID to a window from 1911-2023 --> 112 years or 28 4-year windows
# 2. Repeat matching process

unmatched_cases_window = unmatched_cases.withColumn('matching_DOB', F.floor(((F.year(F.col('DOB'))-1911)/4)))
unmatched_controls_window = unmatched_controls.withColumn('matching_DOB', F.floor(((F.year(F.col('DOB'))-1911)/4)))

unmatched_cases_window = (
    unmatched_cases_window
    .withColumn('matching_ID', F.concat(F.col('gp_practice'), F.col('matching_DOB'), F.col('ethnicity'), F.col('sex')))
    .drop('matching_DOB')
)
unmatched_controls_window = (
    unmatched_controls_window
    .withColumn('matching_ID', F.concat(F.col('gp_practice'), F.col('matching_DOB'), F.col('ethnicity'), F.col('sex')))
    .drop('matching_DOB')
)

ratio = 4
window_matched_cases, window_matched_controls, unmatched_cases, unmatched_controls = match_cases_to_controls(unmatched_cases_window, unmatched_controls_window, ratio)

matched_cases = matched_cases.unionByName(window_matched_cases)
matched_controls = matched_controls.unionByName(window_matched_controls)

# COMMAND ----------

# Match remaining unmatched cases using a 4-year date window and by matching fewer controls

while ratio > 2:
    ratio -= 1

    unmatched_cases = unmatched_cases.withColumn('matching_DOB', F.floor(((F.year(F.col('DOB'))-1911)/4)))
    unmatched_controls  = unmatched_controls.withColumn('matching_DOB', F.floor(((F.year(F.col('DOB'))-1911)/4)))

    unmatched_cases = (
        unmatched_cases
        .withColumn('matching_ID', F.concat(F.col('gp_practice'), F.col('matching_DOB'), F.col('ethnicity'), F.col('sex')))
        .drop('matching_DOB')
    )
    unmatched_controls = (
        unmatched_controls
        .withColumn('matching_ID', F.concat(F.col('gp_practice'), F.col('matching_DOB'), F.col('ethnicity'), F.col('sex')))
        .drop('matching_DOB')
    )
    new_matched_cases, new_matched_controls, unmatched_cases, unmatched_controls = match_cases_to_controls(unmatched_cases, unmatched_controls, ratio)
    matched_cases = matched_cases.unionByName(new_matched_cases)
    matched_controls = matched_controls.unionByName(new_matched_controls)


# COMMAND ----------

# Save table of matched cases and controls

matched_cases.distinct().write.mode('overwrite').option('overwriteSchema', 'True').saveAsTable(f'dsa_391419_j3w9t_collab.ccu068_cases_matched_personal_info_{window}')


# COMMAND ----------

matched_controls.distinct().write.mode('overwrite').option('overwriteSchema', 'True').saveAsTable(f'dsa_391419_j3w9t_collab.ccu068_controls_matched_personal_info_{window}')

# COMMAND ----------

matched_cases = spark.table(f'dsa_391419_j3w9t_collab.ccu068_cases_matched_personal_info_{window}')
matched_controls = spark.table(f'dsa_391419_j3w9t_collab.ccu068_controls_matched_personal_info_{window}')
print(matched_cases.count())
print(matched_controls.count())