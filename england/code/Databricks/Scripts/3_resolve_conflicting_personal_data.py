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

# Validate personal information
# 1. Convert invalid ethnicities to 'Z' (for unknown)
# 2. Convert invalid GP practices to 'Z' (for unknown)
# 3. Remove patients with an invalid date of birth


CHD_cases =  spark.table(f'dsa_391419_j3w9t_collab.ccu068_cases_covid_records_unmatched_{window}').select('NHS_number').distinct().join(spark.table(f'dsa_391419_j3w9t_collab.ccu068_cases_all_records_{window}'), on = 'NHS_number', how = 'inner')
CHD_controls = spark.table(f'dsa_391419_j3w9t_collab.ccu068_controls_covid_records_unmatched_{window}').select('NHS_number').distinct().join(spark.table(f'dsa_391419_j3w9t_collab.ccu068_controls_all_records_{window}').select('NHS_number', 'gp_practice', 'ethnicity', 'sex', 'DOB', 'record_date'), on = 'NHS_number', how = 'inner')

valid_ethnicities = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'P', 'R', 'S', 'Z'] 
unknown_practice_codes = ['&', 'V81997', 'V81998', 'V81999']

CHD_cases = (
    CHD_cases
        .withColumn('ethnicity', F.when(F.col('ethnicity').isin(valid_ethnicities), F.col('ethnicity')).otherwise('Z'))
        .withColumn('gp_practice', F.when(F.col('gp_practice').isin(unknown_practice_codes), 'Z').otherwise(F.col('gp_practice')))
        .filter((F.col('DOB') > F.to_date(F.lit('01-01-1911'), format = 'MM-dd-yyyy')) & (F.col('DOB') < F.to_date(F.lit(window_end_date), format = 'MM-dd-yyyy')))
)

CHD_controls = (
    CHD_controls
        .withColumn('ethnicity', F.when(F.col('ethnicity').isin(valid_ethnicities), F.col('ethnicity')).otherwise('Z'))
        .withColumn('gp_practice', F.when(F.col('gp_practice').isin(unknown_practice_codes), 'Z').otherwise(F.col('gp_practice')))
        .filter((F.col('DOB') > F.to_date(F.lit('01-01-1911'), format = 'MM-dd-yyyy')) & (F.col('DOB') < F.to_date(F.lit(window_end_date), format = 'MM-dd-yyyy')))
)

# COMMAND ----------

# Resolve conflicting entries (ethnicity, gp practice, date of birth, sex)
# 1. Remove 'unknown' values
# 2. Choose the most commonly reported value for each NHS number
# 3. If tied, choose the most recent entry from the tied values
# 4. If there are no remaining entries for an NHS number, return 'Z' for unknown

final_CHD_cases = resolve_conflicting_data(CHD_cases).select('NHS_number', 'sex', 'ethnicity', 'DOB', 'gp_practice').distinct()
final_CHD_controls = resolve_conflicting_data(CHD_controls).select('NHS_number', 'sex', 'ethnicity', 'DOB', 'gp_practice').distinct()

# COMMAND ----------

# Save cases and controls with personal information resolved

final_CHD_cases.write.mode('overwrite').saveAsTable(f'dsa_391419_j3w9t_collab.ccu068_cases_resolved_{window}')


# COMMAND ----------

final_CHD_controls.write.mode('overwrite').saveAsTable(f'dsa_391419_j3w9t_collab.ccu068_controls_resolved_{window}')

# COMMAND ----------

display(spark.table('dsa_391419_j3w9t_collab.ccu068_cases_resolved_post_vaccination'))