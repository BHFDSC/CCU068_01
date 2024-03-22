# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
%run '/Workspace/Repos/catriona.harrison@manchester.ac.uk/ccu068/functions/cohort_checking_functions.py'

# Get chosen window (pre-vaccination/post-vaccination) from widget
dbutils.widgets.dropdown('window', 'pre_vaccination', ['pre_vaccination', 'post_vaccination'])
window = dbutils.widgets.get('window')

# COMMAND ----------

# Read in table of matched cases and controls

CHD_cases = spark.table(f'dsa_391419_j3w9t_collab.ccu068_cases_matched_personal_info_{window}')
CHD_controls = spark.table(f'dsa_391419_j3w9t_collab.ccu068_controls_matched_personal_info_{window}')


# COMMAND ----------

# Count the total number of cases and controls

total_cases = CHD_cases.select('NHS_number').distinct().count()
total_controls = CHD_controls.select('NHS_number').distinct().count()
print(total_controls, total_cases)

# COMMAND ----------

# Compare the percentage born in each year between cases and controls

display(CHD_controls.agg(F.mean(2021-F.year('DOB'))))

birth_year_cases =  CHD_cases.withColumn('DOB', F.year(F.col('DOB')))
birth_year_controls = CHD_controls.select('NHS_number', 'DOB').withColumn('DOB', F.year(F.col('DOB')))

birth_year_cases = get_percentage_for_each_group(birth_year_cases, 'cases', ['DOB'], total_cases)
birth_year_controls = get_percentage_for_each_group(birth_year_controls, 'controls', ['DOB'], total_controls)

birth_year_cases_controls = birth_year_cases.join(birth_year_controls, on = 'DOB', how = 'inner')
birth_year_cases_controls = birth_year_cases_controls.orderBy('DOB').toPandas()
birth_year_cases_controls.plot.line(x = 'DOB')
display(plt.show())

# COMMAND ----------

# Compare the percentage of each sex between cases and controls


sex_cases = get_percentage_for_each_group(CHD_cases, 'cases',['sex'], total_cases)
sex_controls = get_percentage_for_each_group(CHD_controls, 'controls', ['sex'], total_controls)

sex_cases_controls = sex_cases.join(sex_controls, on = 'sex', how = 'inner')

sex_cases_controls_percent_change = (
    sex_cases_controls
        .withColumn('difference_in_percentage', F.round(F.col('percent_of_cases') - F.col('percent_of_controls'), 2))
        .drop('case_control')
        .orderBy(F.col('percent_of_cases').desc())
)

display(sex_cases_controls_percent_change)

# COMMAND ----------

# Compare the percentage of each ethnicity between cases and controls

CHD_cases = CHD_cases.withColumn('ethnicity', F.when(F.col('ethnicity').isin(['A','B', 'C']), 'White').when(F.col('ethnicity').isin(['H', 'K', 'J', 'L']), 'Asian').when(F.col('ethnicity').isin(['M', 'N', 'P']), 'Black').when(F.col('ethnicity').isin(['E','D', 'F', 'G']), 'Mixed').when(F.col('ethnicity').isin(['S', 'R']), 'Other').otherwise('Unknown'))
                     

CHD_controls = CHD_controls.withColumn('ethnicity', F.when(F.col('ethnicity').isin(['A','B', 'C']), 'White').when(F.col('ethnicity').isin(['H', 'K', 'J', 'L']), 'Asian').when(F.col('ethnicity').isin(['M', 'N', 'P']), 'Black').when(F.col('ethnicity').isin(['E','D', 'F', 'G']), 'Mixed').when(F.col('ethnicity').isin(['S', 'R']), 'Other').otherwise('Unknown'))             
                    


ethnicity_cases = get_percentage_for_each_group(CHD_cases, 'cases',['ethnicity'], total_cases)
ethnicity_controls = get_percentage_for_each_group(CHD_controls, 'controls', ['ethnicity'], total_controls)

ethnicity_cases_controls = ethnicity_cases.join(ethnicity_controls, on = 'ethnicity', how = 'inner' )

ethnicity_cases_controls_percent_change = (
    ethnicity_cases_controls
        .withColumn('difference_in_percentage', F.round(F.col('percent_of_cases') - F.col('percent_of_controls'), 2))
        .drop('case_control')
        .orderBy(F.col('percent_of_cases').desc())
)

display(ethnicity_cases_controls_percent_change)

# COMMAND ----------

# For the post-vaccination window, display the percentage with each vaccination status

if window == 'post_vaccination':
    vaccination_cases = get_percentage_for_each_group(CHD_cases_vaccination, 'cases',['vaccinated_pre_infection'], total_cases)
    vaccination_controls = get_percentage_for_each_group(CHD_controls_vaccination, 'controls', ['vaccinated_pre_infection'], total_controls)

    vaccination_cases_controls = vaccination_cases.join(vaccination_controls, on = 'vaccinated_pre_infection', how = 'inner' )

    display(vaccination_cases_controls)