# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import Window
dbutils.widgets.dropdown('window', 'pre_vaccination', ['pre_vaccination', 'post_vaccination'])

# COMMAND ----------

# Read in CHD cases and controls from chosen window

window = dbutils.widgets.get('window')
window_date_dict = {'pre_vaccination': ('03-01-2020','12-08-2020') , 'post_vaccination': ('03-01-2021', '04-01-2022')} 
window_start_date = window_date_dict[window][0]
window_end_date = window_date_dict[window][1]


CHD_cases =  spark.table(f'dsa_391419_j3w9t_collab.ccu068_cases_all_records_{window}').select('NHS_number').distinct()

CHD_controls = spark.table(f'dsa_391419_j3w9t_collab.ccu068_controls_all_records_{window}').select('NHS_number').distinct()



# COMMAND ----------

# MAGIC %md
# MAGIC ## COVID infections
# MAGIC #### Identify date of first COVID infection for each NHS number

# COMMAND ----------

# 1. Read in positive COVID tests from SGSS
# 2. Read in COVID hospitalisations from CHESS
# 3. Read in hospital episode statistics for window and keep codes for COVID diagnosis
# 4. Read in gdppr for window and keep codes for COVID diagnosis
# 5. Combine COVID diagnoses from the four datasets and keep the earliest for each NHS number

#1
covid_PCR_tests = spark.table('dars_nic_391419_j3w9t.sgss_dars_nic_391419_j3w9t')

#2
covid_hospitalisations_chess = spark.table('dars_nic_391419_j3w9t.chess_dars_nic_391419_j3w9t')

# COMMAND ----------

# 3. COVID diagnoses in HES

# Combine hospital episode statistics from 2019 to 2022

hes_19_to_22 = spark.table('dars_nic_391419_j3w9t.hes_apc_1920_dars_nic_391419_j3w9t').select('PERSON_ID_DEID', 'EPISTART', 'DISDEST', 'DISMETH', 'DIAG_4_01', 'SUSRECID')

for i in range(20,21):
    hes_19_to_22 = hes_19_to_22.unionByName(spark.table(f'dars_nic_391419_j3w9t.hes_apc_{i}{i+1}_dars_nic_391419_j3w9t').select('PERSON_ID_DEID', 'EPISTART', 'DISDEST', 'DISMETH', 'DIAG_4_01', 'SUSRECID'))

# Keep those with COVID as primary diagnosis

hes_covid_records = (
    hes_19_to_22
        .filter(F.col('diag_4_01').isin('U071', 'U072'))
        .withColumn('FinalOutcome', F.when((F.col('disdest') == '79')|(F.col('dismeth') == '4'), 'Death').otherwise('Discharged'))        
        .withColumnRenamed('PERSON_ID_DEID', 'NHS_number')
        .withColumnRenamed('epistart', 'infection_date')
)

# COMMAND ----------

# 4. COVID diagnoses in gdppr

gdppr = spark.table('dars_nic_391419_j3w9t.gdppr_dars_nic_391419_j3w9t')
gdppr_covid_records = (
    gdppr
        .filter(F.col('code').isin(['840539006', '186747009']))
        .select('NHS_NUMBER_DEID', 'record_date')
        .withColumnRenamed('NHS_NUMBER_DEID', 'NHS_number')
        .withColumnRenamed('record_date', 'infection_date')
)

# COMMAND ----------

# 5. Combine COVID diagnoses from each dataset

covid_infections = (
    covid_PCR_tests
        .select(F.col('PERSON_ID_DEID').alias('NHS_number'), F.col('specimen_date').alias('infection_date'))
        .withColumn('dataset', F.lit('SGSS'))
        .unionByName(
            covid_hospitalisations_chess
                .select(F.col('PERSON_ID_DEID').alias('NHS_number'), F.col('HospitalAdmissionDate').alias('infection_date'))
                .withColumn('dataset', F.lit('CHESS')))
        .unionByName(
            hes_covid_records
                .select('NHS_number', 'infection_date')
                .withColumn('dataset', F.lit('HES')))
        .unionByName(
            gdppr_covid_records
                .select('NHS_number', 'infection_date')
                .withColumn('dataset', F.lit('gdppr')))
)

# COMMAND ----------

# Keep earliest record for each NHS number

partition_window = Window.partitionBy(F.col('NHS_number')).orderBy(F.col('infection_date').asc())

first_covid_infections = (
    covid_infections
        .withColumn('row_in_partition', F.row_number().over(partition_window))
        .filter(F.col('row_in_partition') == 1)
        .select('infection_date', 'NHS_number', 'dataset')
        .filter((F.col('infection_date') > F.to_date(F.lit(window_start_date), format = 'MM-dd-yyyy')) & (F.col('infection_date') < F.to_date(F.lit(window_end_date), format = 'MM-dd-yyyy')))
)

# Label cases and controls with date of first positive covid test 

CHD_cases_covid = CHD_cases.join(first_covid_infections, on = 'NHS_number', how = 'inner')

CHD_controls_covid = CHD_controls.join(first_covid_infections, on = 'NHS_number', how = 'inner')


# COMMAND ----------

# MAGIC %md
# MAGIC ## COVID hospitalisations

# COMMAND ----------

# 1. Read in COVID hospitalisation records from CHESS
# 2. Add records from HES with COVID code as primary diagnosis
# 3. Join hospitalisation data to COVID infection table
# 4. Remove hospitalisations not within 20 days of first positive COVID test

hospitalisation_partition = Window.partitionBy('NHS_number').orderBy(F.col('hospitalisation_date').asc())

covid_hospitalisations = (
    covid_hospitalisations_chess
        .withColumn('hospitalisation_date', F.col('HospitalAdmissionDate'))
        .withColumn('SUSRECID', F.lit('null'))
        .select(F.col('PERSON_ID_DEID').alias('NHS_number'), 'hospitalisation_date', 'FinalOutcome', F.col('AdmittedToICU').alias('admitted_to_icu'), 'SUSRECID')
        .unionByName(
            hes_covid_records
                .select('NHS_number', F.col('infection_date').alias('hospitalisation_date'), 'FinalOutcome', 'SUSRECID')
                .withColumn('admitted_to_icu', F.lit('No')))
)

first_covid_hospitalisations = (
    covid_hospitalisations
        .withColumn('row_in_partition', F.row_number().over(hospitalisation_partition))
        .filter(F.col('row_in_partition') == 1)
        .drop('row_in_partition')
)

# COMMAND ----------

CHD_cases_covid_hospitalisations = CHD_cases_covid.join(first_covid_hospitalisations, on = ['NHS_number'], how = 'left')
CHD_controls_covid_hospitalisations = CHD_controls_covid.join(first_covid_hospitalisations, on = ['NHS_number'], how = 'left')

CHD_cases_covid_hospitalisations = (
    CHD_cases_covid_hospitalisations
        .withColumn('hospitalisation_date', F.when(F.datediff(F.col('hospitalisation_date'), F.col('infection_date')) <= 20, F.col('hospitalisation_date')).otherwise(F.lit(None)))
        .withColumn('FinalOutcome', F.when(F.col('hospitalisation_date').isNull(), F.lit(None)).otherwise(F.col('FinalOutcome')))
        .withColumn('admitted_to_icu', F.when(F.col('hospitalisation_date').isNull(), F.lit(None)).otherwise(F.col('admitted_to_icu')))
)
CHD_controls_covid_hospitalisations = (
    CHD_controls_covid_hospitalisations
        .withColumn('hospitalisation_date', F.when(F.datediff(F.col('hospitalisation_date'), F.col('infection_date')) <= 20, F.col('hospitalisation_date')).otherwise(F.lit(None)))
        .withColumn('FinalOutcome', F.when(F.col('hospitalisation_date').isNull(), F.lit(None)).otherwise(F.col('FinalOutcome')))
        .withColumn('admitted_to_icu', F.when(F.col('hospitalisation_date').isNull(), F.lit(None)).otherwise(F.col('admitted_to_icu')))
)


# COMMAND ----------

# Find whether admitted to ICU in hes_cc
# 1. Combine hes_cc for years 2019 - 2022
# 2. Join by NHS_number and SUSRECID to covid outcomes table 
# 4. Add 'Yes' or 'No' for admitted_to_ICU

hes_cc_19_to_22 = spark.table('dars_nic_391419_j3w9t.hes_cc_1920_dars_nic_391419_j3w9t').select('PERSON_ID_DEID', 'ADMIDATE', 'SUSRECID')

for i in range(20,21):
    hes_cc_19_to_22 = (
        hes_cc_19_to_22
        .unionByName(spark.table(f'dars_nic_391419_j3w9t.hes_cc_{i}{i+1}_dars_nic_391419_j3w9t')
        .select('PERSON_ID_DEID', 'SUSRECID', 'ADMIDATE'))
        .withColumnRenamed('PERSON_ID_DEID', 'NHS_number')
        .withColumn('in_hes_cc', F.lit('Yes'))
    )

hes_cc_19_to_22 = (
    hes_cc_19_to_22
        .drop('ADMIDATE')
        .distinct()
)

CHD_cases_covid_icu = (
    CHD_cases_covid_hospitalisations
        .join(hes_cc_19_to_22, on = ['NHS_number', 'SUSRECID'], how = 'left')
        .drop('SUSRECID')
        .withColumn('admitted_to_icu', F.when(F.col('in_hes_cc') == 'Yes', 'Yes').otherwise(F.col('admitted_to_icu')))
)

CHD_controls_covid_icu = (
    CHD_controls_covid_hospitalisations
        .join(hes_cc_19_to_22, on = ['NHS_number', 'SUSRECID'], how = 'left')
        .drop('SUSRECID')
        .withColumn('admitted_to_icu', F.when(F.col('in_hes_cc') == 'Yes', 'Yes').otherwise(F.col('admitted_to_icu')))
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## COVID deaths

# COMMAND ----------

# 1. Read in death records
# 2. Keep those with COVID ICD-10 code in cause of death codes
# 3. Join deaths to COVID records
# 4. Remove if not within 30 days of infection
# 5. Change 'FinalOutcome' to 'Death' if death column not null


death_register = spark.table('hive_metastore.dars_nic_391419_j3w9t.deaths_dars_nic_391419_j3w9t')
cause_of_death_codes = ['S_UNDERLYING_COD_ICD10'] + [f'S_COD_CODE_{i}' for i in range(1,15)]
covid_deaths = (
    death_register
        .withColumnRenamed('DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'NHS_number')
        .withColumn('date_of_death', F.to_date(F.regexp_replace(F.col('reg_date_of_death'), '([0-9]{4})([0-9]{2})([0-9]{2})', '$2-$3-$1'), format = 'MM-dd-yyyy'))
        .unpivot(['NHS_number', 'date_of_death'], cause_of_death_codes, 'column_number', 'cause_of_death_ICD10')
        .filter(F.col('cause_of_death_ICD10').isin(['U071', 'U072']))
        .select('NHS_number', 'date_of_death')
        .dropDuplicates(['NHS_number'])
)

# COMMAND ----------

CHD_cases_covid_outcomes = (
    CHD_cases_covid_icu
        .join(covid_deaths, on = 'NHS_number', how = 'left')
        .withColumn('date_of_death', F.when((F.datediff(F.col('date_of_death'), F.col('infection_date')) <= 30) & (F.datediff(F.col('date_of_death'), F.col('infection_date')) > 0), F.col('date_of_death')).otherwise(F.lit(None)))
        .withColumn('FinalOutcome', F.when(F.col('date_of_death').isNotNull(), F.lit('Death')).otherwise(F.col('FinalOutcome')))
)

CHD_controls_covid_outcomes = (
    CHD_controls_covid_icu
        .join(covid_deaths, on = 'NHS_number', how = 'left')
        .withColumn('date_of_death', F.when((F.datediff(F.col('date_of_death'), F.col('infection_date')) <= 30) & (F.datediff(F.col('date_of_death'), F.col('infection_date')) > 0), F.col('date_of_death')).otherwise(F.lit(None)))
        .withColumn('FinalOutcome', F.when(F.col('date_of_death').isNotNull(), F.lit('Death')).otherwise(F.col('FinalOutcome')))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## COVID vaccinations

# COMMAND ----------

# Read in vaccination records and keep only records of first vaccinations - if post-vaccination window

partition_window_vaccine = Window.partitionBy(F.col('NHS_number')).orderBy(F.col('first_vaccination_date').asc())

if window == 'post_vaccination':

    gdppr_first_vaccine_records = (
    gdppr
        .filter(F.col('code') == '1324681000000101')
        .select('NHS_NUMBER_DEID', 'record_date')
        .withColumnRenamed('NHS_NUMBER_DEID', 'NHS_number')
        .withColumnRenamed('record_date', 'first_vaccination_date')
    )

    vaccination_records = spark.table('dars_nic_391419_j3w9t.vaccine_status_dars_nic_391419_j3w9t').select('person_id_deid', 'recorded_date').withColumnRenamed('person_id_deid', 'NHS_number')

    first_vaccinations = (
        vaccination_records
            .withColumn('first_vaccination_date', F.to_date(F.regexp_replace(F.col('recorded_date'), '([0-9]{4})([0-9]{2})([0-9]{2})', '$2-$3-$1'), format = 'MM-dd-yyyy'))
            .drop('recorded_date')
            .unionByName(gdppr_first_vaccine_records)
            .withColumn('row_in_partition', F.row_number().over(partition_window_vaccine))
            .filter(F.col('row_in_partition') == 1)
            .select('first_vaccination_date', 'NHS_number')
            .filter(F.col('first_vaccination_date') < F.to_date(F.lit(window_end_date), format = 'MM-dd-yyyy'))
    )


# COMMAND ----------

# Repeat for second record of vaccination - take the second entry in vaccination staus or gdppr second vaccine code, whichever earlier
partition_window_vaccine = Window.partitionBy(F.col('NHS_number')).orderBy(F.col('second_vaccination_date').asc())

if window == 'post_vaccination':

    gdppr_second_vaccine_records = (
    gdppr
        .filter(F.col('code') == '1324691000000104')
        .select('NHS_NUMBER_DEID', 'record_date')
        .withColumnRenamed('NHS_NUMBER_DEID', 'NHS_number')
        .withColumnRenamed('record_date', 'second_vaccination_date')
    )

    vaccination_records = spark.table('dars_nic_391419_j3w9t.vaccine_status_dars_nic_391419_j3w9t').select('person_id_deid', 'recorded_date').withColumnRenamed('person_id_deid', 'NHS_number')

    second_vaccinations = (
        vaccination_records
            .withColumn('second_vaccination_date', F.to_date(F.regexp_replace(F.col('recorded_date'), '([0-9]{4})([0-9]{2})([0-9]{2})', '$2-$3-$1'), format = 'MM-dd-yyyy'))
            .drop('recorded_date')
            .withColumn('row_in_partition', F.row_number().over(partition_window_vaccine))
            .filter(F.col('row_in_partition') == 2)
            .select('second_vaccination_date', 'NHS_number')
            .unionByName(gdppr_second_vaccine_records)
            .withColumn('row_in_partition', F.row_number().over(partition_window_vaccine))
            .filter(F.col('row_in_partition') == 1)
            .filter(F.col('second_vaccination_date') < F.to_date(F.lit(window_end_date), format = 'MM-dd-yyyy'))
    )

# COMMAND ----------

# Find most recent vaccination
partition_window_vaccine = Window.partitionBy(F.col('NHS_number')).orderBy(F.col('most_recent_vaccination_date').desc())

gdppr_vaccine_records = (
    gdppr
        .filter(F.col('code').isin(['1156257007','1324681000000101', '1324691000000104', '39330711000001103']))
        .select('NHS_NUMBER_DEID', 'record_date')
        .withColumnRenamed('NHS_NUMBER_DEID', 'NHS_number')
        .withColumnRenamed('record_date', 'most_recent_vaccination_date')
    )
    
vaccination_records = spark.table('dars_nic_391419_j3w9t.vaccine_status_dars_nic_391419_j3w9t').select('recorded_date', 'person_id_deid').withColumnRenamed('person_id_deid', 'NHS_number')

most_recent_vaccinations_cases = (
    vaccination_records
        .withColumn('most_recent_vaccination_date', F.to_date(F.regexp_replace(F.col('recorded_date'), '([0-9]{4})([0-9]{2})([0-9]{2})', '$2-$3-$1'), format = 'MM-dd-yyyy'))
        .drop('recorded_date')
        .unionByName(gdppr_vaccine_records)
        .join(CHD_cases_covid_outcomes.select('NHS_number', 'infection_date'), on = 'NHS_number', how = 'inner')
        .filter(F.col('most_recent_vaccination_date') < F.col('infection_date'))
        .withColumn('row_in_partition', F.row_number().over(partition_window_vaccine))
        .filter(F.col('row_in_partition') == 1)
        .select('most_recent_vaccination_date', 'NHS_number') 
)
most_recent_vaccinations_controls = (
    vaccination_records
        .withColumn('most_recent_vaccination_date', F.to_date(F.regexp_replace(F.col('recorded_date'), '([0-9]{4})([0-9]{2})([0-9]{2})', '$2-$3-$1'), format = 'MM-dd-yyyy'))
        .drop('recorded_date')
        .unionByName(gdppr_vaccine_records)
        .join(CHD_controls_covid_outcomes.select('NHS_number', 'infection_date'), on = 'NHS_number', how = 'inner')
        .filter(F.col('most_recent_vaccination_date') < F.col('infection_date'))
        .withColumn('row_in_partition', F.row_number().over(partition_window_vaccine))
        .filter(F.col('row_in_partition') == 1)
        .select('most_recent_vaccination_date', 'NHS_number') 
)


# COMMAND ----------

# Label cases and controls with date of vaccination (within window) - if post-vaccination window

if window == 'post_vaccination':
    CHD_cases_covid_outcomes = CHD_cases_covid_outcomes.join(first_vaccinations, on = 'NHS_number', how = 'inner')
    CHD_controls_covid_outcomes = CHD_controls_covid_outcomes.join(first_vaccinations, on = 'NHS_number', how = 'inner')
    CHD_cases_covid_outcomes = CHD_cases_covid_outcomes.join(second_vaccinations, on = 'NHS_number', how = 'left')
    CHD_controls_covid_outcomes = CHD_controls_covid_outcomes.join(second_vaccinations, on = 'NHS_number', how = 'left')
    CHD_cases_covid_outcomes = CHD_cases_covid_outcomes.join(most_recent_vaccinations_cases, on = 'NHS_number', how = 'left')
    CHD_controls_covid_outcomes = CHD_controls_covid_outcomes.join(most_recent_vaccinations_controls, on = 'NHS_number', how = 'left')

# COMMAND ----------

# Add a column for  vaccination (> 2 weeks) prior to positive covid test - if second window
 
if window == 'post_vaccination':

    CHD_cases_covid_outcomes = CHD_cases_covid_outcomes.filter(F.datediff(F.col('infection_date'), F.col('first_vaccination_date')) > 14)
    CHD_cases_covid_outcomes = CHD_cases_covid_outcomes.withColumn('vaccinated_pre_infection', F.when(F.datediff(F.col('infection_date'), F.col('second_vaccination_date')) > 14, 'Full').otherwise('Partial'))
    CHD_cases_covid_outcomes = CHD_cases_covid_outcomes.withColumn('days_since_vaccination', F.datediff(F.col('infection_date'), F.col('most_recent_vaccination_date')))
    
    CHD_controls_covid_outcomes = CHD_controls_covid_outcomes.filter(F.datediff(F.col('infection_date'), F.col('first_vaccination_date')) > 14)
    CHD_controls_covid_outcomes = CHD_controls_covid_outcomes.withColumn('vaccinated_pre_infection', F.when(F.datediff(F.col('infection_date'), F.col('second_vaccination_date')) > 14, 'Full').otherwise('Partial'))
    CHD_controls_covid_outcomes = CHD_controls_covid_outcomes.withColumn('days_since_vaccination', F.datediff(F.col('infection_date'), F.col('most_recent_vaccination_date')))
  
             
else:
    CHD_cases_covid_outcomes = CHD_cases_covid_outcomes.withColumn('vaccinated_pre_infection', F.lit(None))
    CHD_controls_covid_outcomes = CHD_controls_covid_outcomes.withColumn('vaccinated_pre_infection', F.lit(None))


# COMMAND ----------

# Finalise tables 

CHD_cases_covid_outcomes = (
    CHD_cases_covid_outcomes
        .withColumn('covid_infection', F.when(F.col('infection_date').isNotNull(), 'Yes').otherwise('No'))
        .withColumn('hospitalised_with_first_covid_infection', F.when(F.col('hospitalisation_date').isNotNull(), 'Yes').otherwise('No'))
        .withColumn('death_from_first_covid_infection', F.when(F.col('FinalOutcome') == 'Death', 'Yes').otherwise('No'))
        .select('NHS_number', 'covid_infection', 'infection_date', 'hospitalised_with_first_covid_infection', 'hospitalisation_date', 'death_from_first_covid_infection', 'date_of_death')
)

CHD_controls_covid_outcomes = (
    CHD_controls_covid_outcomes
        .withColumn('covid_infection', F.when(F.col('infection_date').isNotNull(), 'Yes').otherwise('No'))
        .withColumn('hospitalised_with_first_covid_infection', F.when(F.col('hospitalisation_date').isNotNull(), 'Yes').otherwise('No'))
        .withColumn('death_from_first_covid_infection', F.when(F.col('FinalOutcome') == 'Death', 'Yes').otherwise('No'))
        .select('NHS_number', 'covid_infection', 'infection_date', 'hospitalised_with_first_covid_infection', 'hospitalisation_date', 'death_from_first_covid_infection', 'date_of_death')
)


# COMMAND ----------

display(CHD_cases_covid_outcomes)

# COMMAND ----------


CHD_cases_covid_outcomes.write.mode('overwrite').option('overwriteSchema', 'True').saveAsTable(f'dsa_391419_j3w9t_collab.ccu068_cases_covid_records_unmatched_{window}')
CHD_controls_covid_outcomes.write.mode('overwrite').option('overwriteSchema', 'True').saveAsTable(f'dsa_391419_j3w9t_collab.ccu068_controls_covid_records_unmatched_{window}')
