# Databricks notebook source
from pyspark.sql import functions as F

%run '/Workspace/Repos/catriona.harrison@manchester.ac.uk/ccu068/functions/case_filtering_functions.py' 
dbutils.widgets.dropdown('window', 'pre_vaccination', ['pre_vaccination', 'post_vaccination'])


# COMMAND ----------

inclusion_codes = spark.table('hive_metastore.dsa_391419_j3w9t_collab.ccu068_code_dict')
                                                     
snomed_inclusion_codes = inclusion_codes.filter(F.col('code_type') == 'snom').select('code')                             
icd10_inclusion_codes = inclusion_codes.filter(F.col('code_type') == 'diag').select('code')    
opcs4_inclusion_codes = inclusion_codes.filter(F.col('code_type') == 'oper').select('code')    

# Read in SNOMED, ICD10 and OPCS4 inclusion and exclusion codes 

snomed_exclusion_codes = spark.table('hive_metastore.dsa_391419_j3w9t_collab.chd_exclusion_snomed').select('snomed_ct_concept_id').withColumnRenamed('snomed_ct_concept_id','code').withColumn('code_type', F.lit('snom'))
icd10_exclusion_codes = spark.table('hive_metastore.dsa_391419_j3w9t_collab.icd_confounder_codes').select('icd_10').withColumnRenamed('icd_10','code').withColumn('code_type', F.lit('diag'))
opcs4_exclusion_codes = spark.table('hive_metastore.dsa_391419_j3w9t_collab.opcs_confounder_codes').select('opcs_4_code').withColumnRenamed('opcs_4_code','code').withColumn('code_type', F.lit('oper'))
exclusion_codes = snomed_exclusion_codes.unionByName(icd10_exclusion_codes).unionByName(opcs4_exclusion_codes)

# Make dataframe for codes with an upper age threshold

snomed_age_threshold_codes = spark.createDataFrame(['60234000', '194983005', '194984004', '703323003', '60573004', '194987006', '427515002', '250976004', '276790000'], 'string').toDF('code').withColumn('code_type', F.lit('snom'))
icd10_age_threshold_codes = spark.createDataFrame(['I35', 'I350','I351', 'I352'], 'string').toDF('code').withColumn('code_type', F.lit('diag'))
opcs4_age_threshold_codes = spark.createDataFrame(['K312','K352','K261','K262','K263','K264','K265','K268','K269','K302'], 'string').toDF('code').withColumn('code_type', F.lit('oper'))
age_threshold_codes = snomed_age_threshold_codes.unionByName(icd10_age_threshold_codes).unionByName(opcs4_age_threshold_codes)


# COMMAND ----------

# Get chosen window (pre- or post- vaccination) from widget

window = dbutils.widgets.get('window')
window_dict = {'pre_vaccination': '03-01-2020', 'post_vaccination': '03-01-2021'}
window_start = window_dict[window]

# COMMAND ----------

# Read in death register, keeping entries with a date of death before window
death_register = spark.table('hive_metastore.dars_nic_391419_j3w9t.deaths_dars_nic_391419_j3w9t')

death_before_study = (
    death_register
        .withColumn('reg_date_of_death', F.to_date(F.regexp_replace(F.col('reg_date_of_death'), '([0-9]{4})([0-9]{2})([0-9]{2})', '$2-$3-$1'), format = 'MM-dd-yyyy'))
        .filter(F.col('REG_DATE_OF_DEATH') < F.to_date(F.lit(window_start), format = 'MM-dd-yyyy'))
        .select('DEC_CONF_NHS_NUMBER_CLEAN_DEID', 'reg_date_of_death')
        .withColumnRenamed('Dec_conf_NHS_number_clean_deid', 'NHS_number')
)

# COMMAND ----------

# Read in GDPPR data and select relevant columns, removing entries with a date of death before study window

gdppr_data = spark.table('hive_metastore.dars_nic_391419_j3w9t.gdppr_dars_nic_391419_j3w9t')
gdppr_data = (gdppr_data
        .filter(F.col('LSOA').startswith('E'))
        .select(F.col('NHS_NUMBER_DEID').alias('NHS_number'), F.col('CODE').alias('code'), F.col('RECORD_DATE'), F.col('YEAR_MONTH_OF_BIRTH').alias('DOB'), F.col('PRACTICE').alias('gp_practice'), F.col('ETHNIC').alias('ethnicity'), F.col('SEX'))
        .withColumn('RECORD_DATE', F.to_date(F.regexp_replace(F.col('RECORD_DATE'), '([0-9]{4})-([0-9]{2})-([0-9]{2})', '$2-$3-$1'), format = 'MM-dd-yyyy'))
        .withColumn('DOB',  F.to_date(F.regexp_replace(F.col('DOB'), '([0-9]{4})-([0-9]{2})', '$2-01-$1'), format = 'MM-dd-yyyy'))
        .withColumn('code_type', F.lit('snom'))
        .withColumn('dataset', F.lit('gdppr'))
        .filter(F.col('code') != '-')
        .join(death_before_study, on = 'NHS_number', how = 'anti')
)

# COMMAND ----------

# Read in hospital episode data, removing entries with a date of death before study window

diagnosis_cols = [F.col(f'diag_3_0{i}') for i in range(1,6)] + [F.col(f'diag_4_0{i}') for i in range(1,6)]
opertn_cols = [F.col(f'opertn_4_0{i}') for i in range(1,2)] 
diag_oper_cols = diagnosis_cols + opertn_cols

gdppr_NHS_numbers = gdppr_data.select('NHS_number').distinct()

hes_apc = spark.table('dars_nic_391419_j3w9t_collab._hes_apc_all_years')
hes_apc = (
    hes_apc
        .select(F.col('PERSON_ID_DEID').alias('NHS_number'), F.col('EPISTART').alias('record_date'), F.col('SEX'), F.col('MYDOB').alias('DOB'), F.col('gpprac').alias('gp_practice'), F.col('ethnos').alias('ethnicity'), *diagnosis_cols, *opertn_cols)
        .withColumn('RECORD_DATE', F.to_date(F.regexp_replace(F.col('RECORD_DATE'), '([0-9]{4})-([0-9]{2})-([0-9]{2})', '$2-$3-$1'), format = 'MM-dd-yyyy'))
        .withColumn('DOB', F.to_date(F.regexp_replace(F.col('DOB'), '([0-9]{2})([0-9]{4})', '$1-01-$2'), format = 'MM-dd-yyyy'))
        .withColumn('dataset', F.lit('hes_apc'))
        .join(gdppr_NHS_numbers, on = 'NHS_number', how = 'inner')
)

gdppr_dobs = gdppr_data.select(F.col('NHS_number'), F.col('DOB')).distinct()

hes_op = spark.table('dars_nic_391419_j3w9t_collab._hes_op_all_years')
hes_op = (
    hes_op
        .drop(F.col('DOB'))
        .select(F.col('PERSON_ID_DEID').alias('NHS_number'), F.col('APPTDATE').alias('record_date'), F.col('SEX'), F.col('gpprac').alias('gp_practice'), F.col('ethnos').alias('ethnicity'), *diagnosis_cols, *opertn_cols)
        .withColumn('RECORD_DATE', F.to_date(F.regexp_replace(F.col('RECORD_DATE'), '([0-9]{4})-([0-9]{2})-([0-9]{2})', '$2-$3-$1'), format = 'MM-dd-yyyy'))
        .withColumn('dataset', F.lit('hes_op'))
        .join(gdppr_dobs, on = 'NHS_number', how = 'inner')
)


# COMMAND ----------

# Reformat hes data so all diagnosis codes in a single column

hes_apc_stacked = stack_diagnosis_columns(hes_apc, diag_oper_cols).filter(F.col('code') != '-')
hes_op_stacked = stack_diagnosis_columns(hes_op, diag_oper_cols).filter(F.col('code') != '-')


# COMMAND ----------

# Filter datasets to find cases and controls

# Find cases from each dataset with a CHD indicating code
# 1. Combine records from all three datasets
# 2. Join with inclusion codes on code and code type

combined_records = gdppr_data.unionByName(hes_op_stacked).unionByName(hes_apc_stacked) 
pre_filtered_CHD_records = include_patient_records(combined_records, inclusion_codes)


# COMMAND ----------

pre_filtered_CHD_records.write.mode('overwrite').saveAsTable(f'dsa_391419_j3w9t_collab.temp_CHD_records_ccu068_{window}')
pre_filtered = spark.table(f'dsa_391419_j3w9t_collab.temp_CHD_records_ccu068_{window}').distinct()

# COMMAND ----------

# Identify controls by removing IDs of potential cases 

gdppr_controls = exclude_patient_records(gdppr_data, pre_filtered.select('NHS_number').distinct()).distinct()
hes_apc_controls = exclude_patient_records(hes_apc_stacked, pre_filtered.select('NHS_number').distinct()).distinct()
hes_op_controls = exclude_patient_records(hes_op_stacked, pre_filtered.select('NHS_number').distinct()).distinct()

# Apply further filtering steps to pre-filtered CHD records

CHD_records = combine_filters(pre_filtered, age_threshold_codes, exclusion_codes, inclusion_codes).distinct()

# COMMAND ----------

# Save overall tables of cases and controls

gdppr_controls.write.mode('overwrite').saveAsTable(f'dsa_391419_j3w9t_collab.gdppr_controls_ccu068_{window}')
hes_apc_controls.write.mode('overwrite').saveAsTable(f'dsa_391419_j3w9t_collab.hes_apc_controls_ccu068_{window}')
hes_op_controls.write.mode('overwrite').saveAsTable(f'dsa_391419_j3w9t_collab.hes_op_controls_ccu068_{window}')

CHD_records.write.mode('overwrite').saveAsTable(f'dsa_391419_j3w9t_collab.ccu068_cases_chd_records_{window}')

# COMMAND ----------

# Join CHD case NHS numbers to all associated records

cases_all_records = (
    spark.table(f'dsa_391419_j3w9t_collab.ccu068_cases_chd_records_{window}')
    .select('NHS_number')
    .join(pre_filtered, on = 'NHS_number', how = 'inner')
    .drop('dataset')
)
cases_all_records.write.mode('overwrite').saveAsTable(f'dsa_391419_j3w9t_collab.ccu068_cases_all_records_{window}')

# COMMAND ----------

# Combine controls identified from each dataset

CHD_controls_final = (
    hes_apc_controls
        .unionByName(hes_op_controls)
        .unionByName(gdppr_controls)
        .distinct()
)
CHD_controls_final.write.mode('overwrite').saveAsTable(f'dsa_391419_j3w9t_collab.ccu068_controls_all_records_{window}')