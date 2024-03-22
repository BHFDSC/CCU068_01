# Databricks notebook source
from pyspark.sql import functions as F
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import Window

%run '/Workspace/Repos/catriona.harrison@manchester.ac.uk/ccu068/functions/cohort_checking_functions.py'
dbutils.widgets.dropdown('window', 'post_vaccination', ['pre_vaccination', 'post_vaccination'])

# COMMAND ----------

inclusion_codes = spark.table('hive_metastore.dsa_391419_j3w9t_collab.ccu068_code_dict')
                              
snomed_inclusion_codes = inclusion_codes.filter(F.col('code_type') == 'snom').select('code')                             
icd10_inclusion_codes = inclusion_codes.filter(F.col('code_type') == 'diag').select('code')    
opcs4_inclusion_codes = inclusion_codes.filter(F.col('code_type') == 'oper').select('code')    

# Read in SNOMED, ICD10 and OPCS4 inclusion and exclusion codes 

snomed_exclusion_codes = spark.table('hive_metastore.dsa_391419_j3w9t_collab.chd_exclusion_snomed').select('snomed_ct_concept_id').withColumnRenamed('snomed_ct_concept_id','code')

icd10_exclusion_codes = spark.table('hive_metastore.dsa_391419_j3w9t_collab.icd_confounder_codes').select('icd_10').withColumnRenamed('icd_10','code')

opcs4_exclusion_codes = spark.table('hive_metastore.dsa_391419_j3w9t_collab.opcs_confounder_codes').select('opcs_4_code').withColumnRenamed('opcs_4_code','code')

# Make dataframe for codes with an upper age threshold

snomed_age_threshold_codes = spark.createDataFrame(['60234000', '194983005', '194984004', '703323003', '60573004', '194987006', '427515002', '250976004', '276790000'], 'string').toDF('code')
icd10_age_threshold_codes = spark.createDataFrame(['I35', 'I350','I351', 'I352'], 'string').toDF('code')
opcs4_age_threshold_codes = spark.createDataFrame(['K312','K322','K352','K261','K262','K263','K264','K265','K268','K269','K302'], 'string').toDF('code')

# Read in descriptions of codes

code_dictionary = spark.table('hive_metastore.dsa_391419_j3w9t_collab.congenital_heart_disease_code_descriptions')

# COMMAND ----------

# Get chosen window (pre- or post- vaccination) end date from widget

window = dbutils.widgets.get('window')

# Check cases identified and overlap between them

CHD_cases = spark.table(f'dsa_391419_j3w9t_collab.chd_cases_ccu068_{window}')

gdppr_controls = spark.table(f'dsa_391419_j3w9t_collab.gdppr_controls_ccu068_{window}')
hes_apc_controls = spark.table(f'dsa_391419_j3w9t_collab.hes_apc_controls_ccu068_{window}')
hes_op_controls = spark.table(f'dsa_391419_j3w9t_collab.hes_op_controls_ccu068_{window}')

# COMMAND ----------

# Find the cases identified in each dataset

total_CHD_IDs = CHD_cases.select('NHS_number').distinct()
gdppr_CHD_IDs = CHD_cases.filter(F.col('dataset') == 'gdppr').select('NHS_number').distinct()
hes_apc_CHD_IDs = CHD_cases.filter(F.col('dataset') == 'hes_apc').select('NHS_number').distinct()
hes_op_CHD_IDs = CHD_cases.filter(F.col('dataset') == 'hes_op').select('NHS_number').distinct()

# COMMAND ----------

# Print total numbers of cases identified from each dataset

print(total_CHD_IDs.count(), ': Total number of CHD cases')
print(gdppr_CHD_IDs.count(), ': Total number of CHD cases from gp dataset')
print(hes_apc_CHD_IDs.count(), ': Total number of CHD cases from admitted patient care dataset')
print(hes_op_CHD_IDs.count(), ': Total number of CHD cases from outpatient dataset')

# COMMAND ----------

# Find the overlap between cases found in different datasets

print(gdppr_CHD_IDs.intersect(hes_apc_CHD_IDs).count(), ': Overlap in cases between gdppr and hes_apc')
print(gdppr_CHD_IDs.intersect(hes_op_CHD_IDs).count(), ': Overlap in cases between gdppr and hes_op')
print(hes_apc_CHD_IDs.intersect(hes_op_CHD_IDs).count(), ':  Overlap in cases between hes_op and hes_apc ')

# COMMAND ----------

# How many cases are only in outpatient data?

only_op = (
    hes_op_CHD_IDs
        .join(hes_apc_CHD_IDs, on = 'NHS_number', how = 'anti')
        .join(gdppr_CHD_IDs, on = 'NHS_number', how = 'anti')
)
print(only_op.count(), ': number of cases only identified in outpatient data')

# COMMAND ----------

# Which inclusion codes are most common in each dataset?
# GDPPR dataset

gdppr_code_freq = code_frequency(CHD_cases, 'gdppr', snomed_inclusion_codes, 'snom', code_dictionary)
display(gdppr_code_freq)
plot_common_codes(gdppr_code_freq)


# COMMAND ----------

# hes_op dataset common codes

hes_op_icd10_code_freq = code_frequency(CHD_cases, 'hes_op', icd10_inclusion_codes, 'diag', code_dictionary)
display(hes_op_icd10_code_freq)
plot_common_codes(hes_op_icd10_code_freq)

hes_op_opcs4_code_freq = code_frequency(CHD_cases, 'hes_op', opcs4_inclusion_codes, 'oper', code_dictionary)
display(hes_op_opcs4_code_freq)
plot_common_codes(hes_op_opcs4_code_freq)

# COMMAND ----------

# hes_apc common codes

hes_apc_icd10_code_freq = code_frequency(CHD_cases, 'hes_apc', icd10_inclusion_codes, 'diag', code_dictionary)
display(hes_apc_icd10_code_freq)
plot_common_codes(hes_apc_icd10_code_freq)

hes_apc_opcs4_code_freq = code_frequency(CHD_cases, 'hes_apc', opcs4_inclusion_codes, 'oper', code_dictionary)
display(hes_apc_opcs4_code_freq)
plot_common_codes(hes_apc_opcs4_code_freq)

# COMMAND ----------

# Which inclusion codes are most common in cases overlapping between datasets?

intersecting_records = gdppr_CHD_IDs.intersect(hes_op_CHD_IDs).intersect(hes_apc_CHD_IDs)
gdppr_records = CHD_cases.filter(F.col('dataset') == 'gdppr').join(intersecting_records, on = 'NHS_number', how = 'inner')

intersecting_CHD_diagnoses = gdppr_records.join(snomed_inclusion_codes, on = 'code', how = 'inner')
total = intersecting_CHD_diagnoses.count()
intersecting_freq = (
    intersecting_CHD_diagnoses
        .groupBy('code')
        .count()
        .withColumn('percent_of_diagnoses', F.lit(F.col('count')*100/total))
        .join(code_dictionary, on = 'code').dropDuplicates(['code'])
        .orderBy('percent_of_diagnoses', ascending = False)
)
display(intersecting_freq)
#plot_common_codes(intersecting_freq)

# COMMAND ----------

# Check diagnosis ages of people with age threshold codes

age_threshold_icd10 = CHD_cases.filter(F.col('code_type') == 'diag').join(icd10_age_threshold_codes, on = 'code', how = 'inner')
age_threshold_opcs4 = CHD_cases.filter(F.col('code_type') == 'oper').join(opcs4_age_threshold_codes, on = 'code', how = 'inner')
age_threshold_snomed = CHD_cases.filter(F.col('code_type') == 'snom').join(snomed_age_threshold_codes, on = 'code', how = 'inner')

age_threshold_cases = age_threshold_icd10.unionByName(age_threshold_opcs4).unionByName(age_threshold_snomed)
age_threshold_cases = age_threshold_cases.withColumn('age_at_episode', F.months_between(F.col('record_date'), F.col('DOB'))/12).groupBy('NHS_number').agg(F.min(F.col('AGE_AT_EPISODE')))

escaped = age_threshold_cases.filter(F.col('min(AGE_AT_EPISODE)')>65).orderBy(F.col('NHS_number'))
display(escaped)
