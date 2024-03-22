# Databricks notebook source
from pyspark.sql import functions as F
import matplotlib.pyplot as plt
%run '/Workspace/Repos/catriona.harrison@manchester.ac.uk/ccu068/functions/cohort_checking_functions.py'
dbutils.widgets.dropdown('window', 'pre_vaccination', ['pre_vaccination', 'post_vaccination'])
window = dbutils.widgets.get('window')

# COMMAND ----------

# Function to assign each CHD patient severity classification of most severe condition
def assign_severity(list, score_order):
    if list:
        list.sort(key=lambda x:score_order[x])
        most_severe = list[0]
        return most_severe
    else:
        return 'None'

# COMMAND ----------

# Function to check if list of phenotypes for a CHD patient includes pulmonary hypertension/cyanosis
def check_list(condition_list):
    if condition_list:
        for condition in condition_list:
            if condition == 'PH' or condition == 'CY':
                return 'Yes'
        return 'No'
    else:
        return 'No'
    
check_list_udf = F.udf(lambda x: check_list(x), 'string')

# COMMAND ----------

# Read in CHD code abbreviations and severity
code_abbrev_dict = spark.table('dsa_391419_j3w9t_collab.ccu068_code_dict')


# COMMAND ----------

# Assign conditions to each NHS number

CHD_records = spark.table(f'dsa_391419_j3w9t_collab.ccu068_cases_chd_records_{window}').select(['NHS_number', 'code', 'code_type']).distinct()      
matched_CHD_IDs =  spark.table(f'dsa_391419_j3w9t_collab.ccu068_cases_matched_personal_info_{window}').select('NHS_number').distinct()
matched_CHD_records = CHD_records.join(matched_CHD_IDs, on = 'NHS_number', how = 'inner')

CHD_case_conditions = (
    matched_CHD_records
        .join(code_abbrev_dict, on = ['code', 'code_type'], how = 'inner')
        .distinct()
)
print(CHD_case_conditions.select('NHS_number').distinct().count())


# COMMAND ----------

# Count numbers of each condition assigned
total_NHS_numbers = matched_CHD_IDs.select('NHS_number').distinct().count()

condition_counts = get_percentage_for_each_group(CHD_case_conditions.select('NHS_number', 'phenotype_abbreviation', 'phenotype_description').distinct(), 'cases', ['phenotype_abbreviation', 'phenotype_description'], total_NHS_numbers).drop('case_control')

# Add in '0' for conditions with no cases assigned to them
condition_counts= (
    condition_counts
        .join(code_abbrev_dict.select('phenotype_abbreviation', 'phenotype_description').distinct(), on = ['phenotype_abbreviation', 'phenotype_description'], how = 'right')
        .fillna(0)
        .orderBy(F.col('percent_of_cases').desc())
)

print(total_NHS_numbers, ': total number of cases')
display(condition_counts)

# COMMAND ----------

# Assign severity to each NHS number 

# Severe if pulmonary hypertension or cyanotic CHD 

    # 1. Read in CHD records with all diagnosis codes included
    # 2. Filter to include only those with codes in list for cyanosis or pulmonary hypertension
    # 3. Change severity score column to severe

CHD_cases_cyanosis = (
    matched_CHD_records
        .select('NHS_number', 'code')
        .filter(F.col('code').isin(['12770006', '127063008']))                    
        .withColumn('esc_complexity_classification', F.lit('Severe'))
        .withColumn('phenotype_abbreviation', F.lit('CY'))
        .withColumn('phenotype_description', F.lit('cyanosis'))
        .drop('code')
)

#Only incluse PH if case has a diagnosis of a shunt

CHD_all_records = spark.table(f'dsa_391419_j3w9t_collab.ccu068_cases_all_records_{window}').select(['NHS_number', 'code', 'code_type']).distinct()      
matched_CHD_IDs =  spark.table(f'dsa_391419_j3w9t_collab.ccu068_cases_matched_personal_info_{window}').select('NHS_number').distinct()
matched_CHD_all_records = CHD_all_records.join(matched_CHD_IDs, on = 'NHS_number', how = 'inner')

CHD_cases_PH_code = (
    matched_CHD_all_records
        .select('NHS_number', 'code')
        .filter((F.col('code').isin(['I272', 'I278', 'I279'])) & (F.col('code_type') == 'diag'))                     
        .withColumn('esc_complexity_classification', F.lit('Severe'))
        .withColumn('phenotype_abbreviation', F.lit('PH'))
        .withColumn('phenotype_description', F.lit('pulmonary hypertension'))
        .drop('code')
)

CHD_cases_shunt = (
    CHD_case_conditions
        .filter(F.col('phenotype_abbreviation').isin(['ASD', 'VSD', 'AVSD', 'TOF', 'PDA', 'HLH', 'HRH', 'CAT', 'CAVV', 'DILV', 'DIV', 'DIRV', 'DOLV', 'DORV', 'DOV', 'SV', 'TGA', 'TAPVC', 'MS', 'BAV', 'SVAS', 'SAS', 'PVEM', 'IAA', 'SD', 'AR', 'AS', 'AVD']))
        .select('NHS_number')
)

CHD_cases_PH = (
    CHD_cases_PH_code
        .join(CHD_cases_shunt, on = 'NHS_number', how = 'inner')
)
display(CHD_cases_PH)

# COMMAND ----------


# Moderate if septal defect with other associated abnormalities

# 1. Filter CHD cases to include NHS numbers which have septal defects
# 2. Filter to include those with other abnormalities
# 3. Change severity column to moderate

CHD_cases_SD = (
    CHD_case_conditions
        .filter(F.col('phenotype_abbreviation').isin(['ASD', 'VSD', 'AVSD']))
        .select('NHS_number')
)

CHD_cases_SD_associated_abnormalities = (
    CHD_case_conditions
        .join(CHD_cases_SD, on = 'NHS_number', how = 'inner')
        .filter(~F.col('phenotype_abbreviation').isin(['ASD', 'VSD', 'AVSD', 'SD']))
        .select('NHS_number', 'phenotype_abbreviation', 'phenotype_description')
        .withColumn('esc_complexity_classification', F.lit('Moderate'))
)


# COMMAND ----------

# Otherwise, give severity of most severe condition

    # 1. Sort severity score column by severity
    # 2. Keep first severity score in list

score_order = {'Severe':0, 'Moderate':1, 'Mild':2, 'null':3}
assign_severity_udf = F.udf(lambda x: assign_severity(x, score_order), 'string')

CHD_case_conditions_final = (
    CHD_case_conditions
        .select('NHS_number', 'phenotype_abbreviation', 'phenotype_description', 'esc_complexity_classification')
        .unionByName(CHD_cases_SD_associated_abnormalities)
        .unionByName(CHD_cases_cyanosis)
        .unionByName(CHD_cases_PH)
)

CHD_case_severity = (
    CHD_case_conditions_final
        .select('NHS_number', 'phenotype_abbreviation', 'esc_complexity_classification')
        .distinct()
        .groupBy('NHS_number')
        .agg(F.collect_list('esc_complexity_classification').alias('esc_scores'), F.collect_set('phenotype_abbreviation').alias('CHD_conditions'))
        .withColumn('pulmonary_hypertension', check_list_udf(F.col('CHD_conditions')))
        .withColumn('severity_score', assign_severity_udf(F.col('esc_scores')))
        .drop('esc_scores')
)
display(CHD_case_severity)

# COMMAND ----------

# Save final table of NHS numbers with conditions and severity
CHD_case_severity.write.mode('overwrite').option('overwriteSchema', 'True').saveAsTable(f'dsa_391419_j3w9t_collab.ccu068_cases_matched_severity_{window}')

# COMMAND ----------

CHD_case_severity = spark.table(f'dsa_391419_j3w9t_collab.ccu068_cases_matched_severity_{window}')

# COMMAND ----------

display(CHD_case_severity.groupBy('severity_score').agg(100*F.count(F.col('severity_score'))/total_NHS_numbers))
print(CHD_case_severity.count())
