# Databricks notebook source
import pyspark.sql.functions as F
from datetime import datetime

dbutils.widgets.dropdown('Group', 'cases', ['cases', 'controls'])
group = dbutils.widgets.get('Group')
dbutils.widgets.dropdown('Disease', 'IS', ['IS', 'IHD', 'HS', 'VTD', 'PE', 'carditis'])
group = dbutils.widgets.get('Disease')

# Function to remove diagnoses if they are within 14 days of one another
def remove_records_of_same_case(list):
    list.sort()
    start = True
    valid = []
    for date in list:
        if start == True:
            valid.append(date)
            prev_date = date
            start = False
        else:
            diff = date - prev_date
            if abs(diff.days) > 14:
                valid.append(date)
            prev_date = date
    
    number_of_events =  len(valid)
    return number_of_events


remove_same_case_udf = F.udf(lambda x: remove_records_of_same_case(x), 'int')

# COMMAND ----------

# Read in case/control records

cases = spark.table(f'dsa_391419_j3w9t_collab.ccu068_{group}_covid_records_unmatched_complication')
skinny = spark.table('dars_nic_391419_j3w9t_collab.__skinny_record').select('NHS_NUMBER_DEID', 'DATE_OF_BIRTH', 'SEX').withColumnRenamed('NHS_Number_DEID', 'NHS_NUMBER').withColumn('age', F.when(2020-F.year(F.col('Date_of_birth')) >50, '>50').otherwise('<50'))
cases = cases.join(skinny, on = 'NHS_number', how = 'inner')

# COMMAND ----------

thrombotic_snomed_codes = spark.table('dsa_391419_j3w9t_collab.snomed_code_table_for_thrombotic_episodes')

# COMMAND ----------

# Create lists of SNOMED and ICD-10 codes for cardiac inflammation and thrombotic complications

PE_icd10 = spark.createDataFrame([['PE', 'Pulmonary embolism', 'I269'], ['PE', 'Pulmonary embolism', 'I260']], ['disease_entity', 'term', 'code'])
VTD_icd10 = spark.createDataFrame([['VTD', 'Phlebitis and thrombophlebitis', 'I80'], ['VTD', 'Other vein thrombosis', 'I828'], ['VTD', 'Other vein thrombosis', 'I829'], ['VTD', 'Other vein thrombosis', 'I820'], ['VTD', 'Other vein thrombosis', 'I823'], ['VTD', 'Other vein thrombosis', 'I822'], ['VTD', 'Other vein thrombosis', 'I828'], ['VTD', 'Thrombosis during pregnancy and pueperium', 'O223'], ['VTD', 'Thrombosis during pregnancy and pueperium', 'O871'], ['VTD', 'Thrombosis during pregnancy and pueperium', 'O879'], ['VTD', 'Thrombosis during pregnancy and pueperium', 'O882'], ['VTD', 'Intracranial venous thrombosis during pregnancy and pueperium', 'O225'], ['VTD', 'Intracranial venous thrombosis during pregnancy and pueperium', 'O873'], ['VTD', 'Portal vein thrombosis', 'I81'], ['VTD', 'Intracranial venous thrombosis', 'G08'], ['VTD', 'Intracranial venous thrombosis', 'I676'], ['VTD', 'Intracranial venous thrombosis', 'I636'] ], ['disease_entity', 'term', 'code'])

IHD_snomed = thrombotic_snomed_codes.filter(F.col('coronary') == '1').select(F.col('conceptID').alias('code'), 'term').withColumn('disease_entity', F.lit('IHD'))
IHD_icd10 = spark.createDataFrame([['IHD', 'Acute myocardial infarction', 'I21'], ['IHD', 'Subsequent myocardial infarction', 'I22']],  ['disease_entity', 'term', 'code'])

isch_stroke_snomed = thrombotic_snomed_codes.filter(F.col('CVAisc') == '1').select(F.col('conceptID').alias('code'), 'term').withColumn('disease_entity', F.lit('IS'))
isch_stroke_icd10 = spark.createDataFrame([['IS', 'Cerebral infarction', 'I63']], ['disease_entity', 'term', 'code'])

haem_stroke_snomed = thrombotic_snomed_codes.filter(F.col('CVAhaem') == '1').select(F.col('conceptID').alias('code'), 'term').withColumn('disease_entity', F.lit('HS'))
haem_stroke_icd10 = spark.createDataFrame([['HS', 'Subarachnoid haemorrhage', 'I60'], ['HS', 'Intracerebral haemorrhage', 'I61'], ['HS', 'Other non-traumatic intracerebral haemorrhage', 'I62']], ['disease_entity', 'term', 'code'])

stroke_nos_snomed = thrombotic_snomed_codes.filter(F.col('CVAhaem') == '1').select(F.col('conceptID').alias('code'), 'term').withColumn('disease_entity', F.lit('CVAnos'))
stroke_nos_icd10 = spark.createDataFrame([['CVAnos', 'Stroke, not specified as haemorrhage or infarction', 'I64']], ['disease_entity', 'term', 'code'])

endocarditis_icd10 = spark.createDataFrame([['Endocarditis', 'Endocarditis, valve unspecified, in diseases classified elsewhere', 'I398'], ['Endocarditis', 'Endocarditis, valve unspecified', 'I38'], ['Endocarditis', 'Acute endocarditis, unspecified', 'I339'], ['Endocarditis', 'Acute and subacute infective endocarditis', 'I330'], ['Endocarditis', 'Acute and subacute endocarditis', 'I33']],  ['disease_entity', 'term', 'code'])
carditis_snomed = spark.createDataFrame([['Myocarditis', 'COVID-19 myocarditis', '1240531000000103']], ['disease_entity', 'term', 'code'])
carditis_icd10 = spark.table('dsa_391419_j3w9t_collab.ccu068_myopericarditis_icd10').unionByName(endocarditis_icd10)


# COMMAND ----------

# Find records of specified disease in GDPPR, HES APC and HES OP

gdppr_data = spark.table('hive_metastore.dars_nic_391419_j3w9t.gdppr_dars_nic_391419_j3w9t')
gdppr_complication = (gdppr_data
        .select(F.col('NHS_NUMBER_DEID').alias('NHS_number'), F.col('CODE').alias('code'), F.col('RECORD_DATE'))
        .filter((F.col('record_date') > F.to_date(F.lit('2020-03-01'), format = 'yyyy-MM-dd')) & (F.col('record_date') < F.to_date(F.lit('2022-04-01'), format = 'yyyy-MM-dd')))
        .join(stroke_nos_snomed, on = 'code', how = 'inner')
)


hes_apc = spark.table('dars_nic_391419_j3w9t_collab._hes_apc_all_years')
hes_apc_complication_4_char = (
    hes_apc
        .select(F.col('PERSON_ID_DEID').alias('NHS_number'), F.col('EPISTART').alias('record_date'), F.col('diag_4_01').alias('code'))
        .withColumn('RECORD_DATE', F.to_date(F.regexp_replace(F.col('RECORD_DATE'), '([0-9]{4})-([0-9]{2})-([0-9]{2})', '$2-$3-$1'), format = 'MM-dd-yyyy'))
        .filter((F.col('record_date') > F.to_date(F.lit('2020-03-01'), format = 'yyyy-MM-dd')) & (F.col('record_date') < F.to_date(F.lit('2022-04-01'), format = 'yyyy-MM-dd')))
        .join(stroke_nos_icd10, on = 'code', how = 'inner')
)

hes_apc_complication_3_char = (
    hes_apc
        .select(F.col('PERSON_ID_DEID').alias('NHS_number'), F.col('EPISTART').alias('record_date'), F.col('diag_3_01').alias('code'))
        .withColumn('RECORD_DATE', F.to_date(F.regexp_replace(F.col('RECORD_DATE'), '([0-9]{4})-([0-9]{2})-([0-9]{2})', '$2-$3-$1'), format = 'MM-dd-yyyy'))
        .filter((F.col('record_date') > F.to_date(F.lit('2020-03-01'), format = 'yyyy-MM-dd')) & (F.col('record_date') < F.to_date(F.lit('2022-04-01'), format = 'yyyy-MM-dd')))
        .join(stroke_nos_icd10, on = 'code', how = 'inner')
)

# COMMAND ----------


hes_op = spark.table('dars_nic_391419_j3w9t_collab._hes_op_all_years')
hes_op_complication_4_char = (
    hes_op
        .select(F.col('PERSON_ID_DEID').alias('NHS_number'), F.col('APPTDATE').alias('record_date'), F.col('diag_4_01').alias('code'))
        .withColumn('RECORD_DATE', F.to_date(F.regexp_replace(F.col('RECORD_DATE'), '([0-9]{4})-([0-9]{2})-([0-9]{2})', '$2-$3-$1'), format = 'MM-dd-yyyy'))
        .filter((F.col('record_date') > F.to_date(F.lit('2020-03-01'), format = 'yyyy-MM-dd')) & (F.col('record_date') < F.to_date(F.lit('2022-04-01'), format = 'yyyy-MM-dd')))
        .join(stroke_nos_icd10, on = 'code', how = 'inner')
)

hes_op_complication_3_char = (
    hes_op
        .select(F.col('PERSON_ID_DEID').alias('NHS_number'), F.col('APPTDATE').alias('record_date'), F.col('diag_3_01').alias('code'))
        .withColumn('RECORD_DATE', F.to_date(F.regexp_replace(F.col('RECORD_DATE'), '([0-9]{4})-([0-9]{2})-([0-9]{2})', '$2-$3-$1'), format = 'MM-dd-yyyy'))
        .filter((F.col('record_date') > F.to_date(F.lit('2020-03-01'), format = 'yyyy-MM-dd')) & (F.col('record_date') < F.to_date(F.lit('2022-04-01'), format = 'yyyy-MM-dd')))
        .join(stroke_nos_icd10, on = 'code', how = 'inner')
)

# COMMAND ----------

cohort_complication = cases.join(hes_apc_complication_3_char.unionByName(hes_apc_complication_4_char).unionByName(hes_op_complication_3_char).unionByName(hes_op_complication_4_char).unionByName(gdppr_complication), on = 'NHS_number', how = 'left').distinct()


# COMMAND ----------

# Count number of events in 14 days after COVID-19 infection (vaccinated)

vaccinated_covid_cases = cohort_complication.filter(F.col('vaccinated_pre_infection') == 'Yes')
vaccinated_covid_person_days = vaccinated_covid_cases.select('NHS_number', 'age', 'sex').distinct().groupBy('age', 'sex').agg(F.count(F.col('NHS_number')) *14)
vaccinated_covid_cases_complication = vaccinated_covid_cases.filter((F.datediff(F.col('record_date'), F.col('first_infection_date')) >0) & (F.datediff(F.col('record_date'), F.col('first_infection_date')) <= 14)).select('NHS_number', 'age', 'sex', 'disease_entity').distinct().groupBy('age', 'sex', 'disease_entity').agg(F.count(F.col('NHS_number')))

display(vaccinated_covid_cases_complication.join(vaccinated_covid_person_days, on = ['age', 'sex'], how = 'right'))


# COMMAND ----------

# Count number of events in 14 days after COVID-19 infection (unvaccinated)


unvaccinated_covid_cases = cohort_complication.filter((F.col('vaccinated_pre_infection') == 'No') & (F.col('first_infection_date').isNotNull()))
unvaccinated_covid_person_days = unvaccinated_covid_cases.select('NHS_number', 'age', 'sex').distinct().groupBy('age', 'sex').agg(F.count(F.col('NHS_number')) *14)
unvaccinated_covid_cases_complication = unvaccinated_covid_cases.filter((F.datediff(F.col('record_date'), F.col('first_infection_date')) >0) & (F.datediff(F.col('record_date'), F.col('first_infection_date')) <= 14)).select('NHS_number', 'age', 'sex', 'disease_entity').distinct().groupBy('age', 'sex', 'disease_entity').agg(F.count(F.col('NHS_number')))

display(unvaccinated_covid_cases_complication.join(unvaccinated_covid_person_days, on = ['age', 'sex'], how = 'right'))



# COMMAND ----------

# Count number of events in 14 days after 1st COVID-19 vaccination

vaccinated_once = cohort_complication.filter((F.col('first_vaccination_date').isNotNull()) & (
    (F.col('first_infection_date')>F.col('first_vaccination_date')) | (F.col('first_infection_date').isNull()) 
))
first_vaccination_person_days = vaccinated_once.select('NHS_number', 'age', 'sex').distinct().groupBy('age', 'sex').agg(F.count(F.col('NHS_number')) *14)
first_vaccination_complication = vaccinated_once.filter((F.datediff(F.col('record_date'), F.col('first_vaccination_date')) >0) & (F.datediff(F.col('record_date'), F.col('first_vaccination_date')) < 14)).select('NHS_number', 'age', 'sex', 'disease_entity').distinct().groupBy('age', 'sex', 'disease_entity').agg(F.count(F.col('NHS_number')))

display(first_vaccination_complication.join(first_vaccination_person_days, on = ['age', 'sex'], how = 'right'))

# COMMAND ----------

# Count number of events in 14 days after 2nd COVID-19 vaccination

vaccinated_twice = cohort_complication.filter((F.col('second_vaccination_date').isNotNull()) & (
    (F.col('first_infection_date')>F.col('second_vaccination_date')) | (F.col('first_infection_date').isNull()) 
))
second_vaccination_person_days = vaccinated_twice.select('NHS_number', 'age', 'sex').distinct().groupBy('age', 'sex').agg(F.count(F.col('NHS_number')) *14)
second_vaccination_complication = vaccinated_twice.filter((F.datediff(F.col('record_date'), F.col('second_vaccination_date')) >0) & (F.datediff(F.col('record_date'), F.col('second_vaccination_date')) < 14)).select('NHS_number', 'age', 'sex', 'disease_entity').distinct().groupBy('age', 'sex', 'disease_entity').agg(F.count(F.col('NHS_number')))

display(second_vaccination_complication.join(second_vaccination_person_days, on = ['age', 'sex'], how = 'right'))


# COMMAND ----------

# Count number of events in time up until first infection or vaccination, removing diagnoses within 14 days of one another

pre_covid_baseline = cohort_complication.filter(F.col('second_vaccination_date').isNull())

baseline_person_days = pre_covid_baseline.select('NHS_number', 'days_in_study', 'age', 'sex').distinct().groupBy('age', 'sex').agg(F.sum((F.col('days_in_study'))))

baseline_complication = pre_covid_baseline.filter(F.col('record_date') < F.col('exclusion_date')).groupBy('NHS_number', 'disease_entity', 'age', 'sex').agg(F.collect_list(F.col('record_date')).alias('event_list')).withColumn('num_events', remove_same_case_udf(F.col('event_list'))).groupBy('disease_entity', 'age', 'sex').agg(F.sum((F.col('num_events')).alias('cases')))

display(baseline_complication.join(baseline_person_days, on = ['age', 'sex'], how = 'right'))