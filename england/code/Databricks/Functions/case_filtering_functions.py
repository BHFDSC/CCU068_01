from pyspark.sql import functions as F

def stack_diagnosis_columns(data, diagnosis_cols):
    stacked = (
    data
        .unpivot([F.col('NHS_number'), F.col('DOB'), F.col('ethnicity'), F.col('gp_practice'), F.col('sex'), F.col('record_date'), F.col('dataset')], diagnosis_cols, 'code_type', 'code')
        .withColumn('code_type', F.regexp_extract(F.col('code_type'), '^.{4}', 0))
        .filter(~ F.col('code').isin(['null','-   ']))
        .distinct()
    )
    return stacked

def combine_filters(pre_filtered_CHD_records, age_threshold_codes, exclusion_codes, inclusion_codes):

    # Remove age threshold codes from inclusion codes

    congenital_codes = inclusion_codes.join(age_threshold_codes, on = ['code', 'code_type'], how = 'anti')

    # Select records of AV diagnoses

    AV_records = (
        pre_filtered_CHD_records
        .join(age_threshold_codes, on = ['code', 'code_type'], how = 'inner')
    )

    congenital_records = (
        pre_filtered_CHD_records
        .join(congenital_codes.select('code', 'code_type'), on = ['code', 'code_type'], how = 'inner')
    )
    # Apply filtering steps to all AV records                             
    # 1. Remove those first diagnosed after 65
    # 2. Remove those first diagnosed after exclusion condition

    AV_IDs_below_threshold = filter_by_age(AV_records, 65)
    AV_records_below_threshold = AV_records.join(AV_IDs_below_threshold.select('NHS_number').distinct(), on = 'NHS_number', how = 'inner')
 
    AV_IDs_prior_exclusion_diagnosis = find_confounding_diagnosis_IDs(AV_records, pre_filtered_CHD_records, inclusion_codes, exclusion_codes)
    AV_records_not_excluded = AV_records_below_threshold.join(AV_IDs_prior_exclusion_diagnosis, on = 'NHS_number', how = 'anti')

    CHD_records = (
        congenital_records.unionByName(AV_records_not_excluded)
        .distinct()
    )

    return CHD_records

def include_patient_records(data, inclusion_codes):

    included_ids = (
        data
            .join(inclusion_codes, on = ['code', 'code_type'], how = 'inner')
            .select(F.col('NHS_number'))
            .distinct()
    )
    included_records = (
        included_ids
            .join(data, on = 'NHS_number', how = 'inner')
    )
    return included_records

def exclude_patient_records(records, excluded_records):
    filtered_records = (
        records
            .join(excluded_records, on = 'NHS_number', how = 'anti')
    )
    return filtered_records

# Filters  records, keeping only those with a diagnosis age above chosen limit
def filter_by_age(records, age_condition):
    age_filtered_IDs = (
        records
            .withColumn('age_at_episode', F.months_between(F.col('record_date'), F.col('DOB'))/12)
            .groupBy('NHS_number')
            .agg(F.min('age_at_episode'))
            .filter(F.col('min(age_at_episode)') < age_condition)
    )
    return age_filtered_IDs

# Finds the earliest record of an exclusion diagnosis, keeps IDs if CHD diagnosis after 
def find_exclusion_diagnosis_IDs(conditional_records, full_records, age_threshold_codes, exclusion_codes):
    
    exclusion_records = (
        conditional_records
            .select('NHS_number').distinct()
            .join(full_records, on = 'NHS_number', how = 'inner')
            .join(exclusion_codes, on = ['code', 'code_type'], how = 'inner')
    )
    
    earliest_chd_diagnosis = find_earliest_episode(conditional_records).withColumnRenamed('min(record_date)', 'diagnosis_date')
    earliest_exclusion_diagnosis =  find_earliest_episode(exclusion_records).withColumnRenamed('min(record_date)', 'exclusion_diagnosis_date')

    prior_exclusion_diagnosis = (
        earliest_chd_diagnosis
            .join(earliest_exclusion_diagnosis, on = 'NHS_number', how = 'inner')
            .filter(F.col('exclusion_diagnosis_date') <= F.col('diagnosis_date'))
            .select('NHS_number')
            .distinct()
    )
    return prior_exclusion_diagnosis

# Find the earliest record of a particular diagnosis for each patient
def find_earliest_episode(diagnoses):
    earliest_episodes = (
        diagnoses
            .groupBy([F.col('NHS_number')])
            .agg(F.min('record_date'))
    )
    return earliest_episodes