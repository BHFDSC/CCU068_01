from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DateType
from pyspark.sql.window import Window
from collections import Counter

def latest_occurrence(list):
    if list:
        latest_occurrence = sorted(list, reverse = True)[0]
        return latest_occurrence

def find_most_common_entry(CHD_cases, column):

    count_entries_udf = F.udf(lambda x:len(x), IntegerType())
    latest_occurrence_udf = F.udf(lambda x:latest_occurrence(x), DateType())
    column_count_window = Window.partitionBy('NHS_number').orderBy(F.col('column_count').desc())
    latest_occurrence_window = Window.partitionBy('NHS_number').orderBy(F.col('latest_occurrence').desc())

    resolved_details = (
        CHD_cases
            .filter((F.col(column) != 'Z') | (F.date_format(F.col(column), format = 'MM-dd-yyyy') != 'null'))
            .groupBy(['NHS_number', column])
            .agg(F.collect_list('record_date').alias('record_dates'))
            .withColumn('column_count', count_entries_udf(F.col('record_dates')))
            .withColumn('latest_occurrence', latest_occurrence_udf(F.col('record_dates')))
            .withColumn('column_count_rank', F.rank().over(column_count_window))
            .filter(F.col('column_count_rank') == 1)
            .withColumn('occurrence_rank', F.rank().over(latest_occurrence_window))
            .filter(F.col('occurrence_rank') == 1)
            .select('NHS_number', column)
            .dropDuplicates(['NHS_number'])
    )

    CHD_cases_resolved = (
        CHD_cases
            .select('NHS_number').distinct()
            .join(resolved_details, on = 'NHS_number', how = 'left')
            .fillna('Z')
    )
    return CHD_cases_resolved


def label_rows(entries, name):
    window = Window.partitionBy('matching_id').orderBy('matching_id')
    grouped_entries = entries.withColumn('row_number', F.row_number().over(window))
    max_entries = (
        grouped_entries
            .groupBy(F.col('matching_id'))
            .agg(F.max('row_number'))
            .withColumnRenamed('max(row_number)', f'max_{name}_number')
    )
    return grouped_entries, max_entries

def resolve_conflicting_data(dataset):

    DOB_resolved = find_most_common_entry(dataset, 'DOB')
    ethnicity_resolved = find_most_common_entry(dataset, 'ethnicity')
    practice_resolved = find_most_common_entry(dataset, 'gp_practice')
    sex_resolved = find_most_common_entry(dataset, 'sex')
  
    final_data = (
        ethnicity_resolved
            .join(practice_resolved, on = 'NHS_number', how = 'inner')
            .join(sex_resolved, on = 'NHS_number', how = 'inner')
            .join(DOB_resolved, on = 'NHS_number', how = 'inner')
            .distinct()
    )
    return final_data

def match_cases_to_controls(cases, controls, ratio):

    # Count numer of cases/controls with each matching ID 
    grouped_controls, max_controls_per_group = label_rows(controls, 'control')
    grouped_cases, max_cases_per_group = label_rows(cases, 'case')
    
    # Split controls into groups of 4 with same matching ID
    max_control_groups = max_controls_per_group.withColumn('max_groups', F.floor(F.col('max_control_number')/ratio)).select('max_control_number', 'matching_id', 'max_groups').distinct()
    grouped_controls_rounded = grouped_controls.join(max_control_groups, on = 'matching_id', how = 'inner').filter(F.col('row_number') <= F.col('max_control_number') - F.col('max_control_number')%ratio).drop('max_control_number')

    # Join cases and controls on matching id and find case/control ratio
    case_control_ratios = max_control_groups.join(max_cases_per_group, on = 'matching_id', how = 'inner')
    case_control_ratios = case_control_ratios.withColumn('max_controls_required', ratio * F.col('max_case_number')).withColumn('max_cases_required', F.col('max_groups')).select('matching_id', 'max_controls_required', 'max_cases_required').distinct()
    
    # Remove controls with row number above matching cases*4
    matched_controls = grouped_controls_rounded.join(case_control_ratios, on = 'matching_id', how = 'inner').filter(F.col('row_number') <= F.col('max_controls_required'))
    unmatched_controls = grouped_controls.join(matched_controls, on = ['matching_id', 'row_number'], how = 'anti')

    # Remove cases with row number above matching controls/4 
    matched_cases = grouped_cases.join(case_control_ratios, on = 'matching_id', how = 'inner').filter(F.col('row_number') <= F.col('max_cases_required'))
    unmatched_cases = grouped_cases.join(matched_cases, on = ['matching_id', 'row_number'], how = 'anti')

    matched_cases = matched_cases.select('NHS_number', 'DOB', 'sex', 'gp_practice', 'ethnicity').distinct()
    matched_controls = matched_controls.select('NHS_number', 'DOB', 'sex', 'gp_practice', 'ethnicity').distinct()
    unmatched_cases = unmatched_cases.select('NHS_number', 'DOB', 'sex', 'gp_practice', 'ethnicity').distinct()
    unmatched_controls = unmatched_controls.select('NHS_number', 'DOB', 'sex', 'gp_practice', 'ethnicity').distinct()

    return matched_cases, matched_controls, unmatched_cases, unmatched_controls