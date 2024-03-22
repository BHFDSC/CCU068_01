from pyspark.sql import functions as F

def get_percentage_for_each_group(dataset, group, column, total_number):
    
    percentage = (
        dataset
            .select('NHS_number', *column)
            .distinct()
            .groupBy(column).count()
            .withColumn(f'percent_of_{group}', F.round(F.lit(F.col('count')*100/total_number), 6)) # Change back
            .withColumn('case_control', F.lit(group))
            .drop('count')
    )
    return percentage

def code_frequency(CHD_cases, dataset, inclusion_codes, code_type, code_dictionary):  

    records = CHD_cases.filter((F.col('dataset') == dataset) & (F.col('code_type') == code_type)).select(F.col('NHS_number'), F.col('code')).distinct()
    CHD_diagnoses = records.join(inclusion_codes, on = 'code', how = 'inner')
    total = CHD_diagnoses.count()
    diagnosis_totals = (
        CHD_diagnoses
            .groupBy('code')
            .count()
            .withColumn('percent_of_diagnoses', F.round(F.lit(F.col('count')*100/total), 2))
            .join(code_dictionary, on = 'code', how = 'left').dropDuplicates(['code'])
            .orderBy('percent_of_diagnoses', ascending = False)
    )
    return diagnosis_totals

def plot_common_codes(diagnosis_totals):

    code_freq = diagnosis_totals.orderBy(F.col('count'), ascending = False).limit(15)
    code_freq = code_freq.toPandas()
    code_freq.plot.bar(x = 'code')