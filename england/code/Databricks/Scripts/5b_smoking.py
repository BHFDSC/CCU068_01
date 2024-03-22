# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import Window

dbutils.widgets.dropdown('window', 'post_vaccination', ['pre_vaccination', 'post_vaccination'])
window = dbutils.widgets.get('window')
window_dict = {'pre_vaccination': '03-01-2020', 'post_vaccination': '03-01-2021'}
window_start = window_dict[window]

# COMMAND ----------

# Create code lists for assignment of smoking status: current, former, never

current_smoker = spark.createDataFrame(['230062009', '230065006', '230063004', '230060001', '230059006', '225934006', '203191000000107', '230064005', '449869002', '56771006', '56578002', '266918002', '65568007', '77176002', '82302008', '308438006', '394871007', '394872000', '394873005', '446172000', '401159003', '401201003', '134406006', '836001000000109', '413173009', '160613002', '160616005', '160619003', '266918002', '266929003', '308438006', '8517006', '230056004', '230057008', '230058003', '428041000124106', '771376002', '266918002', '266920004', '160606002', '160604004', '160603005', '428041000124106', '428041000124106', '160605003', '266920004', '59978006' ], 'string').toDF('code').withColumn('status', F.lit('current_smoker'))
former_smoker = spark.createDataFrame(['1092111000000104', '360900008', '360890004' , '1092031000000108', '1092091000000109', '1092131000000107', '228486009', '1092041000000104', '48031000119106', '735112005', '735128000', '53896009', '1092071000000105', '492191000000103', '266921000', '266922007', '266923002', '266924008', '266925009', '8517006', '160617001', '160625004', '8517006', '281018007', '266928006', '160621008', '160620009', '8517006', '266928006'], 'string').toDF('code').withColumn('status', F.lit('former_smoker'))
never_smoked = spark.createDataFrame(['221000119102', '266919005'], 'string').toDF('code').withColumn('status', F.lit('never_smoked'))
smoking_codes = current_smoker.unionByName(former_smoker).unionByName(never_smoked)

# COMMAND ----------

# Read in case and control NHS numbers

chd_cases = spark.table(f'dsa_391419_j3w9t_collab.ccu068_cases_matched_personal_info_{window}').select('NHS_number').distinct()
controls = spark.table(f'dsa_391419_j3w9t_collab.ccu068_controls_matched_personal_info_{window}').select('NHS_number').distinct()
gdppr = spark.table('hive_metastore.dars_nic_391419_j3w9t.gdppr_dars_nic_391419_j3w9t').select(F.col('NHS_NUMBER_DEID').alias('NHS_number'), 'code', 'record_date')

# COMMAND ----------

# Join cases and controls to smoking records in GDPPR

smoking_gdppr_codes = gdppr.join(smoking_codes, on = 'code', how = 'inner')
smoking_cases = chd_cases.join(smoking_gdppr_codes, on = 'NHS_number', how = 'left')
smoking_controls = controls.join(smoking_gdppr_codes, on = 'NHS_number', how = 'left')

# COMMAND ----------

# Take most recent record of smoking status for each person

NHS_number_window = Window.orderBy(F.col('record_date').desc()).partitionBy('NHS_number')

smoking_status_cases = (
    smoking_cases
        .withColumn('row_number', F.row_number().over(NHS_number_window))
        .filter(F.col('row_number') == 1)
        .drop('row_number')
        .withColumn('smoking_status', F.when(F.col('status') == None, 'Unknown').otherwise(F.col('status')))
        .drop('status', 'code', 'record_date')
        )
smoking_status_controls = (
    smoking_controls
        .withColumn('row_number', F.row_number().over(NHS_number_window))
        .filter(F.col('row_number') == 1)
        .drop('row_number')
        .withColumn('smoking_status', F.when(F.col('status') == None, 'Unknown').otherwise(F.col('status')))
        .drop('status', 'code', 'record_date')
        )


# COMMAND ----------

# Display percentage of cases and controls with each smoking status

total_cases = smoking_status_cases.select('NHS_number').distinct().count()
total_controls = smoking_status_controls.select('NHS_number').distinct().count()

display(smoking_status_cases.groupBy(F.col('smoking_status')).agg(100*F.count(F.col('NHS_number'))/total_cases))
display(smoking_status_controls.groupBy(F.col('smoking_status')).agg(100*F.count(F.col('NHS_number'))/total_controls))

# COMMAND ----------

smoking_status_cases.write.mode('overwrite').option('overwriteSchema', 'True').saveAsTable(f'dsa_391419_j3w9t_collab.ccu068_cases_matched_smoking_{window}')
smoking_status_controls.write.mode('overwrite').option('overwriteSchema', 'True').saveAsTable(f'dsa_391419_j3w9t_collab.ccu068_controls_matched_smoking_{window}')