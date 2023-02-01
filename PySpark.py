from pyspark.sql import SparkSession
from pyspark.sql.functions import count, countDistinct, col
import matplotlib.pyplot as plt

spark = SparkSession.builder.getOrCreate()
df = spark.read.csv('/Users/harish/Downloads/crime_data.csv', header=True)

district_offense_summary = df.groupby('DISTRICT').agg(count(col('INCIDENT_NUMBER')).alias('district_incident_count'))
district_offense_summary = district_offense_summary.sort('district_incident_count', ascending=False)
district_offense_summary.show(10, False)
# The top 3 districts by crime volume are B2, D4 and C11


offence_type_summary = df.groupby('OFFENSE_CODE_GROUP').agg(count(col('INCIDENT_NUMBER')).alias('incident_count'))
offence_type_summary = offence_type_summary.sort('incident_count', ascending=False)
offence_type_summary.show(10, False)
# Top 3 offense code groups are

offence_type_summary_df = offence_type_summary.toPandas()
offence_type_summary_df['offence_grouping'] = offence_type_summary_df.apply(
    lambda x: x['OFFENSE_CODE_GROUP'] if x['incident_count'] > 1000 else 'other', axis=1)
offence_type_summary_df = offence_type_summary_df.groupby(['offence_grouping'])['incident_count'].sum().reset_index()
offence_type_summary_df = offence_type_summary_df.set_index('offence_grouping')
offence_type_summary_df.plot(kind='pie', y='incident_count', legend=False)

district_offense_type_summary = df.groupby(['DISTRICT', 'OFFENSE_CODE_GROUP']).agg(
    count(col('INCIDENT_NUMBER')).alias('incident_count'))
district_offense_type_summary = district_offense_type_summary.sort('incident_count', ascending=False)
district_offense_type_summary.show(10, False)
district_offense_type_summary_pd = district_offense_type_summary.toPandas()

district_offense_type_pivot = district_offense_type_summary.groupby('OFFENSE_CODE_GROUP').pivot('DISTRICT').sum(
    'incident_count').show(10, False)

district_offense_type_perc = district_offense_type_summary.join(district_offense_summary, on='DISTRICT', how='inner')

district_offense_type_perc = district_offense_type_perc.withColumn('incident_perc', col('incident_count') / col(
    'district_incident_count'))
district_offense_type_perc = district_offense_type_perc.sort(['DISTRICT', 'incident_perc'], ascending=False)

larceny_perc_by_district = district_offense_type_perc.filter('OFFENSE_CODE_GROUP="Larceny"').sort('incident_perc',
                                                                                                  ascending=False)
larceny_perc_by_district.show(10, False)
# Districts D4, A1 and C6 have the highest percentages of larceny as a crime within their district

larceny_perc_by_district_df = larceny_perc_by_district.toPandas()
# This pandas dataframe can be plotted - bar/column chart to show Larceny percentage by district
