import pandas as pd
import re
from collections import Counter
from functools import reduce
import dlh_utils
from dlh_utils import utilities
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, regexp_replace, upper, col, when, length, split, regexp_extract, trim
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
import address_functions.pre_processing as pre_processing
import address_functions.results as results
import address_functions.sac as sac  
from address_functions.config.settings import town_list



spark = dlh_utils.sessions.getOrCreateSparkSession(appName='demo', size='medium')


# reading in pandas, read in is simpler in pandas. Hence I have opted for this rather than spark.read
# this csv file is uploaded to cloudera, change this to suit your need. For me this was the area the csv was located. 
df = pd.read_csv('addr_index/data/pds_2022_under_65_conf.csv')

# making the dataframe to have spark utility 
df = utilities.pandas_to_spark(df)

# quickly cacheing this dataset
df.cache()

df.columns



df.count()

############################################ Testing correct invalid postcodes

# Get the UDF from the map_and_check_postcode function
postcode_correction = map_and_check_postcode()["map_and_check_postcode_udf"]

# Apply the UDF to the DataFrame
df = df.withColumn("output", postcode_correction(col("supplied_query_address")))

# Extract the cleaned address and changes flag from the output
df = df.withColumn("supplied_query_address", col("output.final_cleaned_address")) \
       .withColumn("postcode_corrected_flag", col("output.changes_flag")) \
       .drop("output")

# Count the number of records where postcodes were corrected
postcode_corrected_count = df.filter(df.postcode_corrected_flag == 1).count()

# Print the count of corrected postcodes
print(f"Postcodes corrected count: {postcode_corrected_count}")

# Show some results for inspection
df.select("supplied_query_address", "postcode_corrected_flag").where(df.postcode_corrected_flag == 1).show(100, truncate=False)

#############################################

df_clean = pre_processing.clean_punctuation(df)

df_clean.select("supplied_query_address", "final_cleaned_address").where(df_clean.punctuation_cleaned_flag == 1).show(100, truncate=False)

df_clean2 = pre_processing.remove_noise_words_with_flag(df_clean)

df_clean2.select("supplied_query_address", "final_cleaned_address").where(df_clean2.noise_removed_flag == 1).show(100, truncate=False)

df_clean3 = pre_processing.get_process_and_deduplicate_address_udf(df_clean)

df_clean3.select("supplied_query_address", "final_cleaned_address").where(df_clean3.words_deduplicated_flag == 1).show(100, truncate=False)

df_clean4 = sac.standardise_street_types(df_clean)

df_clean4.select("supplied_query_address", "final_cleaned_address").where(df_clean4.street_type_standardised_flag == 1).show(100, truncate=False)

df_clean5 = pre_processing.remove_noise_words_with_flag(df_clean)

df_clean5.select("supplied_query_address", "final_cleaned_address").where(df_clean2.noise_removed_flag == 1).show(100, truncate=False)

df_clean6 = pre_processing.remove_noise_words_with_flag(df_clean)

df_clean6.select("supplied_query_address", "final_cleaned_address").where(df_clean2.noise_removed_flag == 1).show(100, truncate=False)

df_clean7 = pre_processing.remove_noise_words_with_flag(df_clean)

df_clean7.select("supplied_query_address", "final_cleaned_address").where(df_clean2.noise_removed_flag == 1).show(100, truncate=False)

############################################################ Testing above'''

# Assuming the process_df_default function returns three DataFrames: df, df_all_flags_zero, df_any_flags_set
df, df_all_flags_zero, df_any_flags_set = results.process_df_default(df, "supplied_query_address")

<<<<<<< HEAD
df.select("supplied_query_address", "final_cleaned_address")

=======
>>>>>>> 5dec019ff572e3acd20a44a946714ed3d3a2c9c1
'''# Convert PySpark DataFrame to Pandas DataFrame
df_pandas = df_all_flags_zero.toPandas()

# Write the Pandas DataFrame directly to the CSV file
df_pandas.to_csv('addr_index/data_output/electoral_register_55to64_output.csv', index=False)'''

'''# Check distribution of ones and zeroes for the 'length_flag' column
df.groupBy('punctuation_cleaned_flag').count().show()

df.groupBy('noise_removed_flag').count().show()

df.groupBy('words_deduplicated_flag').count().show()

df.groupBy('postcodes_deduplicated_flag').count().show()

df.groupBy('street_type_standardised_flag').count().show()

df.groupBy('patterns_identified_flag').count().show()

df.groupBy('location_units_identified_flag').count().show()

df.groupBy('unwanted_characters_removed_flag').count().show()

df.groupBy('just_country_postcode_flag').count().show()

df.groupBy('just_county_postcode_flag').count().show()

df.groupBy('just_town_postcode_flag').count().show()

df.groupBy('keyword_flag').count().show()

df.groupBy('criteria_flag').count().show()

df.groupBy('country_postcode_flag').count().show()

df.groupBy('country_position_flag').count().show()

df.groupBy('invalid_postcode_flag').count().show()

df.columns
'''

df.columns
df.limit(40).toPandas()

df.select("supplied_query_address", "final_cleaned_address").where(df.punctuation_cleaned_flag == 1).show(100, truncate=False)

df.select("supplied_query_address", "final_cleaned_address").where(df.noise_removed_flag == 1).show(500, truncate=False)

df.select("supplied_query_address", "final_cleaned_address").where(df.words_deduplicated_flag == 1).show(500, truncate=False)

df.select("supplied_query_address", "final_cleaned_address").where(df.postcodes_deduplicated_flag == 1).show(100, truncate=False)

df.select("supplied_query_address", "final_cleaned_address").where(df.street_type_standardised_flag == 1).show(100, truncate=False)

postcodes_corrected_flag

df_clean.columns

df.select("supplied_query_address", "final_cleaned_address").where(df.patterns_identified_flag == 1).show(100, truncate=False)

df.select("supplied_query_address", "final_cleaned_address").where(df.location_units_identified_flag == 1).show(100, truncate=False)


df.select("supplied_query_address", "final_cleaned_address").where(df.changes_flag == 1).show(100, truncate=False)

df.select("supplied_query_address", "final_cleaned_address").where(df.just_town_postcode_flag == 1).show(100, truncate=False)


df.select("supplied_query_address", "final_cleaned_address").where(df.length_flag = 1).show(40, Truncate=False)

# if you'd like to see how each function behaves (isn't available for all):


######################################################################################

# CLEAN_PUNCTUATION

clean_punctuation_ex = pre_processing.clean_punctuation(df, "supplied_query_address")

clean_punctuation_ex.select("supplied_query_address","final_cleaned_address").where\
(clean_punctuation_ex.punctuation_cleaned_flag == 1).show(50, truncate=False)

#####################################################################################

# WORDS_DEDUPLICATED

df.select("supplied_query_address","final_cleaned_address").where\
(df.words_deduplicated_flag == 1).show(50, truncate=False)

#####################################################################################

DEDUPE_UK_POSTCODE

testing_df.select("supplied_query_address","final_cleaned_address").where\
(testing_df.postcodes_deduplicated_flag == 1).show(50, truncate=False)

######################################################################################

# LENGTH_FLAG


testing_df.select("final_cleaned_address").where\
(testing_df.length_flag == 1).show(50, truncate=False)

######################################################################################

# JUST_TOWN_POSTCODE_EXACT (DEFAULT)

testing_df.select("supplied_query_address","final_cleaned_address").where\
(testing_df.just_town_postcode_flag == 1).show(50, truncate=False)


######################################################################################

# JUST COUNTRY_POSTCODE_EXACT (DEFAULT)

testing_df.select("supplied_query_address","final_cleaned_address").where\
(testing_df.just_country_postcode_flag == 1).show(50, truncate=False)

######################################################################################

#JUST COUNTY_POSTCODE_EXACT (DEFAULT)

testing_df.select("supplied_query_address","final_cleaned_address").where\
(testing_df.just_county_postcode_flag == 1).show(50, truncate=False)

######################################################################################

# KEYWORD PRESENT

testing_df.select("supplied_query_address","final_cleaned_address").where\
(testing_df.keyword_flag == 1).show(50, truncate=False)

######################################################################################

# ALL_3_CRITERIA

testing_df.select("supplied_query_address","final_cleaned_address").where\
(testing_df.criteria_flag == 1).show(50, truncate=False)

######################################################################################

# HAS_COUNTRY_AND_ZZ99

testing_df.select("supplied_query_address","final_cleaned_address").where\
(testing_df.country_postcode_flag == 1).show(50, truncate=False)

######################################################################################

# COUNTRY_IN_LAST_HALF (WORK IN PROGRESS)

testing_df.select("supplied_query_address","final_cleaned_address").where\
(testing_df.country_position_flag == 1).show(50, truncate=False)

######################################################################################

# IS_INVALID_POSTCODE

testing_df.select("supplied_query_address","final_cleaned_address").where\
(testing_df.invalid_postcode_flag == 1).show(50, truncate=False)

######################################################################################

# FILTER_AND_COUNT_ALL_FLAGS_ZERO

all_flags_zero = filter_and_count_all_flags_zero(testing_df)


######################################################################################

# FILTER_WITH_ANY_FLAGS_YES

any_flags_yes = filter_records_with_any_flags_set(testing_df)
                                                  
any_flags_yes.limit(40).toPandas()                                                  


