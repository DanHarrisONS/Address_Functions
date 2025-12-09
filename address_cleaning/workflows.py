import pandas as pd
import re
from collections import Counter
from functools import reduce
from pyspark.sql import SparkSession, DataFrame

import openpyxl
import xlrd
from fuzzywuzzy import fuzz

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame, udf
from pyspark.sql.functions import udf, regexp_replace, upper, col, when, length, split, regexp_extract, trim
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, IntegerType, StructType, StructField


from address_functions.pre_processing import (
    clean_punctuation, remove_noise_words_with_flag,
    get_process_and_deduplicate_address_udf, deduplicate_postcodes_udf, map_and_check_postcode, standardise_street_types
)

from address_functions.quality_flags import (
    add_length_flag, just_town_postcode, just_town_postcode_exact,
    just_country_postcode, just_country_postcode_exact,
    just_county_postcode, just_county_postcode_exact,
    keyword_present, all_3_criteria, has_country_and_ZZ99,
    country_in_last_half, is_invalid_postcode)

from address_functions.sac import (
    extract_postcode_town_address)



#################################################################################
"""
    Function: filter_and_count_all_flags_zero

    Purpose:
    This function processes the DataFrame by:
    1. Filtering the records based on flags.
    2. Excluding specified columns ("punctuation_cleaned_flag", "words_deduplicated_flag", "postcodes_deduplicated_flag") 
       from the flag-checking process.
    3. Checking for all flags set to 0 (excluding the columns we specify).
    4. Filtering the df to retain only rows where all of the checked flags are set to 0.
    5. Counting and printing the number of such records.

    Parameters:
    - df (dataFrame): The input DataFrame containing address data and associated flags.

    Returns:
    - df: A DataFrame that contains only the rows where all of the flags (excluding specific flags) are set to 0.
    
    Example:

    Suppose you have a dataframe `df` with several flag columns:
    +----------------+----------------------+-----------------------+------------------------+----------------------+
    | address        | punctuation_cleaned_flag | words_deduplicated_flag | postcodes_deduplicated_flag | other_flag |
    +----------------+----------------------+-----------------------+------------------------+----------------------+
    | MAIN ROAD      | 0                       | 0                      | 0                       | 0          |
    | HIGH STREET    | 1                       | 0                      | 0                       | 1          |
    | PARK AVENUE    | 0                       | 1                      | 0                       | 0          |
    +----------------+----------------------+-----------------------+------------------------+----------------------+

    After applying the function:
    df_filtered = filter_and_count_all_flags_zero(df)

    The df will contain:
    +----------------+----------------------+-----------------------+------------------------+----------------------+
    | address        | punctuation_cleaned_flag | words_deduplicated_flag | postcodes_deduplicated_flag | other_flag |
    +----------------+----------------------+-----------------------+------------------------+----------------------+
    | MAIN ROAD      | 0                       | 0                      | 0                       | 0          |
    +----------------+----------------------+-----------------------+------------------------+----------------------+

"""

def filter_and_count_all_flags_zero(df):
    from pyspark.sql.functions import col
    from functools import reduce

    exclusion_list = ["punctuation_cleaned_flag", "words_deduplicated_flag", "postcodes_deduplicated_flag", "postcodes_corrected_flag" , "noise_removed_flag", "changes_flag", "unwanted_characters_removed_flag",\
                     "street_type_standardised_flag", "patterns_identified_flag", "location_units_identified_flag", "unwanted_characters_removed_flag"]
    conditions = [col(column) == 0 for column in df.columns if column.endswith("_flag") and column not in exclusion_list]
    combined_condition = reduce(lambda x, y: x & y, conditions)
    filtered_df = df.filter(combined_condition)
    return filtered_df
  
#####################################################################

"""
    Function: filter_records_with_any_flags_set

    Purpose:
    This function processes the DataFrame by:
    1. Filtering the records based on flags.
    2. Excluding specified columns ("punctuation_cleaned_flag", "words_deduplicated_flag", "postcodes_deduplicated_flag") 
       from the flag-checking process.
    3. Checking for any flags set to 1 (excluding the aforementioned columns).
    4. Filtering the DataFrame to retain only rows where any of the checked flags are set to 1.

    Parameters:
    - df (DataFrame): The input DataFrame containing address data and associated flags.

    Returns:
    - df: A DataFrame that contains only the rows where any of the flags (excluding specific flags) are set to 1.
    
    Example:

    +----------------+----------------------+-----------------------+------------------------+----------------------+
    | address        | punctuation_cleaned_flag | words_deduplicated_flag | postcodes_deduplicated_flag | other_flag |
    +----------------+----------------------+-----------------------+------------------------+----------------------+
    | MAIN ROAD      | 0                       | 0                      | 0                       | 1          |
    | HIGH STREET    | 1                       | 0                      | 0                       | 0          |
    | PARK AVENUE    | 0                       | 1                      | 0                       | 0          |
    +----------------+----------------------+-----------------------+------------------------+----------------------+

    After applying the function:
    df_filtered = filter_records_with_any_flags_set(df)

    The DataFrame will contain:
    +----------------+----------------------+-----------------------+------------------------+----------------------+
    | address        | punctuation_cleaned_flag | words_deduplicated_flag | postcodes_deduplicated_flag | other_flag |
    +----------------+----------------------+-----------------------+------------------------+----------------------+
    | MAIN ROAD      | 0                       | 0                      | 0                       | 1          |
    | HIGH STREET    | 1                       | 0                      | 0                       | 0          |
    | PARK AVENUE    | 0                       | 1                      | 0                       | 0          |
    +----------------+----------------------+-----------------------+------------------------+----------------------+
"""


def filter_records_with_any_flags_set(df):
    from pyspark.sql.functions import col
    from functools import reduce

    exclusion_list = ["punctuation_cleaned_flag", "words_deduplicated_flag", "postcodes_deduplicated_flag", "postcodes_corrected_flag" , "noise_removed_flag", "changes_flag", "unwanted_characters_removed_flag",\
                     "street_type_standardised_flag", "patterns_identified_flag", "location_units_identified_flag", "unwanted_characters_removed_flag"]
    conditions = [col(column) == 1 for column in df.columns if column.endswith("_flag") and column not in exclusion_list]
    combined_condition = reduce(lambda x, y: x | y, conditions)
    flagged_df = df.filter(combined_condition)
    return flagged_df
  
##################################################################################

def process_df_precise(df: DataFrame, address_col: str) -> DataFrame:
    """
    Process a DataFrame containing address data using precise matching, with unwanted character removal 
    integrated into clean_punctuation. This function applies a series of steps to clean, validate, 
    and flag address records based on specific criteria.

    Parameters:
    -----------
    df : pyspark.sql.DataFrame
        The input DataFrame containing address data.
    address_col : str
        The name of the address column within the DataFrame.

    Returns:
    --------
    tuple
        - df : pyspark.sql.DataFrame
            The processed DataFrame with additional columns and flags indicating address validity.
        - df_all_flags_zero : pyspark.sql.DataFrame
            A DataFrame subset containing records where all relevant flags are zero.
        - df_any_flags_set : pyspark.sql.DataFrame
            A DataFrame subset containing records where at least one flag is set.

    Processing Steps:
    -----------------
    1. Convert address column to uppercase.
    2. Clean punctuation and remove unwanted characters.
    3. Remove noise words and flag changes.
    4. Process and deduplicate address words.
    5. Deduplicate UK postcodes, correct and standardize them.
    6. Standardise street types.
    7. Extract postcode and town from the address.
    8. Add length flag to indicate long/short addresses.
    9. Flag addresses with only country and postcode using precise matching.
    10. Flag addresses with only county and postcode using precise matching.
    11. Flag addresses with only town and postcode using precise matching.
    12. Check if certain keywords are present.
    13. Apply additional custom rule-based criteria.
    14. Flag addresses containing ZZ99 postcode (a placeholder code).
    15. Check if the country appears in the last half of the address.
    16. Flag addresses with invalid postcodes.
    """

    # Cache the DataFrame to avoid recomputing transformations multiple times
    df = df.cache()

    # Step 1: Convert address column to uppercase for uniformity in processing
    df = df.withColumn(address_col, upper(df[address_col]))

    # Step 2: Clean punctuation (with unwanted character removal integrated)
    df = clean_punctuation(df, address_col)
    punctuation_cleaned_count = df.filter(df.punctuation_cleaned_flag == 1).count()
    print(f"Punctuation cleaned count: {punctuation_cleaned_count}")

    # Step 3: Remove noise words (e.g., common but irrelevant words) with flag
    df = remove_noise_words_with_flag(df)
    noise_words_removed_count = df.filter(df.noise_removed_flag == 1).count()
    print(f"Noise words removed count: {noise_words_removed_count}")

    # Cache the DataFrame again after significant transformations
    df = df.cache()

    # Step 4: Process and deduplicate address to ensure uniqueness of address components
    process_udf = get_process_and_deduplicate_address_udf()
    df = df.withColumn("output", process_udf(col("final_cleaned_address")))
    df = df.withColumn("final_cleaned_address", col("output.cleaned_address")) \
           .withColumn("words_deduplicated_flag", col("output.words_deduplicated_flag")) \
           .drop("output")
    address_deduplicated_count = df.filter(df.words_deduplicated_flag == 1).count()
    print(f"Address deduplicated count: {address_deduplicated_count}")

    # Step 5: Deduplicate UK postcodes, correct and standardize them
    deduplicate_postcodes = deduplicate_postcodes_udf()
    df = df.withColumn("output", deduplicate_postcodes(col("final_cleaned_address")))
    df = df.withColumn("final_cleaned_address", col("output.final_cleaned_address")) \
           .withColumn("postcodes_deduplicated_flag", col("output.changes_flag")) \
           .drop("output")
    postcodes_deduplicated_count = df.filter(df.postcodes_deduplicated_flag == 1).count()
    print(f"Postcodes deduplicated count: {postcodes_deduplicated_count}")

    # Step 6: Standardise street types (e.g., converting "St" to "Street")
    df = standardise_street_types(df, "final_cleaned_address")
    street_type_standardised_count = df.filter(df.street_type_standardised_flag == 1).count()
    print(f"Street type standardised count: {street_type_standardised_count}")

    # Cache the DataFrame again after deduplication and street standardization
    df = df.cache()

    # Step 7: Extract postcode, town, and clean address lines
    df = extract_postcode_town_address(df, "final_cleaned_address")

    # Step 8: Add length flag for addresses that are either too long or too short
    df = add_length_flag(df, "final_cleaned_address")
    length_affected_count = df.filter(df.length_flag == 1).count()
    print(f"Flagged by length: {length_affected_count}")

    # Step 9: Just country postcode (precise)
    df = just_country_postcode(df, "final_cleaned_address")
    country_postcode_affected_count = df.filter(df.just_country_postcode_flag == 1).count()
    print(f"Number of addresses flagged for only having country and postcode: {country_postcode_affected_count}")

    # Step 10: Just county postcode (precise)
    df = just_county_postcode(df, "final_cleaned_address")
    county_postcode_affected_count = df.filter(df.just_county_postcode_flag == 1).count()
    print(f"Number of addresses flagged for only having county and postcode: {county_postcode_affected_count}")

    # Step 11: Just town postcode (precise)
    df = just_town_postcode(df, "final_cleaned_address")
    town_postcode_affected_count = df.filter(df.just_town_postcode_flag == 1).count()
    print(f"Number of addresses flagged for only having town and postcode: {town_postcode_affected_count}")

    # Step 12: Check if specific keywords are present in the address
    df = keyword_present(df, "final_cleaned_address")
    keyword_affected_count = df.filter(df.keyword_flag == 1).count()
    print(f"Flagged by keywords: {keyword_affected_count}")

    # Step 13: Apply rule-based criteria to flag addresses meeting all 3 conditions
    df = all_3_criteria(df, "final_cleaned_address")
    criteria_affected_count = df.filter(df.criteria_flag == 1).count()
    print(f"Number of addresses flagged based on 3 criteria: {criteria_affected_count}")

    # Step 14: Flag addresses containing ZZ99 (a placeholder postcode)
    df = has_country_and_ZZ99(df, "final_cleaned_address")
    country_and_ZZ99_affected_count = df.filter(df.country_postcode_flag == 1).count()
    print(f"Number of addresses flagged for country and ZZ99: {country_and_ZZ99_affected_count}")

    # Step 15: Check if the country appears in the last half of the address
    df = country_in_last_half(df, "final_cleaned_address")
    country_position_affected_count = df.filter(df.country_position_flag == 1).count()
    print(f"Number of addresses with country in last half: {country_position_affected_count}")

    # Step 16: Flag addresses with invalid postcodes
    df = is_invalid_postcode(df, "final_cleaned_address")
    invalid_postcode_affected_count = df.filter(df.invalid_postcode_flag == 1).count()
    print(f"Number of addresses with invalid postcodes: {invalid_postcode_affected_count}")

    # Cache the DataFrame one last time before the final aggregation
    df = df.cache()

    # Final output: filter for records where all flags are zero and where any flag is set
    df_all_flags_zero = filter_and_count_all_flags_zero(df)
    df_any_flags_set = filter_records_with_any_flags_set(df)
    
    all_flags_zero_count = df_all_flags_zero.count()
    any_flags_set_count = df_any_flags_set.count()
    
    print(f"Number of records where all flags (excluding specific flags) are 0: {all_flags_zero_count}")
    print(f"Number of records where any flag (excluding specific flags) is set: {any_flags_set_count}")

    return df, df_all_flags_zero, df_any_flags_set

#################################################################################


def process_df_default(df: DataFrame, address_col: str) -> DataFrame:
    """
    Processes a DataFrame containing address data using exact matching, with unwanted character 
    removal integrated into clean_punctuation. This function applies a series of steps to clean, 
    validate, and flag address records based on specific criteria, including exact postcode matching.

    Parameters:
    ----------- 
    df : pyspark.sql.DataFrame 
        The input DataFrame containing address data. 
    address_col : str 
        The name of the address column within the DataFrame.

    Returns:
    -------- 
    tuple 
        - df : pyspark.sql.DataFrame 
            The processed DataFrame with additional columns and flags indicating address validity. 
        - df_all_flags_zero : pyspark.sql.DataFrame 
            A DataFrame subset containing records where all relevant flags are zero. 
        - df_any_flags_set : pyspark.sql.DataFrame 
            A DataFrame subset containing records where at least one flag is set.
    """

    # Reintroducing cache to optimise performance
    df = df.cache()

    # Step 1: Convert address column to uppercase for uniformity in processing
    df = df.withColumn(address_col, upper(df[address_col]))

    # Step 2: Clean punctuation (with unwanted character removal integrated)
    df = clean_punctuation(df, address_col)
    punctuation_cleaned_count = df.filter(df.punctuation_cleaned_flag == 1).count()
    print(f"Punctuation cleaned count: {punctuation_cleaned_count}")

    # Step 3: Remove noise words (e.g., common but irrelevant words) with flag
    df = remove_noise_words_with_flag(df)
    noise_words_removed_count = df.filter(df.noise_removed_flag == 1).count()
    print(f"Noise words removed count: {noise_words_removed_count}")

    # Cache the DataFrame again after significant transformations
    df = df.cache()

    # Step 4: Process and deduplicate address to ensure uniqueness of address components
    process_udf = get_process_and_deduplicate_address_udf()
    df = df.withColumn("output", process_udf(col("final_cleaned_address")))
    df = df.withColumn("final_cleaned_address", col("output.cleaned_address")) \
           .withColumn("words_deduplicated_flag", col("output.words_deduplicated_flag")) \
           .drop("output")
    address_deduplicated_count = df.filter(df.words_deduplicated_flag == 1).count()
    print(f"Address deduplicated count: {address_deduplicated_count}")

    # Step 5: Deduplicate UK postcodes, correct and standardise them
    deduplicate_postcodes = deduplicate_postcodes_udf()
    df = df.withColumn("output", deduplicate_postcodes(col("final_cleaned_address")))
    df = df.withColumn("final_cleaned_address", col("output.final_cleaned_address")) \
           .withColumn("postcodes_deduplicated_flag", col("output.changes_flag")) \
           .drop("output")
    postcodes_deduplicated_count = df.filter(df.postcodes_deduplicated_flag == 1).count()
    print(f"Postcodes deduplicated count: {postcodes_deduplicated_count}")

    # Step 6: Standardise street types (e.g., converting "St" to "Street")
    df = standardise_street_types(df, "final_cleaned_address")
    street_type_standardised_count = df.filter(df.street_type_standardised_flag == 1).count()
    print(f"Street type standardised count: {street_type_standardised_count}")

    # Cache the DataFrame again after deduplication and street standardisation
    df = df.cache()

    # Step 7: Extract postcode, town, and clean address lines
    df = extract_postcode_town_address(df, "final_cleaned_address")

    # Step 8: Add length flag for addresses that are either too long or too short
    df = add_length_flag(df, "final_cleaned_address")
    length_affected_count = df.filter(df.length_flag == 1).count()
    print(f"Flagged by length: {length_affected_count}")

    # Step 9: Just country postcode exact
    df = just_country_postcode_exact(df, "final_cleaned_address")
    country_postcode_affected_count = df.filter(df.just_country_postcode_flag == 1).count()
    print(f"Number of addresses flagged for only having country and postcode: {country_postcode_affected_count}")

    # Step 10: Just county postcode exact
    df = just_county_postcode_exact(df, "final_cleaned_address")
    county_postcode_affected_count = df.filter(df.just_county_postcode_flag == 1).count()
    print(f"Number of addresses flagged for only having county and postcode: {county_postcode_affected_count}")

    # Step 11: Just town postcode exact
    df = just_town_postcode_exact(df, "final_cleaned_address")
    town_postcode_affected_count = df.filter(df.just_town_postcode_flag == 1).count()
    print(f"Number of addresses flagged for only having town and postcode: {town_postcode_affected_count}")

    # Step 12: Check if specific keywords are present in the address
    df = keyword_present(df, "final_cleaned_address")
    keyword_affected_count = df.filter(df.keyword_flag == 1).count()
    print(f"Flagged by keywords: {keyword_affected_count}")

    # Step 13: Apply rule-based criteria to flag addresses meeting all 3 conditions
    df = all_3_criteria(df, "final_cleaned_address")
    criteria_affected_count = df.filter(df.criteria_flag == 1).count()
    print(f"Number of addresses flagged based on 3 criteria: {criteria_affected_count}")

    # Step 14: Flag addresses containing ZZ99 (a placeholder postcode)
    df = has_country_and_ZZ99(df, "final_cleaned_address")
    country_and_ZZ99_affected_count = df.filter(df.country_postcode_flag == 1).count()
    print(f"Number of addresses flagged for country and ZZ99: {country_and_ZZ99_affected_count}")

    # Step 15: Check if the country appears in the last half of the address
    df = country_in_last_half(df, "final_cleaned_address")
    country_position_affected_count = df.filter(df.country_position_flag == 1).count()
    print(f"Number of addresses with country in last half: {country_position_affected_count}")

    # Step 16: Flag addresses with invalid postcodes
    df = is_invalid_postcode(df, "final_cleaned_address")
    invalid_postcode_affected_count = df.filter(df.invalid_postcode_flag == 1).count()
    print(f"Number of addresses with invalid postcodes: {invalid_postcode_affected_count}")

    # Cache the DataFrame one last time before the final aggregation
    df = df.cache()

    # Final output: filter for records where all flags are zero and where any flag is set
    df_all_flags_zero = filter_and_count_all_flags_zero(df)
    df_any_flags_set = filter_records_with_any_flags_set(df)
    
    all_flags_zero_count = df_all_flags_zero.count()
    any_flags_set_count = df_any_flags_set.count()
    
    print(f"Number of records where all flags (excluding specific flags) are 0: {all_flags_zero_count}")
    print(f"Number of records where any flag (excluding specific flags) is set: {any_flags_set_count}")

    return df, df_all_flags_zero, df_any_flags_set