import pandas as pd
import re
from collections import Counter
from functools import reduce

from fuzzywuzzy import fuzz
import openpyxl
import xlrd

import pyspark.sql.functions as F
from pyspark.sql.functions import udf, regexp_replace, upper, col, when, length, split, regexp_extract, trim
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, IntegerType, StructType, StructField

from address_functions.config.settings import (
    town_list, alternative_town_list, 
    allowed_country_list, disallowed_country_list, county_list
)

#################################################################################

"""
Adds a 'length_flag' column to the DataFrame based on the length of the specified column.

This function checks the length of the content in the specified column. If the length 
of the content is less than 10 characters or greater than 150 characters, the 'length_flag'
column is set to 1, otherwise it's set to 0.

Parameters:
- df (DataFrame): The input DataFrame.
- column_name (str): The name of the column whose length will be checked. 
                     Defaults to "final_cleaned_address".

Returns:
DataFrame: The DataFrame with the added 'length_flag' column.

Examples:
--------
+---------------------------------------------+-------------+
| final_cleaned_address                       | length_flag |
+---------------------------------------------+-------------+
| 123 MAIN ST, NY                             |           0 |
| UNK                                         |           1 |
| 1001 CENTRAL AVE, CA 90001                  |           0 |
| MOVED ADDRESS, LOS ANGELES, CA 90210        |           0 |
| VERY LONG ADDRESS THAT EXCEEDS...           |           1 |
+---------------------------------------------+-------------+

"""

def add_length_flag(df, column_name="final_cleaned_address"):
    df = df.withColumn("length_flag", 
                       when((length(col(column_name)) < 10) | 
                            (length(col(column_name)) > 150), 1).otherwise(0))
    return df

###################################################################################

# just town and postcode flag

def fuzzy_match_town(town, valid_towns):
    """
    Evaluates if a given town name closely matches any town name in a predefined list of valid towns using fuzzy matching.
    A match is considered valid if the similarity score is 90 or above.

    Parameters:
    - town (str): The town name to check for a match.
    - valid_towns (list of str): A list of valid town names to match against.

    Returns:
    - bool: True if a match is found with a similarity score of 90 or above; otherwise, False.
    """
    
    for valid_town in valid_towns:
        if fuzz.ratio(town, valid_town) >= 90:
            return True
    return False

def just_town_postcode(df: DataFrame, address_col: str = "final_cleaned_address") -> DataFrame:
    """
    Flags DataFrame records that contain only a town name from a predefined list and a valid UK postcode,
    excluding the special case 'ZZ99'. It leverages fuzzy matching to determine if the town name in each
    address matches any name in the predefined list with a high degree of similarity.

    Parameters:
    - df (DataFrame): The input DataFrame containing address data.
    - address_col (str, optional): The column name in df that contains the address information. Defaults to "final_cleaned_address".

    Returns:
    - DataFrame: The DataFrame with an additional column 'just_town_postcode_flag'. This flag is set to 1 for records
      where the address contains a valid town name and postcode according to the specified criteria, and 0 otherwise.

    Note:
    The function requires a predefined list of valid towns (`town_list`) and utilises the `fuzzy_match_town` function
    to perform fuzzy matching on town names. It assumes the presence of a valid UK postcode regex pattern for postcode validation.
    """
    # UK postcode pattern to validate postcode format
    uk_postcode_pattern = r"([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z]\d{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y]\d{1,2})|(([A-Za-z]\d[A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y]\d[A-Za-z]?))))\s?\d[A-Za-z]{2})"

    # List of valid towns for matching
    valid_towns = town_list  

    # Define a UDF for fuzzy matching towns against the valid towns list
    fuzzy_match_town_udf = udf(lambda x: fuzzy_match_town(x, valid_towns), IntegerType())

    # Split address into town and postcode components for analysis
    split_address = F.split(col(address_col), ", ")
    town = split_address.getItem(0)
    postcode = split_address.getItem(1)

    # Determine valid address flag based on town match and postcode criteria
    valid_address_flag = F.when(
        (fuzzy_match_town_udf(town) == 1) &  # Town fuzzy match check
        (F.regexp_extract(postcode, uk_postcode_pattern, 0) != "") &  # Postcode format validation
        (~postcode.contains("ZZ99")),  # Exclude 'ZZ99' special case
        1).otherwise(0)

    # Add the 'just_town_postcode_flag' column to the DataFrame
    df = df.withColumn("just_town_postcode_flag", valid_address_flag)

    return df
  
###################################################################################

def just_town_postcode_exact(df: DataFrame, address_col: str = "final_cleaned_address") -> DataFrame:
    """
    Identifies and flags addresses in a DataFrame that precisely contain a town name from a predefined list 
    and a valid UK postcode, explicitly excluding any addresses containing the special postcode 'ZZ99'. 
    This exact matching version does not use fuzzy logic, instead it requires an exact match with the town 
    names in the predefined list, which is in config/settings.py


    Parameters:
    - df (DataFrame): The input DataFrame containing address data.
    - address_col (str, optional): The column name in the DataFrame that contains the address information. 
      Defaults to "final_cleaned_address".

    Returns:
    - DataFrame: The original DataFrame augmented with a new column 'just_town_postcode_flag'. This column 
      contains a flag set to 1 for records that match the criteria (valid town name from the list and a 
      valid UK postcode excluding 'ZZ99') and 0 for all other records.

    Note:
    - The UK postcode pattern is defined to match standard UK postcode formats, excluding the placeholder 
      postcode 'ZZ99' often used for anonymisation or testing purposes.
    """
    # Define the UK postcode pattern to validate the format
    uk_postcode_pattern = r"([A-Za-z]{1,2}\d{1,2}\s*\d[A-Za-z]{2}|\w{1,2}\d\w\s*\d\w{2})"

    # Split the address into town and postcode components for analysis
    split_address = split(col(address_col), ", ")
    town = split_address.getItem(0)
    postcode = split_address.getItem(1)

    # Check if the town is in the predefined list and if the postcode matches the UK format, excluding 'ZZ99'
    valid_address_flag = F.when(
        town.isin(town_list) &  # Exact match check against predefined list of towns
        (regexp_extract(postcode, uk_postcode_pattern, 0) != "") &  # Postcode format validation
        (~postcode.contains("ZZ99")),  # Exclude 'ZZ99' special case
        1  # Flag as valid
    ).otherwise(0)  # Flag as not valid

    # Add the 'just_town_postcode_flag' column to the DataFrame
    df = df.withColumn("just_town_postcode_flag", valid_address_flag)

    return df


###################################################################################

# just country and postcode

def fuzzy_match(country, valid_countries):
    """
    Performs fuzzy matching to determine if a given country name closely matches any country name in a predefined list of valid countries. A match is considered valid if the similarity score is 90 or above.

    Parameters:
    - country (str): The country name to check for a match.
    - valid_countries (list of str): A list of valid country names for matching.

    Returns:
    - bool: True if a match is found with a similarity score of 90 or above; otherwise, False.
    """
    for valid_country in valid_countries:
        if fuzz.ratio(country, valid_country) >= 90:
            return True
    return False

def just_country_postcode(df: DataFrame, address_col: str = "final_cleaned_address") -> DataFrame:
    """
    Identifies and flags addresses in a DataFrame that contain only a country name from a predefined list and a valid UK postcode, explicitly excluding addresses with the special postcode 'ZZ99'. This function employs fuzzy matching for country names to allow for minor variations or typos in the country field.

    Parameters:
    - df (DataFrame): The input DataFrame containing address data.
    - address_col (str, optional): The column name in the DataFrame that contains the address information. Defaults to "final_cleaned_address".

    Returns:
    - DataFrame: The original DataFrame augmented with a new column 'just_country_postcode_flag'. This column contains a flag set to 1 for records that match the criteria (valid country name from the list and a valid UK postcode excluding 'ZZ99') and 0 for all other records.

    Note:
    - This function requires a predefined list `allowed_country_list` containing valid country names for fuzzy matching. This is in config/settings.py
    - The UK postcode pattern is defined to match standard UK postcode formats, excluding the 'ZZ99' postcode, which is often used for anonymisation or testing purposes.
    """
    # Define the UK postcode pattern to validate the format
    uk_postcode_pattern = r"([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z]\d{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y]\d{1,2})|(([A-Za-z]\d[A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y]\d[A-Za-z]?))))\s?\d[A-Za-z]{2})"

    # List of valid countries for fuzzy matching
    valid_countries = allowed_country_list  # Ensure `allowed_country_list` is defined elsewhere in your code

    # Define a UDF for fuzzy matching country names against the valid countries list
    fuzzy_match_udf = udf(lambda x: fuzzy_match(x, valid_countries), IntegerType())

    # Split the address into country and postcode components for analysis
    split_address = split(col(address_col), ", ")
    country = split_address.getItem(0)
    postcode = split_address.getItem(1)

    # Determine valid address flag based on fuzzy match for country and postcode criteria
    valid_address_flag = F.when(
        (fuzzy_match_udf(country) == 1) &  # Country fuzzy match check
        (regexp_extract(postcode, uk_postcode_pattern, 0) != "") &  # Postcode format validation
        (~postcode.contains("ZZ99")),  # Exclude 'ZZ99' special case
        1  # Flag as valid
    ).otherwise(0)  # Flag as not valid

    # Add the 'just_country_postcode_flag' column to the DataFrame
    df = df.withColumn("just_country_postcode_flag", valid_address_flag)

    return df

###################################################################################

def just_country_postcode_exact(df: DataFrame, address_col: str = "final_cleaned_address") -> DataFrame:
    """
    Flags addresses within a DataFrame that exactly match a country from a predefined list (sourced from `config.settings.py`)
    and a valid UK postcode, excluding any with 'ZZ99'. This function is essential for filtering datasets to ensure addresses meet specific geographic criteria.

    Parameters:
    - df (DataFrame): The input DataFrame to be processed.
    - address_col (str, optional): The name of the column containing address information to be evaluated. Defaults to "final_cleaned_address".

    Returns:
    - DataFrame: The original DataFrame augmented with a 'just_country_postcode_flag' column. 

    Note:
    - This function relies on a list of allowed countries defined in `config.settings.py`.
    - It strictly validates against UK postcode patterns to exclude entries using the placeholder 'ZZ99', typically used for anonymisation or testing.
    """
    # Define the UK postcode pattern for validation
    uk_postcode_pattern = r"([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z]\d{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y]\d{1,2})|(([A-Za-z]\d[A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y]\d[A-Za-z]?))))\s?\d[A-Za-z]{2})"

    # Split the address column into country and postcode for validation
    split_address = split(col(address_col), ", ")
    country = split_address.getItem(0)
    postcode = split_address.getItem(1)

    # Execute validation against predefined criteria
    valid_address_flag = F.when(
        country.isin(allowed_country_list) &  # Match against allowed country list
        (regexp_extract(postcode, uk_postcode_pattern, 0) != "") &  # Validate postcode format
        (~postcode.contains("ZZ99")),  # Exclude 'ZZ99' postcodes
        1  # Flag as valid
    ).otherwise(0)  # Flag as invalid

    # Update DataFrame with validation flags
    df = df.withColumn("just_country_postcode_flag", valid_address_flag)

    return df

###################################################################################

# just county and postcode

def fuzzy_match_county(county, valid_counties):
    # Define a function to perform fuzzy matching
    for valid_county in valid_counties:
        if fuzz.ratio(county, valid_county) >= 90:
            return True
    return False

def just_county_postcode(df: DataFrame, address_col: str = "final_cleaned_address") -> DataFrame:
    """
    flag addresses that contain only a county from a predefined list and a UK postcode (excluding ZZ99).

    This function takes a DataFrame and the name of the address column.
    It adds a new column 'just_county_postcode_flag' with values 1 (valid) or 0 (not valid) based on the criteria.

    Parameters:
    - df (DataFrame): The input DataFrame.
    - address_col (str, optional): The name of the address column. Default is "final_cleaned_address".

    Returns:
    - df: A new DataFrame with the 'just_county_postcode_flag' column added.
    """
    # our Uk postcode
    uk_postcode_pattern = r"([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z]\d{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y]\d{1,2})|(([A-Za-z]\d[A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y]\d[A-Za-z]?))))\s?\d[A-Za-z]{2})"

    # making it a python list
    valid_counties = county_list

    # creating a UDF to apply fuzzy_match_county
    fuzzy_match_county_udf = F.udf(lambda x: fuzzy_match_county(x, valid_counties), IntegerType())

    # split the string into county and postcode
    split_address = F.split(F.col(address_col), ", ")
    county = split_address.getItem(0)
    postcode = split_address.getItem(1)

    # check if the county is similar to any county in our county_list and the postcode matches the UK format and does not contain "ZZ99"
    valid_address_flag = F.when(
        (fuzzy_match_county_udf(county) == 1) &
        (F.regexp_extract(postcode, uk_postcode_pattern, 0) != "") &
        (~postcode.contains("ZZ99")), 1).otherwise(0)

    # add the 'just_county_postcode_flag' column to the df
    df = df.withColumn("just_county_postcode_flag", valid_address_flag)

    return df
###################################################################################

def just_county_postcode_exact(df: DataFrame, address_col: str = "final_cleaned_address") -> DataFrame:
    """
    flag addresses that contain only a county from a predefined list and a UK postcode (excluding ZZ99).

    This function takes a DataFrame and the name of the address column.
    It adds a new column 'just_county_postcode_flag' with values 1 (valid) or 0 (not valid) based on the criteria.

    Parameters:
    - df (DataFrame): The input DataFrame.
    - address_col (str, optional): The name of the address column. Default is "final_cleaned_address".

    Returns:
    - df: A new DataFrame with the 'just_county_postcode_flag' column added.
    """
    # Our Uk postcode pattern
    uk_postcode_pattern = r"([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z]\d{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y]\d{1,2})|(([A-Za-z]\d[A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y]\d[A-Za-z]?))))\s?\d[A-Za-z]{2})"

    # splitting the address into county and postcode
    split_address = F.split(F.col(address_col), ", ")
    county = split_address.getItem(0)
    postcode = split_address.getItem(1)

    # check if the county is in the list of valid counties and the postcode matches our postcode_regex and does not contain "ZZ99"
    valid_address_flag = F.when(
        (county.isin(county_list)) &
        (F.regexp_extract(postcode, uk_postcode_pattern, 0) != "") &
        (~postcode.contains("ZZ99")), 1).otherwise(0)

    # adding 'just_county_postcode_flag' column to our df
    df = df.withColumn("just_county_postcode_flag", valid_address_flag)

    return df

###################################################################################

# i've recently removed "YYYY" and "ZZZZ" from this list and have instead added it as a cleaning step in
# "remove_noise_words_with_flag"
def keyword_present(df, input_col="final_cleaned_address"):
    """
    Checks for the presence of specific keywords within a designated column of a DataFrame and 
    flags entries containing these keywords.

    The function searches for a predefined list of keywords and special keywords within each 
    address string. If any of the keywords are found in the address, the function flags that 
    entry as a "1"

    Parameters:
    - df (DataFrame): A Spark DataFrame that includes a column with address or similar strings.
    - input_col (str, optional): The name of the column to search for keywords. Defaults to 'final_cleaned_address'.

    Returns:
    - DataFrame: A modified Spark DataFrame that includes a new 'keyword_flag' column, where entries
                 are flagged with 1 if a keyword is present, and 0 otherwise.

    Example Output:
    +---------------------------+--------------------------------+-------------+
    | Original Address          | Keywords Searched              | Keyword Flag |
    +---------------------------+--------------------------------+-------------+
    | 123 Main St, LOST CONTACT | LOST CONTACT, UNKNOWN ADDRESS  | 1           |
    | 456 Elm St, City          |                                | 0           |
    +---------------------------+--------------------------------+-------------+
    """
  
    keyword_list = ["LOST CONTACT", "ADDRESS NOT FOUND",
                    "MOVED ADDRESS", "CHILD HAS MOVED", "LOST CONTACT", "CHILD HAS NOW",
                    "ADDRESS NOT KNOWN", "MOVED ABROAD", "MOVED OUT OF", "NO TRACE", 
                    "UNKNOWN ADDRESS", "NOT KNOWN", "NOT GIVEN",
                    "0, 0, 0", "Z Z, Z", "WHEREABOUTS UNKNOWN"]
    special_keywords = ["UNKNOWN", "UNK", "ANK", "ABROAD", "NOT APPLICABLE"]
    
    def check_keywords(address):
        half_length = len(address) // 2
        first_half = address[:half_length]

        for keyword in special_keywords:
            if (address.startswith(f"{keyword} ") or 
                address.startswith(f"{keyword},") or 
                f" {keyword} " in first_half or 
                f" {keyword}," in first_half):
                return 1

        for keyword in keyword_list: 
            if keyword not in special_keywords and keyword in address:
                return 1
        return 0
    
    keyword_present_udf = udf(check_keywords, IntegerType())
    df = df.withColumn("keyword_flag", keyword_present_udf(col(input_col)))
    return df

##################################################################################


def all_3_criteria(df, input_col="final_cleaned_address"):
    """
    Check if a given address in a dataframe column contains a country name and the string "ZZ99" in its latter half.
    
    Parameters:
    ----------
    df : dataFrame
        The input dataframe that needs to be checked.
    
    input_col : str, optional
        The name of the column in the dataframe that contains the address strings. 
        Default is "final_cleaned_address".
    
    Returns:
    -------
    df : pyspark df
        The df with an additional column named "criteria_flag". This column will 
        have a value of `1` for each row where the latter half of the corresponding value 
        in the `input_col` contains both a country name and "ZZ99", otherwise the value will be `0`.
    
    Example:
    --------
    If the input dataframe looks like:
        +---------------------------+
        | final_cleaned_address     |
        +---------------------------+
        | A street AFGHANISTAN ZZ99 |
        | USA Texas                 |
        | A road CHINA ZZ99         |
        | INDIA Delhi               |
        +---------------------------+
    
    The returned dataframe will look like:
        +---------------------------+--------------+
        | final_cleaned_address     | criteria_flag|
        +---------------------------+--------------+
        | A street AFGHANISTAN ZZ99 |             1|
        | USA Texas                 |             0|
        | A road CHINA ZZ99         |             1|
        | INDIA Delhi               |             0|
        +---------------------------+--------------+
    """

    def check_criteria(s):
        """
        Nested function to check if the latter half of a string contains any country from the country_list and "ZZ99".
        
        Parameters:
        ----------
        s : str
            The input string to check.
        
        Returns:
        -------
        int : 
            Returns `1` if the latter half of the string contains both a country name and "ZZ99", otherwise returns `0`.
        """
        midpoint = len(s) // 2
        last_half = s[midpoint:]
        has_country_in_last_half = any(country in last_half for country in disallowed_country_list)
        
        return 1 if has_country_in_last_half and "ZZ99" in last_half else 0
    
    all_3_criteria_udf = udf(check_criteria, IntegerType())
    df = df.withColumn("criteria_flag", all_3_criteria_udf(col(input_col)))

    return df

###################################################################################

def has_country_and_ZZ99(df, input_col="final_cleaned_address"):
    """
    checks if a given address in a dataframe column contains both a country name and the string "ZZ99".
    
    parameters:
    ----------
    df : dataframe
        The input dataframe that needs to be checked.
    
    input_col : str, optional
        The name of the column in the dataframe that contains the address strings. 
        default is "final_cleaned_address".
    
    Returns:
    -------
    df : dataframe
        The dataframe with an additional column named "country_postcode_flag". This column will 
        have a value of `1` for each row where the corresponding value in the `input_col` contains 
        both a country name and "ZZ99", otherwise the value will be `0`.
    
    Example:
    --------
    If the input dataframe looks like:
        +-------------------------+
        | final_cleaned_address   |
        +-------------------------+
        | AFGHANISTAN ZZ99 STREET |
        | USA TEXAS               |
        | CHINA ZZ99 ROAD         |
        | INDIA DELHI             |
        +-------------------------+
    
    The returned dataframe will look like:
        +-------------------------+--------------------+
        | final_cleaned_address   | country_postcode_flag |
        +-------------------------+--------------------+
        | AFGHANISTAN ZZ99 STREET |                    1 |
        | USA TEXAS               |                    0 |
        | CHINA ZZ99 ROAD         |                    1 |
        | INDIA DELHI             |                    0 |
        +-------------------------+--------------------+
    """
    
    def check_country_and_postcode(s):
        """
        Nested function to check if a string contains any country from the country_list and "ZZ99".
        
        Parameters:
        ----------
        s : str
            The input string to check.
        
        Returns:
        -------
        int : 
            Returns `1` if the string contains both a country name and "ZZ99", otherwise returns `0`.
        """
        has_country = any(country in s for country in disallowed_country_list)
        if has_country and "ZZ99" in s:
            return 1
        return 0
    
    country_postcode_udf = udf(check_country_and_postcode, IntegerType())
    df = df.withColumn("country_postcode_flag", country_postcode_udf(col(input_col)))

    return df
  
  
###################################################################################

def country_in_last_half(df, input_col="final_cleaned_address"):
    """
    Flags records where the word 'INDIA' appears by itself in the last half of the address.
    The word 'INDIA' is flagged only when it is surrounded by commas or at the end of the address,
    but not when it is part of a larger name (e.g., 'EAST INDIA COURT').

    Parameters:
    -----------
    df : pyspark.sql.DataFrame
        The input DataFrame containing the address data.
    input_col : str, optional
        The name of the column in the DataFrame that holds the full address. Default is "final_cleaned_address".

    Returns:
    --------
    pyspark.sql.DataFrame
        A DataFrame with an additional column 'country_position_flag' that flags if 'INDIA' is found alone in the last half of the address.
    """
    
    def check_country_in_last_half(s):
        # Define the specific country we are looking for ("INDIA")
        target_country = "INDIA"
        
        # Split the address into two halves
        midpoint = len(s) // 2
        last_half = s[midpoint:]
        
        # Check if 'INDIA' is in the last half, but only as a standalone word, between commas or at the end
        match = re.search(rf"(,?\s*\b{target_country}\b\s*,?|,\s*\b{target_country}\b$)", last_half, re.IGNORECASE)
        
        if match:
            # Ensure it's not part of a larger name (e.g., 'EAST INDIA COURT')
            full_match = match.group(0).strip().upper()
            # If "INDIA" is followed by "COURT", "ROAD", or "STREET", it should not be flagged
            if "COURT" not in full_match and "ROAD" not in full_match and "STREET" not in full_match:
                return 1
        
        return 0

    # Define the UDF to apply the check function
    country_position_udf = udf(check_country_in_last_half, IntegerType())

    # Apply the UDF to create the flag
    df = df.withColumn("country_position_flag", country_position_udf(col(input_col)))

    return df

###################################################################################

"""

    Function: is_invalid_postcode

    Purpose:
    This function processes the df to identify records with invalid UK postcodes. 
    An invalid postcode is one that doesn't conform to the standard UK postcode format or contains a "ZZ99" pattern, which is often used as a placeholder for missing data.

    Processing Stages:
    1. Define a regex pattern to identify standard UK postcodes.
    2. Check if each address matches the regular expression and doesn't contain "ZZ99".
    3. Flag records with invalid postcodes.

    Parameters:
    - df (dataFrame): The input DataFrame containing address data.
    - input_col (String): The name of the column containing addresses to be checked for invalid postcodes. Default is "final_cleaned_address".

    Returns:
    - df: A df with an additional "invalid_postcode_flag" column, where a flag value of 1 indicates an invalid postcode and a value of 0 indicates a valid one.

    Example:

    Suppose you have a dataframe `df` with a column "address" containing:
    +-----------------------+
    | address               |
    +-----------------------+
    | 12 MAIN ST, ZZ99 1AB  |
    | 45 HIGH RD, NW1 5GH   |
    | PARK AVE, E2 6JJ      |
    +-----------------------+

    after applying the function:
    df_processed = is_invalid_postcode(df, "address")

    The resulting DataFrame will be:
    +-----------------------+---------------------+
    | final_cleaned_address | invalid_postcode_flag|
    +-----------------------+---------------------+
    | 12 MAIN ST, ZZ99 1AB  | 1                   |
    | 45 HIGH RD, NW1 5GH   | 0                   |
    | 46 HIGH RD, NW1       | 1                   |    
    | PARK AVE, E2 6JJ      | 0                   |
    +-----------------------+---------------------+

"""

# Main function to check for invalid postcodes and to reformat if necessary
def is_invalid_postcode(df, input_col="final_cleaned_address"):
    # Modified regex to account for 0 or 1 space where the space should be in the UK postcode
    postcode_regex = r"([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z]\d{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y]\d{1,2})|(([A-Za-z]\d[A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y]\d[A-Za-z]?))))\s?\d[A-Za-z]{2})"

    def check_invalid_postcode(address):
        import re
        normalised_address = re.sub(r"\s+", "", address)
        if re.search(postcode_regex, normalised_address) and "ZZ99" not in normalised_address:
            return 0
        return 1

    invalid_postcode_udf = udf(check_invalid_postcode, IntegerType())
    df = df.withColumn("invalid_postcode_flag", invalid_postcode_udf(col(input_col)))

    return df