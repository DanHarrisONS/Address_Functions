import re

from pyspark.sql import SparkSession, DataFrame, udf
from pyspark.sql.functions import col, regexp_extract, regexp_replace, when, trim, upper, concat_ws, lit, udf
from pyspark.sql.types import StringType
from address_functions.config.settings import town_list

def extract_postcode_town_address(df: DataFrame, address_col: str = "final_cleaned_address") -> DataFrame:
    """
    Extracts the postcode and town from the given address column, and cleans the address by removing 
    the extracted postcode and town. This function helps in separating key address components like 
    postcode and town while leaving behind a cleaned address without these elements.

    Parameters:
    -----------
    df : pyspark.sql.DataFrame
        The input DataFrame containing the address data.
    address_col : str, optional
        The name of the column in the DataFrame that holds the full address. Default is "final_cleaned_address".

    Returns:
    --------
    pyspark.sql.DataFrame
        A DataFrame with the following additional columns:
        - postcode : str
            The extracted postcode from the address.
        - town : str
            The extracted town name from the address.
        - address_lines : str
            The cleaned address with the postcode and town removed.

    Processing Steps:
    -----------------
    1. **Extract Postcode**: 
        - Uses a regular expression to extract valid UK postcodes from the address column.
        - The postcode regex pattern matches a variety of valid UK postcode formats.
    
    2. **Extract Town**:
        - First, it attempts to match town names using a predefined list (`town_list`) of known towns.
        - If no match is found in the predefined list, a fallback regex is applied to extract the town from the address.
        - The town regex ensures that the extracted town is not part of a street name or other irrelevant address components.

    3. **Clean Address**:
        - Removes the extracted postcode and town from the address column.
        - Cleans up any excess commas, spaces, and other punctuation left after removing the postcode and town.
        - The cleaned address is stored in a new column `address_lines`.

    Example:
    --------
    Given a DataFrame with the following address:

    Input Address: "123 Main Street, London, NO31 NL3" (fake postcode)

    The function will extract the town ("London") and postcode ("NO31 NL3"), and the `address_lines` will be cleaned as:

    Output Address: "123 Main Street"

    """
    
    # Regex patterns for postcode and town
    postcode_pattern = r"([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z]\d{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y]\d{1,2})|(([A-Za-z]\d[A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y]\d[A-Za-z]?))))\s?\d[A-Za-z]{2})"
    town_regex = r',\s*(?!STREET|ROAD|LANE|AVENUE|DRIVE|SQUARE|HILL)\b([A-Za-z ]+?)\b(?=,|$)'
    towns_pattern = f"({'|'.join([re.escape(town) for town in town_list])})"
    
    # Step 1: Extract postcode
    df = df.withColumn('postcode', regexp_extract(col(address_col), postcode_pattern, 0))
    
    # Step 2: Extract town using town list or fallback regex
    df = df.withColumn('town', regexp_extract(col(address_col), towns_pattern, 0))
    df = df.withColumn('town', when(col('town') == "", regexp_extract(col(address_col), town_regex, 1)).otherwise(col('town')))
    
    # Step 3: Clean address lines by removing postcodes and town names
    df = df.withColumn('address_lines', regexp_replace(col(address_col), f"({postcode_pattern}|{towns_pattern})", ""))
    df = df.withColumn('address_lines', regexp_replace(col('address_lines'), r'\s*,\s*', ', '))
    df = df.withColumn('address_lines', trim(regexp_replace(col('address_lines'), r',+', ',')))
    df = df.withColumn('address_lines', trim(regexp_replace(col('address_lines'), r'(^,|,$)', '')))
    df = df.withColumn('address_lines', regexp_replace(col('address_lines'), r', ,', ','))
    df = df.withColumn('address_lines', trim(regexp_replace(col('address_lines'), r'\s+', ' ')))
    
    return df

  
#################################################################
'''
def identify_patterns(df: DataFrame, address_col: str = "final_cleaned_address") -> DataFrame:
    """
    Identifies numeric patterns within the address that may indicate building numbers, 
    suffixes, or other address-related patterns. This function extracts a variety of 
    numeric patterns commonly found in UK addresses, flagging records where patterns are identified.

    Parameters:
    -----------
    df : pyspark.sql.DataFrame
        The input DataFrame containing the address data.
    address_col : str, optional
        The name of the column in the DataFrame that holds the full address. Default is "final_cleaned_address".

    Returns:
    --------
    pyspark.sql.DataFrame
        A DataFrame with the following additional columns:
        - unclass_num : str
            Identifies numbers in the address that don't fall into any specific class.
        - single_num : str
            Identifies single standalone numbers, possibly building numbers or unit numbers.
        - txt_b4_num : str
            Captures text that appears immediately before a number in the address.
        - end_num : str
            Identifies numbers that appear toward the end of the address.
        - start_num : str
            Extracts numbers that appear at the start of the address.
        - start_num_suff : str
            Extracts suffixes attached to numbers at the start of the address (e.g., "123A").
        - end_num_suff : str
            Extracts suffixes attached to numbers at the end of the address (e.g., "123A").
        - patterns_identified_flag : int
            A flag (1) indicating that one or more numeric patterns were identified in the address.

    Notes:
    ------
    - This function uses regular expressions (regex) to match and extract numeric patterns from the address.
    - The identified patterns may help categorize building or street numbers for further processing.
    """
    df = df.withColumn('unclass_num', regexp_extract(col(address_col), r'(^|\s)(\D?(\d+\.\d+(\.\d+)?\.?\s)|([A-Z]{1,2}\d+\s))', 0))
    df = df.withColumn('single_num', regexp_extract(col(address_col), r'((^|^\D*\s)\d+(\s?[A-Z]\b)?)((\s?-\s?|\sTO\s|\\|\/|\s)(\d+(\s?[A-Z]\b)?))?(?!.+\b\d)', 0))
    df = df.withColumn('txt_b4_num', regexp_extract(col(address_col), r'^\D+\s(?=\d+)', 0))
    df = df.withColumn('end_num', regexp_extract(col(address_col), r'((?<=\s)\d+(\s?[A-Z]\b)?)((\s?-\s?|\sTO\s|\\|\/|\s)(\d+(\s?[A-Z]\b)?))?(?!.+(\b\d+(\s?[A-Z]\b)?)((\s?-\s?|\sTO\s|\\|\/|\s)(\d+(\s?[A-Z]\b)?))?)', 0))
    df = df.withColumn('start_num', regexp_extract(col(address_col), r'^\d+', 0))
    df = df.withColumn('start_num_suff', regexp_extract(col(address_col), r'((?<=^\d{1})|(?<=^\d{2})|(?<=^\d{3})|(?<=^\d{4}))\s?([A-Z]\b)', 0))
    df = df.withColumn('end_num_suff', regexp_extract(col(address_col), r'((?<=^\d{1})|(?<=^\d{2})|(?<=^\d{3})|(?<=^\d{4}))(?<=\s)?([A-Z]\b)?(\s?-\s?|\sTO\s|\\|\/|\s)\s?\d+', 0))
    df = df.withColumn('patterns_identified_flag', lit(1))
    return df

#################################################################

def identify_location_units(df: DataFrame, address_col: str = "final_cleaned_address") -> DataFrame:
    """
    Identifies specific location-related units within the address, such as flats, rooms, units, blocks, 
    apartments, and floors. This function extracts common building-related terms and flags records 
    where these terms are found.

    Parameters:
    -----------
    df : pyspark.sql.DataFrame
        The input DataFrame containing the address data.
    address_col : str, optional
        The name of the column in the DataFrame that holds the full address. Default is "final_cleaned_address".

    Returns:
    --------
    pyspark.sql.DataFrame
        A DataFrame with the following additional columns:
        - flat : str
            Extracts references to flats in the address (e.g., "Flat 2").
        - room : str
            Extracts references to rooms in the address (e.g., "Room 5").
        - unit : str
            Extracts references to units in the address (e.g., "Unit 12").
        - block : str
            Extracts references to blocks in the address (e.g., "Block A").
        - apartment : str
            Extracts references to apartments in the address (e.g., "Apartment 4").
        - floor : str
            Extracts references to floors in the address (e.g., "First Floor", "Basement").
        - location_units_identified_flag : int
            A flag (1) indicating that one or more location units were identified in the address.

    Notes:
    ------
    - This function uses regular expressions (regex) to match and extract building-related terms such as flat, room, unit, block, and floor.
    - The function provides a structured way to identify the presence of these terms for further address processing.
    """
    df = df.withColumn('flat', regexp_extract(col(address_col), r'((\bFLAT\s((NO|NO\.)\s?)?([-A-Z0-9\.]{1,4}|([^A-Z]{1,6})))\s|\bFLAT[-0-9A-RU-Z\.]{1,4}\s|\bFLAT\s)((\s?-\s?|\s?TO\s|\s?&\s?|\s?\\\s?)((\bFLAT\s)?[-A-Z0-9\.]{1,4}\b)\s)?', 0))
    df = df.withColumn('room', regexp_extract(col(address_col), r'((\bROOM\s((NO|NO\.)\s?)?([-A-Z0-9\.]{1,4}|([^A-Z]{1,6})))\s|\bROOM[-0-9A-RT-Z\.]{1,4}\s|\bROOM\s)((\s?-\s?|\s?TO\s|\s?&\s?|\s?\\\s?)((\bROOM\s)?[-A-Z0-9\.]{1,4}\b)\s)?', 0))
    df = df.withColumn('unit', regexp_extract(col(address_col), r'((\bUNIT\s((NO|NO\.)\s?)?([-A-Z0-9\.]{1,4}|([^A-Z]{1,6})))\s|\bUNIT[-0-9A-RT-Z\.]{1,4}\s|\bUNIT\s)((\s?-\s?|\s?TO\s|\s?&\s?\\\s?)((\bUNIT\s)?[-A-Z0-9\.]{1,4}\b)\s)?', 0))
    df = df.withColumn('block', regexp_extract(col(address_col), r'((\bBLOCK\s((NO|NO\.)\s?)?([-A-Z0-9\.]{1,4}|([^A-Z]{1,6})))\s|\bBLOCK[-0-9A-RT-Z\.]{1,4}\s|\bBLOCK\s)((\s?-\\s?|\s?TO\s|\s?&\s?\\\s?)((\bBLOCK\s)?[-A-Z0-9\.]{1,4}\b)\s)?', 0))
    df = df.withColumn('apartment', regexp_extract(col(address_col), r'((\bAPARTMENT\s((NO|NO\.)\s?)?([-A-Z0-9\.]{1,4}|([^A-Z]{1,6})))\s|\bAPARTMENT[-0-9A-RT-Z\.]{1,4}\s|\bAPARTMENT\s)((\s?-\s?|\s?TO\s|\s?&\s?\\\s?)((\bAPARTMENT\s)?[-A-Z0-9\.]{1,4}\b)\s)?', 0))
    df = df.withColumn('floor', regexp_extract(col(address_col), r'BASEMENT|((GROUND|FIRST|SECOND|THIRD|FOURTH|FIFTH|SIXTH|SEVENTH)\sFLOOR\b)', 0))
    df = df.withColumn('location_units_identified_flag', when(col('flat').isNotNull() | col('room').isNotNull() | col('unit').isNotNull() | col('block').isNotNull() | col('apartment').isNotNull() | col('floor').isNotNull(), lit(1)).otherwise(lit(0)))
    return df
  '''
