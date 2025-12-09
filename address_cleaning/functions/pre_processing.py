import pandas as pd
import re
from collections import Counter
from functools import reduce

import fuzzywuzzy
from fuzzywuzzy import fuzz
import openpyxl
import xlrd

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, regexp_replace, upper, col, when, length, split, regexp_extract, trim, concat_ws, lit
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, ArrayType

from address_functions.config.settings import town_list

####################################################################################

def clean_punctuation(df: DataFrame, input_col="supplied_query_address", create_flag=True):
    """
    Cleans up punctuation from address strings by removing or fixing unwanted characters, while preserving hyphens 
    and periods where necessary (e.g., between numbers or block names).

    Parameters:
    - df (DataFrame): The input DataFrame containing address data.
    - input_col (str): The name of the column containing the address strings to be cleaned.
    - create_flag (bool): Whether to create a flag indicating if punctuation was cleaned (default is True).

    Returns:
    - df (DataFrame): A DataFrame with cleaned address strings in 'final_cleaned_address', and an optional flag
                      ('punctuation_cleaned_flag') indicating if changes were made.

    Examples:
    - Input: ",.123- MAIN STREET, LONDON,"
      Output: "123 MAIN STREET, LONDON"
      (Leading commas and periods are removed, and the hyphen is preserved between numbers.)

    - Input: "BLOCK A-1-2, 14 - 16, SOMEWHERE ROAD"
      Output: "BLOCK A-1-2, 14-16, SOMEWHERE ROAD"
      (Hyphens between alphanumeric chars are preserved, while hyphens between numbers are standardised.)

    - Input: "FLAT 4 . 2, 67 HIGH STREET - 123, CITY - 45, TOWN"
      Output: "FLAT 4.2, 67 HIGH STREET-123, CITY-45, TOWN"
      (Unnecessary spaces around periods and hyphens are removed, standardising punctuation between numbers and alphanumeric strings.)

    - Input: ",,,, UNIT 9,, BLOCK-5,, RANDOM ROAD,,"
      Output: "UNIT 9, BLOCK-5, RANDOM ROAD"
      (Multiple commas and unnecessary punctuation are cleaned up to create a clearer string.)
    """
    def clean_part(part):
        if part:
            # Replace hyphen between room numbers with a comma (e.g., "Room 7 - 1"), account for varying spaces
            part = re.sub(r'(Room\s+\d+)\s*-\s*(\d+)', r'\1, \2', part)

            # Preserve hyphens between numbers, even if there are spaces around the hyphen (e.g., "14 - 16")
            part = re.sub(r'(?<=\d)\s*-\s*(?=\d)', ' TEMP_HYPHEN ', part)
            
            # Preserve periods (.) between numbers (e.g., "14.16")
            part = re.sub(r'(?<=\d)\s*\.\s*(?=\d)', ' TEMP_DOT ', part)

            # Preserve hyphens in cases like "II-2" or "Gp2-4-B-7"
            part = re.sub(r'(?<=\w)-(?=\w)', ' TEMP_HYPHEN ', part)
            part = re.sub(r'(?<=BLOCK\s\w)-(?=\d)', ' TEMP_HYPHEN ', part)  # Preserve hyphen in block names
            part = re.sub(r'(?<=\w)-(?=\d\w)', ' TEMP_HYPHEN ', part)  # Preserve hyphens like "C-11E"

            # Remove leading hyphens before numbers, except in preserved cases
            part = re.sub(r'-\s*(?=\d)', '', part)

            # Remove punctuation at the beginning and end
            part = re.sub(r"^[^a-zA-Z0-9]+|[^a-zA-Z0-9]+$", "", part)

            # Normalise whitespace
            part = re.sub(r"\s+", " ", part)

            # Restore preserved hyphens and periods
            part = part.replace(' TEMP_HYPHEN ', '-')
            part = part.replace(' TEMP_DOT ', '.')
        return part.strip()

    # Apply cleaning logic to each address part
    @udf(ArrayType(StringType()))
    def clean_parts_udf(parts):
        return [clean_part(part) for part in parts]

    # Step 1: Temporarily preserve hyphens between numbers or in special cases
    df = df.withColumn("cleaned_address", regexp_replace(col(input_col), r'(?<=\d)-(?=\d)', ' TEMP_HYPHEN '))
    df = df.withColumn("cleaned_address", regexp_replace(col("cleaned_address"), r'(?<=\d)\.\s*(?=\d)', ' TEMP_DOT '))
    df = df.withColumn("cleaned_address", regexp_replace(col("cleaned_address"), r'(?<=\w)-(?=\d\w)', ' TEMP_HYPHEN '))

    # Step 2: Handle punctuation marks and spaces, excluding preserved hyphens and periods
    df = df.withColumn("cleaned_address",
                       regexp_replace(col("cleaned_address"),
                                      r"[\s,.-]*\.,[\s,.-]*|[\s,.-]+\,|,\s*[\s,.-]+",
                                      ", "))
    df = df.withColumn("cleaned_address",
                       regexp_replace(col("cleaned_address"),
                                      r",\s*,|(^[\s,.-]+)|([\s,.-]+$)",
                                      ", "))

    # Step 3: Remove leading punctuation or commas at the start of the string (again after transformations)
    df = df.withColumn("cleaned_address", regexp_replace(col("cleaned_address"), r"^[,.\s]+", ""))

    # Step 4: Split the address into parts to handle each part separately
    df = df.withColumn("address_parts", split(col("cleaned_address"), ",\\s*"))

    # Step 5: Clean each part and join back into a single address string
    df = df.withColumn("cleaned_parts", clean_parts_udf(col("address_parts")))
    df = df.withColumn("final_cleaned_address", concat_ws(", ", col("cleaned_parts")))

    # Step 6: Restore preserved hyphens and periods in the final cleaned address
    df = df.withColumn("final_cleaned_address", regexp_replace(col("final_cleaned_address"), ' TEMP_HYPHEN ', '-'))
    df = df.withColumn("final_cleaned_address", regexp_replace(col("final_cleaned_address"), ' TEMP_DOT ', '.'))

    # Step 7: Remove any trailing commas and spaces in the final cleaned address
    df = df.withColumn("final_cleaned_address", regexp_replace(col("final_cleaned_address"), r",\s*$", ""))

    # Step 8: Create a flag indicating whether punctuation was cleaned
    if create_flag:
        df = df.withColumn("punctuation_cleaned_flag",
                           when(col(input_col) == col("final_cleaned_address"), 0).otherwise(1))
    else:
        df = df.withColumn("punctuation_cleaned_flag",
                           when(col("punctuation_cleaned_flag").isNotNull(), col("punctuation_cleaned_flag"))
                           .otherwise(when(col(input_col) == col("final_cleaned_address"), 0).otherwise(1)))

    # Drop intermediate columns
    df = df.drop("cleaned_address", "address_parts", "cleaned_parts")

    return df
  


##############################################################################

def remove_noise_words_with_flag(df, input_col="final_cleaned_address"):
    """
    Removes noise words from the input address column and flags any rows where noise words were removed.

    Noise words are defined as sequences of the same uppercase letter repeated three or more times (e.g., "AAAA").

    Parameters:
    - df (DataFrame): The input DataFrame containing address data.
    - input_col (str): The name of the column containing the address strings to be cleaned (default is "final_cleaned_address").

    Returns:
    - df (DataFrame): The updated DataFrame with noise words removed and a flag ('noise_removed_flag') indicating 
                      whether any noise words were removed.

    Example:
    - Input: "123 MAIN ROAD, AAAA, LONDON"
    - Output: "123 MAIN ROAD, LONDON"
      (The noise word "AAAA" is removed, and the 'noise_removed_flag' is set to 1 for this row.)
    """
    
    # Define the regex pattern for noise words: sequences of the same uppercase letter repeated 3 or more times.
    noise_pattern = r"\b([A-Z])\1{3,}\b"
    
    # Replace noise words in the input address column with an empty string
    df = df.withColumn("cleaned_address", regexp_replace(col(input_col), noise_pattern, ""))
    
    # Create a flag that indicates whether noise words have been removed
    df = df.withColumn("noise_removed_flag", when(col("cleaned_address") != col(input_col), 1).otherwise(0))
    
    # Update the original address column with the cleaned address and drop the column created for this function
    df = df.withColumn(input_col, col("cleaned_address"))
    df = df.drop("cleaned_address")
    
    return df
  
##############################################################################


def get_process_and_deduplicate_address_udf(column_name="final_cleaned_address"):
    """
    Processes and deduplicates parts of an address based on similarity. 
    The function compares consecutive parts of the address, and if they are highly similar (based on a threshold), 
    it keeps only one. It also flags rows where changes were made.

    Parameters:
    - column_name (str): The name of the address column to process (default is "final_cleaned_address").

    Returns:
    - UDF: A User-Defined Function (UDF) that takes an address string and returns:
        - cleaned_address: The address with deduplicated parts.
        - words_deduplicated_flag: A flag indicating whether any deduplication was done (1 if changes were made, 0 otherwise).

    Example:
    - Input: "123 MAIN ROAD, MAIN ROAD, LONDON"
    - Output: ("123 MAIN ROAD, LONDON", 1)
      (The repeated part "MAIN ROAD" is removed, and the 'words_deduplicated_flag' is set to 1.)
    """

    def process_and_deduplicate_address(address, threshold=95):
        """
        Deduplicates parts of the address by comparing consecutive parts based on a similarity threshold. (fuzzy uses Levenshtein)
        
        Parameters:
        - address (str): The address string to process.
        - threshold (int): The similarity threshold (default is 95). Parts with a similarity ratio above this will be considered duplicates.

        Returns:
        - tuple: A tuple containing:
            - cleaned_address (str): The deduplicated address string.
            - words_deduplicated_flag (int): A flag indicating whether any deduplication was done (1 if changes were made, 0 otherwise).
        """
        def contains_numbers(s):
            # Check if the string contains any numbers
            return bool(re.search(r'\d', s))

        parts = [part.strip() for part in address.split(',')]
        processed = []
        seen = set()
        skip_next = False
        changes_made = False

        for i in range(len(parts)):
            if skip_next:
                skip_next = False
                continue

            current_part = parts[i]
            if i < len(parts) - 1:
                next_part = parts[i + 1]
                # Check if the current part and the next part are highly similar
                if fuzz.ratio(current_part, next_part) >= threshold and abs(len(current_part) - len(next_part)) < 3:
                    if contains_numbers(current_part) or not contains_numbers(next_part):
                        chosen_part = current_part
                    else:
                        chosen_part = next_part
                    processed.append(chosen_part)
                    skip_next = True
                    changes_made = True
                else:
                    processed.append(current_part)
            else:
                processed.append(current_part)

        final_parts = []
        for part in processed:
            # Add the part if it's not already seen, ensuring no duplicates are added
            if part not in seen:
                final_parts.append(part)
                seen.add(part)
            else:
                changes_made = True

        # Flag is set to 1 if any changes were made, 0 otherwise
        flag = 1 if changes_made else 0
        return (', '.join(final_parts), flag)

    # Return a UDF to process and deduplicate addresses
    return udf(process_and_deduplicate_address, StructType([
        StructField("cleaned_address", StringType(), True),
        StructField("words_deduplicated_flag", IntegerType(), True)
    ]))

    


###################################################################################



def deduplicate_postcodes_udf():
    import re
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    # UK postcode regex pattern (with or without space)
    postcode_regex = r"([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z]\d{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y]\d{1,2})|(([A-Za-z]\d[A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y]\d[A-Za-z]?))))\s?\d[A-Za-z]{2})"

    def normalise_postcode(postcode):
        """Normalize postcode by removing spaces and dashes."""
        return re.sub(r'[\s-]', '', postcode.upper())

    def ensure_postcode_format(postcode):
        """Ensure the postcode has the correct format (with a space)."""
        postcode = normalise_postcode(postcode)
        if len(postcode) > 3:
            return postcode[:-3] + " " + postcode[-3:]
        return postcode

    def extract_postcodes(address):
        """Extract all valid UK postcodes from an address string."""
        postcodes = re.findall(postcode_regex, address)
        if not postcodes:
            return []
        return [next(pc for pc in pc_tuple if pc) for pc_tuple in postcodes]  # Flatten tuples

    def remove_prefix_duplicates(address, formatted_postcodes):
        """Remove all occurrences of postcodes (both spaced and unspaced) except the last one."""
        for postcode in formatted_postcodes[:-1]:  # Leave the last one untouched for now
            normalised_pc = normalise_postcode(postcode)
            # Remove the postcode regardless of spacing
            address = re.sub(re.escape(normalised_pc), '', address)
            address = re.sub(re.escape(postcode), '', address)
        return re.sub(r'\s+', ' ', address.strip())  # Clean up extra spaces

    def move_last_postcode_to_end(address, formatted_postcodes):
        """Ensure that the last formatted postcode appears at the end of the address."""
        last_postcode = formatted_postcodes[-1]  # Take the last correctly formatted postcode
        # Remove all instances of postcodes from the address
        for pc in formatted_postcodes:
            address = re.sub(re.escape(pc), '', address)

        # Append the last formatted postcode to the end
        address = address.strip(', ') + ", " + last_postcode
        return re.sub(r'\s*,\s*', ', ', address.strip(', '))  # Clean up punctuation

    def deduplicate_postcodes(address):
        """Main function to deduplicate postcodes in an address."""
        postcodes = extract_postcodes(address)
        if not postcodes:
            return address, 0  # No valid postcodes found
        
        formatted_postcodes = [ensure_postcode_format(pc) for pc in postcodes]
        changes_flag = 0
        new_address = address

        # Step 1: Remove all duplicate postcodes except the last formatted one
        new_address = remove_prefix_duplicates(new_address, formatted_postcodes)
        
        # Step 2: Ensure the last formatted postcode is kept at the end
        new_address = move_last_postcode_to_end(new_address, formatted_postcodes)

        # Check if any changes were made
        if new_address != address:
            changes_flag = 1
        
        return new_address, changes_flag

    return udf(deduplicate_postcodes, StructType([
        StructField("final_cleaned_address", StringType()), 
        StructField("changes_flag", IntegerType())
    ]))
  
  
###################################################################################
  
def map_and_check_postcode(address):
    """
    Corrects and validates UK postcodes within an address string by applying character mapping 
    to fix common misinterpretations (e.g., 'I' to '1') and checks if the resulting postcode is valid.

    The function performs the following steps:
    1. Splits the address into parts to identify potential postcodes.
    2. Checks if any valid UK postcodes exist using a regex pattern.
    3. If no valid postcode is found, applies a character map to correct common mistakes such as:
       - 'I' -> '1'
       - 'O' -> '0'
       - 'S' -> '5'
       - 'Z' -> '2'
    4. Re-checks if the corrected postcode is now valid.
    5. Returns the cleaned address with the valid postcode, if found or corrected, and sets a flag if any changes were made.

    Parameters:
    ----------
    None. The UDF will be applied to a DataFrame column.

    Returns:
    --------
    pyspark.sql.functions.udf: A PySpark UDF that processes an address string to identify, correct, and validate postcodes.

    UDF Output Schema:
    - final_cleaned_address (StringType): The cleaned address with a valid postcode, if applicable.
    - changes_flag (IntegerType): A flag indicating if any changes were made to the postcode (1 if changed, 0 otherwise).

    Example:
    -------
    Input: "123 MAIN STREET, LNI 2QB, LONDON"
    Output: "123 MAIN STREET, LN1 2QB, LONDON", 1  # 'I' corrected to '1' in the postcode

    Input: "FLAT 4, 1-3 FAKE STREET, AB1 2CD, CITY"
    Output: "FLAT 4, 1-3 FAKE STREET, AB1 2CD, CITY", 0  # Postcode already valid, no changes

    Input: "SO41 NLP, 123 MAIN ROAD" (fake postcode)
    Output: "123 MAIN ROAD, SO41 NLP", 1  # Postcode moved to the end of the address
    """
    import re
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType

    # The preferred postcode regex
    postcode_regex = r"([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z]\d{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y]\d{1,2})|(([A-Za-z]\d[A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y]\d[A-Za-z]?))))\s?\d[A-Za-z]{2})"

    def normalise_postcode(postcode):
        return postcode.replace(" ", "").replace("-", "").upper()

    def format_postcode(postcode):
        postcode = normalise_postcode(postcode)
        if len(postcode) > 3:
            return postcode[:-3] + " " + postcode[-3:]
        return postcode

    def is_valid_postcode(postcode):
        # Use the preferred postcode regex
        return re.fullmatch(postcode_regex, postcode) is not None

    def looks_like_postcode(part):
        # Check if a part looks like a UK postcode (combination of letters and numbers)
        return bool(re.search(r'[A-Za-z0-9]', part)) and not part.isalpha()  # Ensure it's not just a word

    def correct_postcode(postcode):
        # Character mapping for commonly misinterpreted characters
        char_map = {
            'I': '1', 'O': '0', 'S': '5', 'Z': '2',
            'B': '8', 'D': '0', 'G': '6', 'J': '1',
            'A': '4', 'E': '3', 'H': '4', 'L': '1',
            'U': '0', 'Y': '4', 'C': '0', 'K': '1', 'M': '1'
        }
        return ''.join([char_map.get(char, char) for char in postcode])

    def map_and_check_postcode(address):
        parts = [part.strip() for part in address.split(',')]
        changes_flag = 0
        valid_postcode_found = False

        print(f"Processing address: {address}")  # Debugging statement

        # First, check if a valid UK postcode already exists in the string
        for part in parts:
            if is_valid_postcode(part):
                valid_postcode_found = True
                print(f"Valid postcode found: {part}")  # Debugging statement
                break  # Exit if a valid postcode is found

        # If no valid postcode was found, apply mapping and correction
        if not valid_postcode_found:
            for i, part in enumerate(parts):
                print(f"Checking part: {part}")  # Debugging statement
                # Only apply mapping to parts that look like they might be postcodes (but not words)
                if looks_like_postcode(part) and not is_valid_postcode(part):
                    print(f"Applying correction to part: {part}")  # Debugging statement
                    corrected = correct_postcode(part)
                    if is_valid_postcode(corrected):
                        print(f"Corrected postcode: {corrected}")  # Debugging statement
                        parts[i] = format_postcode(corrected)
                        changes_flag = 1  # Set the flag if a valid postcode is corrected

        final_address = ', '.join(parts)
        return (final_address, changes_flag)

    map_and_check_postcode_udf = udf(map_and_check_postcode, StructType([
        StructField("final_cleaned_address", StringType()), 
        StructField("changes_flag", IntegerType())
    ]))

    return {
        "map_and_check_postcode_udf": map_and_check_postcode_udf
    }
  
#############################################################################################

def standardise_street_types(df, address_col="final_cleaned_address"):
    """
    Standardises street type abbreviations and common misspellings within an address column, 
    applying a set of predefined rules to replace short forms like 'ST' with 'STREET' and 
    fix common typos. Adds a flag to indicate if any standardisation occurred.

    The function performs the following steps:
    1. Identifies and replaces abbreviations or misspellings of street types (e.g., 'ST' becomes 'STREET').
    2. Applies regex-based transformations for common street type abbreviations such as:
       - 'STR' or 'STRT' -> 'STREET'
       - 'ST' (not followed by 'REET') -> 'STREET'
       - 'RD' or 'RAOD' -> 'ROAD'
       - 'AVE', 'AVE.' or 'AVENEU' -> 'AVENUE'
       - 'CRT', 'CRT.', or 'CT' -> 'COURT'
       - 'CRESENT' or 'CRSNT' -> 'CRESCENT'
       - 'DRV' or 'DR' -> 'DRIVE'
       - 'GRDN' or 'GDN' -> 'GARDEN'
       - 'PK' -> 'PARK'
       - 'CL' -> 'CLOSE'
    3. Compares the original and modified address columns to determine if any changes were made.
    4. Adds a flag (`street_type_standardised_flag`) indicating whether any street type standardisation occurred.

    Parameters:
    ----------
    df : pyspark.sql.DataFrame
        The input DataFrame containing address data.
    address_col : str, optional
        The name of the address column to apply standardisation to. Default is "final_cleaned_address".

    Returns:
    --------
    pyspark.sql.DataFrame
        A DataFrame with the following additions:
        - The standardised address column with street type abbreviations expanded and misspellings corrected.
        - 'street_type_standardised_flag' (IntegerType): A flag (1 if changes were made, 0 otherwise) indicating whether any standardisation occurred.

    Example:
    -------
    Input: "123 MAIN ST, LONDON"
    Output: "123 MAIN STREET, LONDON", 1  # 'ST' expanded to 'STREET'

    Input: "456 PARK AVE., CITY"
    Output: "456 PARK AVENUE, CITY", 1  # 'AVE.' corrected to 'AVENUE'

    Input: "789 GARDEN CRT, TOWN"
    Output: "789 GARDEN COURT, TOWN", 1  # 'CRT' corrected to 'COURT'

    Input: "321 DRIVE LANE"
    Output: "321 DRIVE LANE", 0  # No changes, street type already standardised
    """
    original_column = col(address_col)

    # Apply standardisation rules for street types
    df = df.withColumn(address_col, regexp_replace(col(address_col), r'\bSTR\b|\bSTRT\b', 'STREET'))
    df = df.withColumn(address_col, regexp_replace(col(address_col), r'\bST\b(?!REET\b)', 'STREET'))
    df = df.withColumn(address_col, regexp_replace(col(address_col), r'\bRD\b|RAOD', 'ROAD'))
    df = df.withColumn(address_col, regexp_replace(col(address_col), r'\bAVE\b|\bAVE\.\b|\bAVENEU\b', 'AVENUE'))
    df = df.withColumn(address_col, regexp_replace(col(address_col), r'\bCRT\b|\bCRT\.\b|\bCT\b', 'COURT'))
    df = df.withColumn(address_col, regexp_replace(col(address_col), r'\bCRESENT\b|\bCRSNT\b', 'CRESCENT'))
    df = df.withColumn(address_col, regexp_replace(col(address_col), r'\bDRV\b|\bDR\b', 'DRIVE'))
    df = df.withColumn(address_col, regexp_replace(col(address_col), r'\bGRDN(?=S\b)?\b|\bGDN(?=S\b)?\b', 'GARDEN'))
    df = df.withColumn(address_col, regexp_replace(col(address_col), r'\bPK\b', 'PARK'))
    df = df.withColumn(address_col, regexp_replace(col(address_col), r'\bCL\b', 'CLOSE'))

    # Add a flag to indicate if any standardization has occurred by comparing the original column and the modified one
    df = df.withColumn(
        'street_type_standardised_flag',
        when(col(address_col) != original_column, lit(1)).otherwise(lit(0))
    )

    return df
  
