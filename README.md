# Address Cleaning

## Introduction
This repository contains a collection of address cleaning and processing functions, including pre-processing, quality flagging, result handling, and standardization of address columns. The functions are designed to work on `pyspark.sql.DataFrame` types. To transform from `pandas.DataFrame` the following code can be run:

``` 
from dlh_utils import utilities
import pandas as pd

df = pd.read_csv('addr_index/data/pds_2022_under_65_conf.csv')
df = utilities.pandas_to_spark(df) 
```

## Package Contents: 

| Name | Description |
| ------------- | ------------- |
| `pre_processing.py` | Functions to clean, standardise and flag address data |
| `quality_flags.py` | Functions to flag addresses based on their quality |
| `sac.py` | Extracts "address lines", "town" and "postcode" from address string |
| `results.py` | Runs the above sequentially |

## Example Usage
```
from results import process_df_default

data = {
    "supplied_query_address": [
        "10 Downing St, Westminster, London SW1A 2AA, UK",
        "221B Baker St, Marylebone, London NW1 6XE, UK"
    ]
}

df = pd.DataFrame(data)
processed_df = process_df_default(df)
```

## Testing 
In the testing folder is an example script of implementing the functions on a dataframe

## Config
Config includes python lists that are integral to making sure the quality and sac modules work. By using the list/s as a call in regex.

