# Address Cleaning

## Introduction
This repository contains a collection of address cleaning and processing functions, including pre-processing, quality flagging, result handling, and standardization of address columns. The functions are designed to work on `pyspark.sql.DataFrame` types. To register a spark dataframe from a CSV, the following code can be run. 

``` 
from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local").appName("test").getOrCreate()
df = spark.read.csv("test.csv")
```

## Package Contents: 

| Name | Description | Type |
| ------------- | ------------- | ------------- |
| `resources.py` | Keywords to assist the functions | Resources |
| `pre_processing.py` | Functions to clean, standardise and flag address data | Functions |
| `quality_flags.py` | Functions to flag addresses based on their quality | Functions |
| `sac.py` | Extracts "address lines", "town" and "postcode" from address string | Functions |
| `workflows.py` | Runs the above sequentially | Workflows |

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

