# GeoJSON Fuzzy Matching Script

## Overview
This script analyzes the GeoJSON property ownership file and performs fuzzy matching to identify which owner names correspond to businesses in the output folder data.

## What It Does

1. **Analyzes GeoJSON Structure**: Examines all properties/columns in the GeoJSON file
2. **Extracts Owner Names**: Collects all unique OWNERNAME values (excluding generic "PRIVATE" entries)
3. **Loads Business Data**: Combines data from:
   - `all_businesses.json`
   - All CSV files in the output folder
4. **Fuzzy Matching**: Uses RapidFuzz library to match owner names with business names (85% similarity threshold)
5. **Generates Reports**: Creates three output files

## Output Files

### 1. `geojson_matches_[timestamp].csv`
Contains all successfully matched owner names with:
- GeoJSON owner name
- Matched business name
- Similarity score (85-100%)
- Business type
- Corporation number
- Location
- Status
- Source file

### 2. `geojson_no_matches_[timestamp].csv`
Contains owner names that didn't match with:
- GeoJSON owner name
- Best partial match (below threshold)
- Partial match score

### 3. `geojson_matching_summary_[timestamp].txt`
Comprehensive summary report including:
- Total statistics
- Match quality distribution
- Matches by source file
- Matches by business type
- Top 20 best matches

### 4. `geojson_matched_properties_[timestamp].geojson` ⭐ NEW
A GeoJSON file containing ONLY the properties that matched with businesses. Each feature includes:
- All original GeoJSON properties (OWNERNAME, CIVIC_NO, STREET, etc.)
- **Enhanced properties**:
  - `MATCHED_BUSINESS_NAME`: The matched business name from the database
  - `BUSINESS_TYPE`: Type of business (Non-Profit, Co-operative, etc.)
  - `CORPORATION_NUMBER`: Corporate registration number
  - `MATCH_SCORE`: Fuzzy match similarity score (85-100)
  - `DATA_SOURCE`: Which file the match came from

This GeoJSON can be opened in mapping tools like QGIS, ArcGIS, or web mapping libraries to visualize the locations of matched businesses on a map.

## GeoJSON Properties

The script identified these columns in the GeoJSON file:
- **AGENCY**: Agency type
- **CIVIC_NO**: Civic/street number
- **OBJECTID**: Unique object identifier
- **OWNERNAME**: Property owner name (primary matching field)
- **PROPERTY_UNIT_ID**: Property unit identifier
- **STREET**: Street name
- **UNIT**: Unit number (if applicable)

## Results Summary

From the latest run:
- **Total GeoJSON features**: 144,037
- **Unique owner names**: 3,522 (excluding "PRIVATE")
- **Total businesses in database**: 43,535
- **Matches found**: 37 (1.1%)
- **No matches**: 3,485 (98.9%)
- **Matched properties in output GeoJSON**: 107 (properties from 37 unique businesses)

### Match Quality
- 95-100% (Excellent): 5 matches
- 85-89% (Good): 32 matches

### Top Business Types Matched
1. Not-for-Profit Corporation: 59.5%
2. Co-operative Non-Share: 24.3%
3. Federal Non-Profit: 13.5%
4. Co-operative with Share: 2.7%

## Usage

```bash
cd etl/data_analysis
python fuzzy_match_geojson.py
```

## Configuration

You can adjust the matching threshold by editing the `FUZZY_THRESHOLD` constant in the script (default: 85).

## Dependencies

- Python 3.x
- rapidfuzz (for fuzzy string matching)

## Notes

- The low match rate (1.1%) is expected because most properties are owned by individuals or generic private entities
- The script filters out "PRIVATE" owner names to focus on organizational/business owners
- Matches include churches, non-profits, housing cooperatives, and other organizations that own property
