#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Converted from Jupyter Notebook: notebook.ipynb
Conversion Date: 2025-11-27T02:30:08.500Z
"""

# 0) IMPORT PACKAGES

# SETUP INSTRUCTIONS TODO
# a) Install the  libpostal C library according to instructions at https://github.com/openvenues/libpostal
# b) TODO

import pandas as pd
import difflib
#import postal


# 1) READ IN DATA

# PUBLIC LAND OWNERSHIP DATA (KITCHENER)
# ownership DataFrame describes public land ownership data (Kitchener) 
# from https://open-kitchenergis.opendata.arcgis.com/datasets/KitchenerGIS::property-ownership-public/explore?location=43.383752%2C-80.482943%2C12.06

# REGISTERED CHARITIES (KITCHENER)
# charity_kitchener DataFrame describes registered charities (Kitchener) from https://apps.cra-arc.gc.ca/ebci/hacc/srch/pub/dsplyBscSrch?request_locale=en
# filtered by Status = 'Registered', City = 'Kitchener'

ownership = pd.read_csv("/home/indy/Desktop/union-coop-project/Property_Ownership_Public_-8905505954391439593.csv")
charity_kitchener = pd.read_csv('/home/indy/Desktop/union-coop-project/Charities_results_2025-11-09-14-17-45.txt', sep="	", encoding="cp863",on_bad_lines='warn')

# 2) PRE-PROCESSING STEP

# CHARITY_KITCHENER DATAFRAME
# Remove trailing whitespace for 'Address:' column
charity_kitchener['Address:']  = charity_kitchener['Address:'].str.rstrip()
# Normalize 'Address:' column using libpostal
#TODO
# String normalize 'Organization name:' column
charity_kitchener['Organization name:'] = charity_kitchener['Organization name:'].str.lower()
# Rename relevant columns
charity_kitchener = charity_kitchener.rename({'BN/Registration number:': 'BN/Registration Number', 'Organization name:': 'Organization Name', 
                                              'Effective date of status:': 'Effective Date of Status', 'Charity type: ': 'Charity Type', 'Category: ': 'Category', 
                                              'Postal code/Zip code:': 'Postal Code/Zip Code'}, axis=1)

# OWNERSHIP DATAFRAME
# Convert 'Civic No' column to type str
ownership['Civic No'] = ownership['Civic No'].astype('Int64').astype(str)
# Combine 'Civic No' and 'Street' columns
ownership['Civic No + Street'] = ownership['Civic No'] + " " + ownership['Street']
# Normalize 'Civic No + Street' column using libpostal
#TODO
# String normalize 'Ownername' column
ownership['Ownername'] = ownership['Ownername'].str.lower()

# 3) DATA MERGING STEP

# Compare entries in charity_kitchener DataFrame and ownership DataFrame to get rows that match based on addresses
# These rows are charities in Kitchener that own property in Kitchener (based on the public land ownership data for Kitchener)

rel_cols = ["Objectid", "Property Unit Id", "Ownername", "AGENCY", "x", "y", "Civic No + Street", "BN/Registration Number", "Organization Name", "Effective Date of Status", 
            "Charity Type", "Category", "Postal Code/Zip Code"]


ownership_charity = ownership.merge(charity_kitchener, left_on="Civic No + Street", right_on="Address:", how='inner')


final_df = ownership_charity.loc[:, rel_cols]

#TODO: compare entries based on Ownername and Organization Name

# 4) DATA POST-PROCESSING STEP
# - rename columns in DataFrame to be more human-friendly
# - reorganize columns in DataFrame

col_dict = {'Objectid': 'object_id', 'Property Unit Id': 'property_unit_id', 'Ownername': 'owner_name', 'AGENCY': 'agency', 'Civic No + Street': 'address', 'BN/Registration Number': 'business_registration_number',
            'Organization Name': 'organization_name', 'Effective Date of Status': 'effective_date_of_status'}

reorder = ['object_id', 'property_unit_id', 'business_registration_number', 'owner_name', 'organization_name', 'address', 'agency', 'effective_date_of_status']

final_df = final_df.rename(col_dict, axis=1)
final_df = final_df[reorder]

# 5) DATA EXPORT
# Export data as csv so other team members can use it

final_df.to_csv('kitchener_charities_land.csv', index=False)