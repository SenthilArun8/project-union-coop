#!/usr/bin/env python3
"""
Cross-check federal non-profits and cooperatives against registered charities.
Identifies overlaps based on business numbers.
"""

import csv
import re
from collections import defaultdict

def extract_business_number(bn_string):
    """
    Extract the 9-digit business number from various formats.
    Examples:
        - '733132559RR0001' -> '733132559'
        - '749949871RC0001' -> '749949871'
        - '766717813' -> '766717813'
        - 'Not Available' -> None
    """
    if not bn_string or bn_string == 'Not Available':
        return None
    
    # Extract first 9 digits
    match = re.match(r'(\d{9})', str(bn_string))
    if match:
        return match.group(1)
    return None

def load_charities(filepath):
    """Load charities from tab-separated text file."""
    charities = {}
    
    with open(filepath, 'r', encoding='latin-1') as f:
        lines = f.readlines()
        
        # Skip header line
        for line in lines[1:]:
            # Split by tab
            parts = line.strip().split('\t')
            if len(parts) > 0 and parts[0]:
                bn_full = parts[0]
                bn = extract_business_number(bn_full)
                
                if bn:
                    org_name = parts[1] if len(parts) > 1 else ''
                    charities[bn] = {
                        'bn_full': bn_full,
                        'name': org_name,
                        'status': parts[2] if len(parts) > 2 else '',
                        'type': parts[3] if len(parts) > 3 else '',
                        'city': parts[10] if len(parts) > 10 else '',
                        'province': parts[11] if len(parts) > 11 else ''
                    }
    
    return charities

def load_csv_businesses(filepath, business_type):
    """Load businesses from CSV file."""
    businesses = {}
    
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            bn_full = row.get('Business Number', '')
            bn = extract_business_number(bn_full)
            
            if bn:
                businesses[bn] = {
                    'bn_full': bn_full,
                    'corporate_name': row.get('Corporate Name', ''),
                    'corporation_number': row.get('Corporation Number', ''),
                    'business_type': business_type
                }
    
    return businesses

def find_overlaps(charities, nonprofits, cooperatives):
    """Find businesses that appear in both charities and business registries."""
    overlaps = []
    
    # Check non-profits
    for bn, nonprofit_data in nonprofits.items():
        if bn in charities:
            overlaps.append({
                'business_number': bn,
                'charity_bn_full': charities[bn]['bn_full'],
                'charity_name': charities[bn]['name'],
                'charity_status': charities[bn]['status'],
                'charity_type': charities[bn]['type'],
                'charity_city': charities[bn]['city'],
                'charity_province': charities[bn]['province'],
                'business_type': 'Federal Non-Profit',
                'business_bn_full': nonprofit_data['bn_full'],
                'corporate_name': nonprofit_data['corporate_name'],
                'corporation_number': nonprofit_data['corporation_number']
            })
    
    # Check cooperatives
    for bn, coop_data in cooperatives.items():
        if bn in charities:
            overlaps.append({
                'business_number': bn,
                'charity_bn_full': charities[bn]['bn_full'],
                'charity_name': charities[bn]['name'],
                'charity_status': charities[bn]['status'],
                'charity_type': charities[bn]['type'],
                'charity_city': charities[bn]['city'],
                'charity_province': charities[bn]['province'],
                'business_type': 'Federal Cooperative',
                'business_bn_full': coop_data['bn_full'],
                'corporate_name': coop_data['corporate_name'],
                'corporation_number': coop_data['corporation_number']
            })
    
    return overlaps

def main():
    print("=" * 80)
    print("Cross-Checking Charities with Federal Businesses")
    print("=" * 80)
    print()
    
    # File paths
    charities_file = 'Charities_results_2025-11-09-14-17-45.txt'
    nonprofits_file = 'federal-non-for-profit-Ontario.csv'
    cooperatives_file = 'federal-cooperative-Ontario.csv'
    
    # Load data
    print("Loading charities data...")
    charities = load_charities(charities_file)
    print(f"  - Loaded {len(charities)} charities with valid business numbers")
    
    print("\nLoading federal non-profits data...")
    nonprofits = load_csv_businesses(nonprofits_file, 'Federal Non-Profit')
    print(f"  - Loaded {len(nonprofits)} non-profits with valid business numbers")
    
    print("\nLoading federal cooperatives data...")
    cooperatives = load_csv_businesses(cooperatives_file, 'Federal Cooperative')
    print(f"  - Loaded {len(cooperatives)} cooperatives with valid business numbers")
    
    # Verify business number as unique identifier
    print("\n" + "=" * 80)
    print("Verification: Are business numbers unique within each dataset?")
    print("=" * 80)
    
    # Sample some business numbers for verification
    print("\nSample Business Numbers from each dataset:")
    print("\nCharities (first 5):")
    for i, (bn, data) in enumerate(list(charities.items())[:5]):
        print(f"  {data['bn_full']} -> {bn} ({data['name'][:50]}...)")
    
    print("\nNon-Profits (first 5):")
    for i, (bn, data) in enumerate(list(nonprofits.items())[:5]):
        print(f"  {data['bn_full']} -> {bn} ({data['corporate_name'][:50]}...)")
    
    print("\nCooperatives (first 5):")
    for i, (bn, data) in enumerate(list(cooperatives.items())[:5]):
        print(f"  {data['bn_full']} -> {bn} ({data['corporate_name'][:50]}...)")
    
    # Find overlaps
    print("\n" + "=" * 80)
    print("Finding Overlaps...")
    print("=" * 80)
    overlaps = find_overlaps(charities, nonprofits, cooperatives)
    
    print(f"\nFound {len(overlaps)} overlaps!")
    
    # Save to CSV
    output_file = 'charity_business_overlaps.csv'
    
    if overlaps:
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = [
                'business_number',
                'business_type',
                'charity_name',
                'corporate_name',
                'charity_bn_full',
                'business_bn_full',
                'charity_status',
                'charity_type',
                'charity_city',
                'charity_province',
                'corporation_number'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(overlaps)
        
        print(f"\nResults saved to: {output_file}")
        
        # Display summary
        print("\n" + "=" * 80)
        print("Summary of Overlaps by Business Type:")
        print("=" * 80)
        
        type_counts = defaultdict(int)
        for overlap in overlaps:
            type_counts[overlap['business_type']] += 1
        
        for btype, count in type_counts.items():
            print(f"  {btype}: {count}")
        
        # Display first few overlaps
        print("\n" + "=" * 80)
        print("Sample Overlaps (first 10):")
        print("=" * 80)
        for i, overlap in enumerate(overlaps[:10], 1):
            print(f"\n{i}. Business Number: {overlap['business_number']}")
            print(f"   Type: {overlap['business_type']}")
            print(f"   Charity Name: {overlap['charity_name']}")
            print(f"   Corporate Name: {overlap['corporate_name']}")
            print(f"   Location: {overlap['charity_city']}, {overlap['charity_province']}")
    else:
        print("\nNo overlaps found.")
    
    print("\n" + "=" * 80)
    print("Analysis Complete!")
    print("=" * 80)

if __name__ == "__main__":
    main()
