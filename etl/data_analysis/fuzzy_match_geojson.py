#!/usr/bin/env python3
"""
GeoJSON Owner Name Fuzzy Matcher
Matches owner names from GeoJSON with business data from JSON and CSV files
"""

import json
import csv
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from rapidfuzz import fuzz
from rapidfuzz import process

# Configuration
FUZZY_THRESHOLD = 85  # Minimum similarity score (0-100)
OUTPUT_DIR = Path(__file__).parent


def load_geojson(file_path):
    """Load and parse GeoJSON file"""
    print(f"Loading GeoJSON from: {file_path}")
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Extract all unique owner names
    owner_names = set()
    feature_count = 0
    
    if 'features' in data:
        for feature in data['features']:
            if 'properties' in feature and 'OWNERNAME' in feature['properties']:
                owner_name = feature['properties']['OWNERNAME']
                if owner_name and owner_name.strip() and owner_name.upper() != 'PRIVATE':
                    owner_names.add(owner_name.strip())
            feature_count += 1
    
    print(f"Loaded {feature_count:,} features from GeoJSON")
    print(f"Found {len(owner_names):,} unique owner names (excluding 'PRIVATE')")
    return sorted(owner_names)


def load_business_json(file_path):
    """Load business data from JSON file"""
    print(f"\nLoading business data from: {file_path}")
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Extract business names
    businesses = {}
    for entry in data:
        name = entry.get('Business Name', '')
        if name:
            businesses[name] = {
                'name': name,
                'type': entry.get('Business Type', ''),
                'corp_number': entry.get('Corporation Number', ''),
                'location': entry.get('Location', ''),
                'status': entry.get('Status', ''),
                'source': 'all_businesses.json'
            }
    
    print(f"Loaded {len(businesses):,} businesses from JSON")
    return businesses


def load_csv_files(output_dir):
    """Load business data from all CSV files"""
    businesses = {}
    csv_files = list(output_dir.glob('*.csv'))
    
    print(f"\nLoading data from {len(csv_files)} CSV files...")
    
    for csv_file in csv_files:
        print(f"  Reading: {csv_file.name}")
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Try different possible name columns
                    name = (row.get('charity_name') or 
                           row.get('corporate_name') or 
                           row.get('business_name') or 
                           row.get('Business Name') or 
                           row.get('name') or '')
                    
                    if name and name not in businesses:
                        businesses[name] = {
                            'name': name,
                            'type': row.get('business_type', ''),
                            'corp_number': row.get('corporation_number', ''),
                            'location': row.get('charity_city', '') or row.get('location', ''),
                            'status': row.get('charity_status', '') or row.get('status', ''),
                            'source': csv_file.name
                        }
        except Exception as e:
            print(f"    Warning: Could not read {csv_file.name}: {e}")
    
    print(f"Loaded {len(businesses):,} unique businesses from CSV files")
    return businesses


def fuzzy_match_names(owner_names, business_data, threshold=FUZZY_THRESHOLD):
    """
    Perform fuzzy matching between owner names and business names
    Returns matches and non-matches
    """
    print(f"\nPerforming fuzzy matching (threshold: {threshold})...")
    
    business_names = list(business_data.keys())
    matches = []
    no_matches = []
    
    total = len(owner_names)
    for idx, owner_name in enumerate(owner_names, 1):
        if idx % 100 == 0:
            print(f"  Progress: {idx}/{total} ({(idx/total)*100:.1f}%)")
        
        # Get best match using fuzzy matching
        best_match = process.extractOne(
            owner_name, 
            business_names, 
            scorer=fuzz.token_sort_ratio
        )
        
        if best_match and best_match[1] >= threshold:
            match_name, score = best_match[0], best_match[1]
            business_info = business_data[match_name]
            matches.append({
                'geojson_name': owner_name,
                'matched_name': match_name,
                'similarity_score': score,
                'business_type': business_info['type'],
                'corporation_number': business_info['corp_number'],
                'location': business_info['location'],
                'status': business_info['status'],
                'source_file': business_info['source']
            })
        else:
            no_matches.append({
                'geojson_name': owner_name,
                'best_partial_match': best_match[0] if best_match else 'None',
                'partial_score': best_match[1] if best_match else 0
            })
    
    print(f"\nMatching complete:")
    print(f"  Matches found: {len(matches):,}")
    print(f"  No matches: {len(no_matches):,}")
    
    return matches, no_matches


def analyze_geojson_columns(file_path):
    """Analyze and display all columns/properties in the GeoJSON"""
    print("\n" + "=" * 80)
    print("GEOJSON STRUCTURE ANALYSIS")
    print("=" * 80)
    
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if 'features' not in data or not data['features']:
        print("No features found in GeoJSON")
        return
    
    # Get all unique property keys
    all_keys = set()
    for feature in data['features']:
        if 'properties' in feature:
            all_keys.update(feature['properties'].keys())
    
    print(f"\nAvailable properties/columns in GeoJSON:")
    for key in sorted(all_keys):
        print(f"  - {key}")
    
    # Sample a few entries
    print(f"\nSample entries (first 5 features):")
    for i, feature in enumerate(data['features'][:5], 1):
        print(f"\nFeature {i}:")
        if 'properties' in feature:
            for key, value in feature['properties'].items():
                print(f"  {key}: {value}")


def save_matches(matches, output_file):
    """Save matched results to CSV"""
    if not matches:
        print(f"\nNo matches to save to {output_file}")
        return
    
    fieldnames = [
        'geojson_name', 'matched_name', 'similarity_score',
        'business_type', 'corporation_number', 'location', 
        'status', 'source_file'
    ]
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(matches)
    
    print(f"\nMatches saved to: {output_file}")
    print(f"  Total matches: {len(matches):,}")


def save_no_matches(no_matches, output_file):
    """Save non-matched results to CSV"""
    if not no_matches:
        print(f"\nNo non-matches to save to {output_file}")
        return
    
    fieldnames = ['geojson_name', 'best_partial_match', 'partial_score']
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(no_matches)
    
    print(f"\nNon-matches saved to: {output_file}")
    print(f"  Total non-matches: {len(no_matches):,}")


def create_matched_geojson(geojson_file, matches, output_file):
    """Create a new GeoJSON file containing only matched properties"""
    print(f"\nCreating matched GeoJSON file...")
    
    # Load original GeoJSON
    with open(geojson_file, 'r', encoding='utf-8') as f:
        geojson_data = json.load(f)
    
    # Get set of matched owner names for fast lookup
    matched_names = {match['geojson_name'] for match in matches}
    
    # Filter features to only include matches
    matched_features = []
    for feature in geojson_data.get('features', []):
        if 'properties' in feature:
            owner_name = feature['properties'].get('OWNERNAME', '').strip()
            if owner_name in matched_names:
                # Add matched business info to properties
                match_info = next((m for m in matches if m['geojson_name'] == owner_name), None)
                if match_info:
                    feature['properties']['MATCHED_BUSINESS_NAME'] = match_info['matched_name']
                    feature['properties']['BUSINESS_TYPE'] = match_info['business_type']
                    feature['properties']['CORPORATION_NUMBER'] = match_info['corporation_number']
                    feature['properties']['MATCH_SCORE'] = match_info['similarity_score']
                    feature['properties']['DATA_SOURCE'] = match_info['source_file']
                matched_features.append(feature)
    
    # Create new GeoJSON with matched features
    output_geojson = {
        'type': 'FeatureCollection',
        'features': matched_features
    }
    
    # Save to file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_geojson, f, indent=2)
    
    print(f"Matched GeoJSON saved to: {output_file}")
    print(f"  Total matched properties: {len(matched_features):,}")
    print(f"  Properties from {len(matched_names)} unique matched businesses")


def generate_summary_report(owner_names, matches, no_matches, output_file):
    """Generate a summary report of the matching process"""
    report_lines = []
    
    def log(message):
        print(message)
        report_lines.append(message)
    
    log("=" * 80)
    log("GEOJSON BUSINESS MATCHING SUMMARY REPORT")
    log(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 80)
    log("")
    
    log(f"Total unique owner names in GeoJSON: {len(owner_names):,}")
    log(f"Matches found: {len(matches):,} ({(len(matches)/len(owner_names)*100):.1f}%)")
    log(f"No matches: {len(no_matches):,} ({(len(no_matches)/len(owner_names)*100):.1f}%)")
    log("")
    
    # Match quality distribution
    log("-" * 80)
    log("MATCH QUALITY DISTRIBUTION")
    log("-" * 80)
    
    if matches:
        score_ranges = {
            '95-100% (Excellent)': 0,
            '90-94% (Very Good)': 0,
            '85-89% (Good)': 0
        }
        
        for match in matches:
            score = match['similarity_score']
            if score >= 95:
                score_ranges['95-100% (Excellent)'] += 1
            elif score >= 90:
                score_ranges['90-94% (Very Good)'] += 1
            else:
                score_ranges['85-89% (Good)'] += 1
        
        for range_name, count in score_ranges.items():
            percentage = (count / len(matches)) * 100 if matches else 0
            log(f"  {range_name}: {count:,} ({percentage:.1f}%)")
    log("")
    
    # Source file distribution
    log("-" * 80)
    log("MATCHES BY SOURCE FILE")
    log("-" * 80)
    
    if matches:
        source_counts = defaultdict(int)
        for match in matches:
            source_counts[match['source_file']] += 1
        
        for source, count in sorted(source_counts.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / len(matches)) * 100
            log(f"  {source}: {count:,} ({percentage:.1f}%)")
    log("")
    
    # Business type distribution for matches
    log("-" * 80)
    log("MATCHES BY BUSINESS TYPE")
    log("-" * 80)
    
    if matches:
        type_counts = defaultdict(int)
        for match in matches:
            btype = match['business_type'] or 'Unknown'
            type_counts[btype] += 1
        
        for btype, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / len(matches)) * 100
            log(f"  {btype}: {count:,} ({percentage:.1f}%)")
    log("")
    
    # Top 20 matches by score
    log("-" * 80)
    log("TOP 20 BEST MATCHES")
    log("-" * 80)
    
    if matches:
        sorted_matches = sorted(matches, key=lambda x: x['similarity_score'], reverse=True)
        for i, match in enumerate(sorted_matches[:20], 1):
            log(f"{i}. {match['geojson_name']}")
            log(f"   → {match['matched_name']} ({match['similarity_score']}%)")
            log(f"   Type: {match['business_type']}, Source: {match['source_file']}")
    
    log("")
    log("=" * 80)
    log("END OF REPORT")
    log("=" * 80)
    
    # Save report
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report_lines))
    
    print(f"\nSummary report saved to: {output_file}")


def main():
    """Main execution function"""
    # File paths
    geojson_file = Path(__file__).parent.parent.parent / 'quick_scrap' / 'Property_Ownership_Public_8585062059551015044.geojson'
    output_dir = Path(__file__).parent.parent / 'output'
    json_file = output_dir / 'all_businesses.json'
    
    # Check if files exist
    if not geojson_file.exists():
        print(f"Error: GeoJSON file not found at {geojson_file}")
        return
    
    if not json_file.exists():
        print(f"Error: JSON file not found at {json_file}")
        return
    
    # Analyze GeoJSON structure
    analyze_geojson_columns(geojson_file)
    
    # Load data
    owner_names = load_geojson(geojson_file)
    business_json = load_business_json(json_file)
    business_csv = load_csv_files(output_dir)
    
    # Combine all business data (CSV takes precedence for duplicates)
    all_businesses = {**business_json, **business_csv}
    print(f"\nTotal unique businesses combined: {len(all_businesses):,}")
    
    # Perform fuzzy matching
    matches, no_matches = fuzzy_match_names(owner_names, all_businesses)
    
    # Generate output files with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    matches_file = OUTPUT_DIR / f'geojson_matches_{timestamp}.csv'
    no_matches_file = OUTPUT_DIR / f'geojson_no_matches_{timestamp}.csv'
    summary_file = OUTPUT_DIR / f'geojson_matching_summary_{timestamp}.txt'
    matched_geojson_file = OUTPUT_DIR / f'geojson_matched_properties_{timestamp}.geojson'
    
    # Save results
    save_matches(matches, matches_file)
    save_no_matches(no_matches, no_matches_file)
    generate_summary_report(owner_names, matches, no_matches, summary_file)
    create_matched_geojson(geojson_file, matches, matched_geojson_file)
    
    print("\n" + "=" * 80)
    print("PROCESSING COMPLETE")
    print("=" * 80)
    print("\nOutput files generated:")
    print(f"  1. Matches CSV: {matches_file.name}")
    print(f"  2. No matches CSV: {no_matches_file.name}")
    print(f"  3. Summary report: {summary_file.name}")
    print(f"  4. Matched GeoJSON: {matched_geojson_file.name}")


if __name__ == "__main__":
    main()
