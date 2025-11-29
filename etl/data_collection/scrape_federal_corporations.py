#!/usr/bin/env python3
import csv
from bs4 import BeautifulSoup
import requests
import time
import random
from typing import List, Dict, Optional

# A list of common user agents to rotate through
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/109.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/109.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.2 Safari/605.1.15',
]


def parse_html(html_content: str) -> List[Dict[str, str]]:
    """
    Parses the HTML content to extract corporation data.
    Assumes all corporations in the HTML are the ones to be extracted.

    Args:
        html_content: A string containing the HTML of the search results page.

    Returns:
        A list of dictionaries, where each dictionary represents an active
        corporation with its name, corporation number, and business number.
    """
    soup = BeautifulSoup(html_content, 'lxml')
    results = []
    
    # The search results are in an ordered list with list items
    # having the class 'pad-md row'
    list_items = soup.select('ol.list-unstyled > li.pad-md.row')

    # Since we filter for active corporations in the URL, we no longer need to check status here.
    for item in list_items:
        # Extract Corporate Name
        name_tag = item.find('a')
        if not name_tag:
            continue
        # Some names have English and French versions separated by <br>
        # We'll take the first one.
        corporate_name = name_tag.get_text(separator='|', strip=True).split('|')[0]

        # Extract Corporation Number
        corp_num_span = item.find('span', string=lambda t: t and 'Corporation number:' in t)
        corporation_number = corp_num_span.get_text(strip=True).replace('Corporation number:', '').strip()

        # Extract Business Number
        bus_num_span = item.find('span', string=lambda t: t and 'Business Number:' in t)
        business_number = bus_num_span.get_text(strip=True).replace('Business Number:', '').strip()

        results.append({
            'Corporate Name': corporate_name,
            'Corporation Number': corporation_number,
            'Business Number': business_number,
        })
            
    return results

def main():
    """
    Main function to read the HTML file, parse it, and write to a CSV.
    """
    # html_file_path = '/Users/rezababaee/01_Personal/01_Projects/04_Union_Coop/Search for a Federal Corporation - Online Filing Centre - Corporations Canada - Corporations - Innovation, Science and Economic Development Canada.html'
    csv_file_path = 'federal-non-for-profit.csv'
    base_url = 'https://ised-isde.canada.ca/cc/lgcy/fdrlCrpSrch.html'
    params = {
        'crpNm': '',
        'crpNmbr': '',
        'bsNmbr': '',
        'cProv': '', # ON: Ontario
        'cStatus': '1', # 1 = Active
        'cAct': '14' # 14 Canada Not-for-profit Corporations Act, 12 Canada Cooperatives Act
    }
    
    # Commented out section for reading from a local file
    # try:
    #     with open(html_file_path, 'r', encoding='utf-8') as f:
    #         html_content = f.read()
    # except FileNotFoundError:
    #     print(f"Error: The file '{html_file_path}' was not found.")
    #     return
    # except Exception as e:
    #     print(f"An error occurred while reading the file: {e}")
    #     return

    session = requests.Session()
    all_active_corporations = []
    page_number = 0
    max_retries = 5
    
    while True:
        print(f"Fetching page {page_number}...")
        params['p'] = page_number
        
        response = None
        for attempt in range(max_retries):
            try:
                # Rotate user agent and add a random delay
                headers = {'User-Agent': random.choice(USER_AGENTS)}
                response = session.get(base_url, params=params, headers=headers, timeout=20)
                response.raise_for_status() # Will raise an HTTPError for bad responses (4xx or 5xx)
                break # If request is successful, break the retry loop
            except requests.RequestException as e:
                print(f"  Attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt + 1 == max_retries:
                    print("Max retries reached. Aborting.")
                    break
                # Exponential backoff: wait 2s, 4s, 8s, ...
                wait_time = 2 ** (attempt + 1)
                print(f"  Waiting {wait_time} seconds before retrying...")
                time.sleep(wait_time)
        
        if response is None or not response.ok:
            break # Stop if all retries failed

        corporations_on_page = parse_html(response.text)
        if not corporations_on_page:
            print("No more results found. Stopping.")
            break
        
        all_active_corporations.extend(corporations_on_page)
        page_number += 1
        
        # Be polite and wait a bit before the next request
        time.sleep(random.uniform(1, 3))
    
    if not all_active_corporations:
        print("No active corporations found in the HTML file.")
        return

    # Write the data to a CSV file
    try:
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['Corporate Name', 'Corporation Number', 'Business Number']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            writer.writerows(all_active_corporations)
            
        print(f"\nSuccessfully extracted {len(all_active_corporations)} active corporations to '{csv_file_path}'.")
        
    except Exception as e:
        print(f"An error occurred while writing to the CSV file: {e}")


if __name__ == '__main__':
    main()
