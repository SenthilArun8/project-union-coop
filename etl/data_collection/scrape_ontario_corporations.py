"""
Concurrent Playwright scraper for Non-Profit Corporations.
Refactored to use a single persistent session per Business Type.
Flow:
1. Open Browser/Tab for a Business Type.
2. Set Filters (Advanced -> Corp -> Active -> Type).
3. Iterate 'a' through 'z' in the SAME tab.
4. Save results after each search.
"""

import asyncio
import os
import sys
import time
import string
import random
from typing import List, Dict, Set, Optional, Callable
from playwright_scraper_utils import ConcurrentPlaywrightScraper, SearchResult
from playwright.async_api import Page
from bs4 import BeautifulSoup

# Configuration
SAVE_DEBUG_FILES = True
OUTPUT_FOLDER = 'business_lookup_output'
# We process 3 business types concurrently, each in its own long-running session
MAX_CONCURRENT = 3 

# Common User Agents for rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/109.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0'
]

def ensure_output_folder():
    """Create the output folder if it doesn't exist."""
    if SAVE_DEBUG_FILES and not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
        print(f"Created output folder: {OUTPUT_FOLDER}")

class FilteredConcurrentScraper(ConcurrentPlaywrightScraper):
    """
    Extended scraper that maintains a session per business type.
    """
    
    async def start(self):
        """Initialize browser pool and contexts with stealthier settings."""
        from playwright.async_api import async_playwright
        self.playwright = await async_playwright().start()
        
        # Create multiple browser instances for better isolation
        for i in range(self.browser_pool_size):
            # Use minimal args to look more like a real browser
            browser = await self.playwright.chromium.launch(
                headless=self.headless,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--start-maximized',
                    '--no-default-browser-check'
                ]
            )
            self.browser_pool.append(browser)
            
            # Create multiple contexts per browser
            contexts_per_browser = max(1, self.max_concurrent // self.browser_pool_size)
            for j in range(contexts_per_browser):
                # Create context with stealth settings
                context = await browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent=random.choice(USER_AGENTS),
                    java_script_enabled=True,
                    locale='en-US',
                    timezone_id='America/Toronto'
                )
                
                # Apply stealth scripts to every context
                await context.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                """)
                
                self.context_pool.append(context)
        
        print(f"Initialized {len(self.browser_pool)} browsers with {len(self.context_pool)} contexts (Stealth Mode)")

    async def process_business_type_session(self, business_type: str, letters: List[str], context_id: int, result_callback: Callable, start_delay: float = 0):
        """
        Runs a full session for a single business type:
        1. Sets up filters once.
        2. Iterates through all letters.
        """
        if start_delay > 0:
            print(f"[Context {context_id}] ⏳ Waiting {start_delay}s before starting...")
            await asyncio.sleep(start_delay)

        # Select context from pool
        context = self.context_pool[context_id % len(self.context_pool)]
        page = await context.new_page()
        
        try:
            print(f"[Context {context_id}] 🚀 Starting session for '{business_type}'")
            
            # --- INITIAL SETUP & FILTERS ---

# ... (lines 104-367 remain unchanged, I will skip them in the replacement to keep it clean, but wait, replace_file_content needs contiguous block. 
# I will split this into two edits if needed, or just replace the function definition start and the call site.
# Actually, I can just replace the function definition line and the first few lines, and then separately replace the call loop.
# But wait, I can't do multiple edits in one replace_file_content unless I use multi_replace.
# I will use multi_replace_file_content.


            
            # --- INITIAL SETUP & FILTERS ---
            
            # 1. Navigate
            search_url = "https://www.appmybizaccount.gov.on.ca/onbis/master/entry.pub?applicationCode=onbis-master&businessService=registerItemSearch"
            await page.goto(search_url, wait_until='domcontentloaded')
            await asyncio.sleep(2.0)

            # Cookie handling
            try:
                if await page.query_selector("button:has-text('Accept all')"):
                    await page.click("button:has-text('Accept all')", timeout=2000)
            except:
                pass

            # 2. Click "Advanced"
            print(f"[Context {context_id}] Setting filters...")
            try:
                advanced_clicked = False
                for selector in ["#expandonodeW297", "text=Advanced", ".advanced-search-toggle"]:
                    if await page.query_selector(selector):
                        await page.click(selector, timeout=2000)
                        advanced_clicked = True
                        await asyncio.sleep(1.0)
                        break
                if not advanced_clicked:
                    print(f"[Context {context_id}] Warning: Could not click 'Advanced'")
            except Exception as e:
                print(f"[Context {context_id}] Error clicking Advanced: {e}")

            # 3. Select Register -> Corporations
            try:
                await page.wait_for_selector("#SourceAppCode", timeout=10000)
                await page.select_option("#SourceAppCode", label="Corporations")
                await asyncio.sleep(3.0) # Wait for Business Type to load
            except Exception as e:
                print(f"[Context {context_id}] Error setting Register: {e}")
                return # Critical failure

            # 4. Select Business Type
            try:
                found_type = False
                for attempt in range(5):
                    if await page.query_selector("#EntitySubTypeCode"):
                        options = await page.eval_on_selector_all("#EntitySubTypeCode option", "opts => opts.map(o => o.text)")
                        if any(business_type in opt for opt in options):
                            await page.select_option("#EntitySubTypeCode", label=business_type)
                            found_type = True
                            print(f"[Context {context_id}] ✅ Filter Set: '{business_type}'")
                            break
                    await asyncio.sleep(2.0)
                
                if not found_type:
                    print(f"[Context {context_id}] ❌ Failed to set Business Type: {business_type}")
                    return
            except Exception as e:
                print(f"[Context {context_id}] Error setting Business Type: {e}")
                return

            # 5. Scroll & Select Status -> Active
            try:
                await page.evaluate("window.scrollBy(0, 300)")
                await asyncio.sleep(0.5)
                if await page.query_selector("#Status"):
                    await page.select_option("#Status", label="Active")
                    print(f"[Context {context_id}] ✅ Filter Set: Status 'Active'")
                else:
                    print(f"[Context {context_id}] Warning: Status dropdown not found")
            except Exception as e:
                print(f"[Context {context_id}] Error setting Status: {e}")

            # --- SEARCH LOOP ---
            print(f"[Context {context_id}] Starting search loop for {len(letters)} letters...")
            
            for letter in letters:
                start_time = time.time()
                try:
                    # Check for CAPTCHA
                    content = await page.content()
                    if "captcha" in content.lower() or "unblock" in content.lower() or "bot" in content.lower():
                        print(f"\n[Context {context_id}] ⚠️ CAPTCHA DETECTED! Pausing for 60s...")
                        for k in range(12):
                            await asyncio.sleep(5)
                            if await page.query_selector("#QueryString"):
                                print(f"[Context {context_id}] ✅ Captcha cleared. Resuming...")
                                break
                    
                    # Enter Letter
                    print(f"[Context {context_id}] Searching: '{letter}'")
                    await page.fill("#QueryString", letter)
                    
                    # Click Search
                    if await page.query_selector("#nodeW303"):
                        await page.click("#nodeW303")
                    else:
                        await page.click("button:has-text('Search')")
                    
                    # Wait for page to load after clicking search
                    print(f"[Context {context_id}] ⏳ Waiting for page to load...")
                    try:
                        # Wait for network to be idle (no more than 2 network connections for at least 500ms)
                        await page.wait_for_load_state('networkidle', timeout=30000)
                        print(f"[Context {context_id}] ✅ Page loaded")
                    except Exception as e:
                        print(f"[Context {context_id}] Warning: Network idle timeout: {e}")
                        # Fallback: just wait a bit
                        await asyncio.sleep(3.0)
                    
                    # Wait for Results (Robust)
                    # We wait for the results container OR "No results"
                    try:
                        results_loc = page.locator(".appMinimalBox.ItemBox").first
                        no_results_loc = page.locator("text=/No results found|No matches found/i")
                        
                        await results_loc.or_(no_results_loc).wait_for(state="attached", timeout=30000)
                        print(f"[Context {context_id}] ✅ Results displayed")
                    except Exception as e:
                        print(f"[Context {context_id}] Warning: Timeout waiting for results for '{letter}': {e}")

                    # Expand Page Size to 200 (Only for 'a', effectively once per session)
                    if letter == 'a':
                        try:
                            # Only try if we actually found results
                            if await page.locator(".appMinimalBox.ItemBox").count() > 0:
                                print(f"[Context {context_id}] 📄 Setting Page Size to 200...")
                                # Select value "4" (200 items)
                                await page.select_option(".appSearchPageSize select", value="4")
                                
                                # Wait for the update to complete
                                await asyncio.sleep(3.0)
                                await results_loc.wait_for(state="attached", timeout=30000)
                                print(f"[Context {context_id}] ✅ Page Size updated")
                        except Exception as e:
                            print(f"[Context {context_id}] Warning: Could not set page size: {e}")

                    # Capture Data
                    html_content = await page.content()
                    search_time = time.time() - start_time
                    
                    success = True
                    if "No results found" in html_content or "No matches found" in html_content:
                        pass # Success but empty
                    
                    result = SearchResult(
                        business_name=f"{letter} ({business_type})",
                        html_content=html_content,
                        success=success,
                        search_time=search_time
                    )
                    
                    # Save Result via Callback
                    await result_callback(result)
                    
                    # Random delay between searches
                    await asyncio.sleep(random.uniform(2.0, 5.0))
                    
                except Exception as e:
                    print(f"[Context {context_id}] Error processing letter '{letter}': {e}")
                    # Try to recover? Maybe refresh? 
                    # For now, just continue to next letter
                    await asyncio.sleep(5)

        finally:
            await page.close()
            print(f"[Context {context_id}] Session ended for '{business_type}'")


async def process_non_profits():
    """Main processing function."""
    ensure_output_folder()
    output_dir = os.path.join(OUTPUT_FOLDER, 'non_profit_lookups')
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    report_file = os.path.join(output_dir, f'non_profit_search_report_{timestamp}.txt')
    
    letters = list(string.ascii_lowercase) # a-z
    business_types = [
        "Co-operative Non-Share",
        "Co-operative with Share",
        "Not-for-Profit Corporation"
    ]
    
    print(f"🚀 Starting concurrent sessions for {len(business_types)} business types...")
    
    # Shared lock for writing to the report file
    file_lock = asyncio.Lock()
    
    # Callback to handle saving results
    async def save_result(res: SearchResult):
        async with file_lock:
            with open(report_file, 'a', encoding='utf-8') as f:
                status = "✅ Found" if res.success and "No results found" not in res.html_content else "❌ No Data"
                if not res.success: status = "⚠️ Error"
                
                print(f"   [Saved] {status}: {res.business_name}")
                
                f.write(f"SEARCH: {res.business_name}\n")
                f.write("-" * 40 + "\n")
                f.write(f"Status: {status}\n")
                f.write(f"Time: {res.search_time:.2f}s\n")
                
                if res.success:
                    # Save HTML
                    safe_name = res.business_name.replace(' ', '_').replace('(', '').replace(')', '')
                    html_filename = os.path.join(output_dir, f"result_{safe_name}_{timestamp}.html")
                    with open(html_filename, 'w', encoding='utf-8') as hf:
                        hf.write(res.html_content)
                    f.write(f"Saved HTML: {os.path.basename(html_filename)}\n")
                    
                    # Parse Data
                    if "No results found" not in res.html_content:
                        try:
                            soup = BeautifulSoup(res.html_content, 'html.parser')
                            # Corrected selector based on HTML inspection
                            result_blocks = soup.select('.appMinimalBox.ItemBox')
                            f.write(f"Results Found: {len(result_blocks)}\n\n")
                            
                            for idx, block in enumerate(result_blocks, 1):
                                f.write(f"--- Result #{idx} ---\n")
                                
                                # 1. Business Name
                                name_elem = block.select_one('.registerItemSearch-results-page-line-ItemBox-resultLeft-viewMenu span:nth-of-type(2)')
                                business_name = name_elem.get_text(strip=True) if name_elem else "N/A"
                                f.write(f"Business Name: {business_name}\n")
                                
                                # 2. Business Type
                                type_elem = block.select_one('.appMinimalAttr.EntitySubTypeCode .appMinimalValue')
                                b_type = type_elem.get_text(strip=True) if type_elem else "N/A"
                                f.write(f"Business Type: {b_type}\n")
                                
                                # 3. Amalgamation/Incorporation Date
                                date_elem = block.select_one('.appMinimalAttr.RegistrationDate .appMinimalValue')
                                date_val = date_elem.get_text(strip=True) if date_elem else "N/A"
                                f.write(f"Amalgamation/Inc. Date: {date_val}\n")
                                
                                # 4. Location
                                loc_elem = block.select_one('.addressSearchResultBox .appAttrValue')
                                location = loc_elem.get_text(strip=True) if loc_elem else "N/A"
                                f.write(f"Location: {location}\n")
                                
                                # 5. Status
                                status_elem = block.select_one('.statusSearchResult .appMinimalAttr.Status .appMinimalValue')
                                status_val = status_elem.get_text(strip=True) if status_elem else "N/A"
                                f.write(f"Status: {status_val}\n")
                                
                                f.write("\n")
                        except Exception as e:
                            f.write(f"Error parsing HTML: {e}\n")
                else:
                    f.write(f"Error: {res.error_message}\n")
                
                f.write("\n" + "="*60 + "\n\n")

    # Initialize report file
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("NON-PROFIT CORPORATION SEARCH REPORT\n")
        f.write("=" * 60 + "\n\n")

    # Run sessions
    async with FilteredConcurrentScraper(max_concurrent=MAX_CONCURRENT, headless=False) as scraper:
        tasks = []
        for i, b_type in enumerate(business_types):
            # Launch a session for each business type with a staggered start
            # Wait 7 seconds between each launch to avoid detection
            delay = i * 7.0
            tasks.append(scraper.process_business_type_session(b_type, letters, i, save_result, start_delay=delay))
        
        await asyncio.gather(*tasks)

    print(f"\n🎉 Completed! Report saved to: {report_file}")

if __name__ == "__main__":
    try:
        asyncio.run(process_non_profits())
    except KeyboardInterrupt:
        print("\nStopped by user.")
    except Exception as e:
        print(f"\nFatal Error: {e}")
        import traceback
        traceback.print_exc()
