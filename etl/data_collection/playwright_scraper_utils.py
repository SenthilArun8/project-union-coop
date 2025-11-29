"""
Concurrent web scraper using Playwright with multiple browser contexts.
Optimized for parallel business registry searches.
"""

import asyncio
import os
import time
from playwright.async_api import async_playwright, Page, Browser, BrowserContext
from typing import Optional, List, Dict
import concurrent.futures
from dataclasses import dataclass
from datetime import datetime

# Configuration
SAVE_DEBUG_FILES = True
OUTPUT_FOLDER = 'business_lookup_output'
MAX_CONCURRENT_SEARCHES = 5  # Adjust based on your system and website limits
BROWSER_POOL_SIZE = 3  # Number of browser instances to maintain

@dataclass
class SearchResult:
    business_name: str
    html_content: str
    success: bool
    error_message: str = ""
    search_time: float = 0.0


class ConcurrentPlaywrightScraper:
    """
    High-performance web scraper with concurrent search capabilities.
    """
    
    def __init__(self, max_concurrent: int = MAX_CONCURRENT_SEARCHES, 
                 browser_pool_size: int = BROWSER_POOL_SIZE, headless: bool = False):
        self.max_concurrent = max_concurrent
        self.browser_pool_size = browser_pool_size
        self.headless = headless
        self.playwright = None
        self.browser_pool: List[Browser] = []
        self.context_pool: List[BrowserContext] = []
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
    async def __aenter__(self):
        """Async context manager entry."""
        await self.start()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
    
    async def start(self):
        """Initialize browser pool and contexts."""
        self.playwright = await async_playwright().start()
        
        # Create multiple browser instances for better isolation
        for i in range(self.browser_pool_size):
            browser = await self.playwright.chromium.launch(
                headless=self.headless,
                args=[
                    '--disable-dev-shm-usage',
                    '--disable-extensions',
                    '--disable-gpu',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-web-security',
                    '--disable-background-networking',
                    '--disable-default-apps',
                    '--disable-sync',
                ]
            )
            self.browser_pool.append(browser)
            
            # Create multiple contexts per browser
            contexts_per_browser = max(1, self.max_concurrent // self.browser_pool_size)
            for j in range(contexts_per_browser):
                context = await browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                )
                self.context_pool.append(context)
        
        print(f"Initialized {len(self.browser_pool)} browsers with {len(self.context_pool)} contexts")
    
    async def close(self):
        """Close all browsers and contexts."""
        for context in self.context_pool:
            try:
                await context.close()
            except Exception as e:
                print(f"Error closing context: {e}")
        
        for browser in self.browser_pool:
            try:
                await browser.close()
            except Exception as e:
                print(f"Error closing browser: {e}")
        
        if self.playwright:
            await self.playwright.stop()
    
    async def _get_available_context(self) -> BrowserContext:
        """Get an available context from the pool."""
        # Simple round-robin selection
        # In production, you might want more sophisticated load balancing
        context_index = len(self.context_pool) % len(self.context_pool) if self.context_pool else 0
        return self.context_pool[context_index]
    
    async def search_business_optimized(self, business_name: str, context_id: int = 0) -> SearchResult:
        """
        Optimized single business search with reduced wait times and better error handling.
        """
        start_time = time.time()
        
        async with self.semaphore:  # Limit concurrent searches
            try:
                # Get context from pool
                context = self.context_pool[context_id % len(self.context_pool)]
                page = await context.new_page()
                
                try:
                    # Set shorter timeouts for faster failure detection
                    page.set_default_timeout(15000)  # 15 seconds
                    page.set_default_navigation_timeout(30000)  # 30 seconds
                    
                    print(f"[Context {context_id}] Searching for: {business_name}")
                    
                    # Navigate to search page
                    search_url = "https://www.appmybizaccount.gov.on.ca/onbis/master/entry.pub?applicationCode=onbis-master&businessService=registerItemSearch"
                    
                    await page.goto(search_url, wait_until='domcontentloaded')  # Faster than 'networkidle'
                    
                    # Quick cookie handling
                    try:
                        await page.click("button:has-text('Accept all')", timeout=2000)
                        await asyncio.sleep(0.5)
                    except:
                        pass  # Cookie banner might not be present
                    
                    # Fill search box
                    await page.wait_for_selector("#QueryString", timeout=10000)
                    await page.fill("#QueryString", business_name)
                    
                    # Try different search button selectors (same as web_scraper_playwright.py)
                    search_button_selectors = [
                        "button[type='submit']",
                        "input[type='submit']",
                        "button:has-text('Search')",
                        "button:has-text('SEARCH')",
                        "input[value='Search']",
                        "input[value='SEARCH']",
                        "#nodeW20"  # Original ID as fallback
                    ]
                    
                    search_clicked = False
                    for selector in search_button_selectors:
                        try:
                            button = page.locator(selector)
                            if await button.count() > 0:
                                await button.click(timeout=5000)
                                print(f"[Context {context_id}] Search button clicked using {selector}")
                                search_clicked = True
                                break
                        except Exception as e:
                            continue
                    
                    if not search_clicked:
                        raise Exception("Could not find or click the search button")
                    
                    # Wait for page to load after clicking
                    await page.wait_for_load_state('domcontentloaded')
                    
                    # Smart wait - check for content instead of fixed time
                    await self._wait_for_results_smart(page)
                    
                    # Get results
                    html_content = await page.content()
                    search_time = time.time() - start_time
                    
                    print(f"[Context {context_id}] Completed {business_name} in {search_time:.2f}s")
                    
                    return SearchResult(
                        business_name=business_name,
                        html_content=html_content,
                        success=True,
                        search_time=search_time
                    )
                
                finally:
                    await page.close()
                    
            except Exception as e:
                search_time = time.time() - start_time
                print(f"[Context {context_id}] Error searching {business_name}: {e}")
                return SearchResult(
                    business_name=business_name,
                    html_content="",
                    success=False,
                    error_message=str(e),
                    search_time=search_time
                )
    
    async def _wait_for_results_smart(self, page: Page, max_wait: float = 8.0):
        """
        Smart waiting that checks for actual content instead of fixed delays.
        """
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            # Check if results are loaded
            try:
                # Look for result containers
                results = await page.query_selector_all("div.registerItemSearch-results-page-line-ItemBox")
                if results:
                    print("Results detected, stopping wait")
                    return
                
                # Check for "no results" message
                no_results = await page.query_selector("text=/No results found|No matches found/i")
                if no_results:
                    print("No results message detected")
                    return
                
                # Check if search is still processing
                loading = await page.query_selector("text=/Loading|Searching|Please wait/i")
                if not loading:
                    # If no loading indicator and we've waited a bit, probably done
                    if time.time() - start_time > 3.0:
                        return
                
                await asyncio.sleep(0.5)  # Check every 500ms
                
            except Exception as e:
                # If we can't check, just wait minimum time
                if time.time() - start_time > 3.0:
                    return
                await asyncio.sleep(0.5)
    
    async def search_multiple_businesses(self, business_names: List[str]) -> List[SearchResult]:
        """
        Search multiple businesses concurrently.
        """
        print(f"Starting concurrent search for {len(business_names)} businesses...")
        print(f"Max concurrent: {self.max_concurrent}, Contexts: {len(self.context_pool)}")
        
        # Create tasks with context assignment
        tasks = []
        for i, business_name in enumerate(business_names):
            context_id = i % len(self.context_pool)
            task = self.search_business_optimized(business_name, context_id)
            tasks.append(task)
        
        # Execute all searches concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        search_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                search_results.append(SearchResult(
                    business_name=business_names[i],
                    html_content="",
                    success=False,
                    error_message=str(result)
                ))
            else:
                search_results.append(result)
        
        return search_results


# Batch processing function
async def batch_search_businesses(business_names: List[str], 
                                batch_size: int = MAX_CONCURRENT_SEARCHES) -> List[SearchResult]:
    """
    Process businesses in batches to avoid overwhelming the server.
    """
    all_results = []
    
    async with ConcurrentPlaywrightScraper(max_concurrent=batch_size, headless=False) as scraper:
        for i in range(0, len(business_names), batch_size):
            batch = business_names[i:i + batch_size]
            print(f"\nProcessing batch {i//batch_size + 1}: {len(batch)} businesses")
            
            batch_results = await scraper.search_multiple_businesses(batch)
            all_results.extend(batch_results)
            
            # Brief pause between batches to be respectful to the server
            if i + batch_size < len(business_names):
                print("Pausing briefly between batches...")
                await asyncio.sleep(2)
    
    return all_results


# Synchronous wrapper for existing code compatibility
def search_multiple_businesses_sync(business_names: List[str]) -> List[SearchResult]:
    """
    Synchronous wrapper for batch business search.
    """
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError("Loop is closed")
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    return loop.run_until_complete(batch_search_businesses(business_names))


if __name__ == "__main__":
    # Example usage
    test_businesses = [
        "MTD Products Limited",
        "Union Co-op",
        "Concordia Club",
        "Grand River Hospital",
        "University of Waterloo"
    ]
    
    print(f"Testing concurrent search with {len(test_businesses)} businesses...")
    start_time = time.time()
    
    results = search_multiple_businesses_sync(test_businesses)
    
    total_time = time.time() - start_time
    successful = sum(1 for r in results if r.success)
    
    print(f"\n=== RESULTS ===")
    print(f"Total time: {total_time:.2f} seconds")
    print(f"Successful searches: {successful}/{len(results)}")
    print(f"Average time per search: {total_time/len(results):.2f} seconds")
    
    for result in results:
        status = "✅" if result.success else "❌"
        print(f"{status} {result.business_name}: {result.search_time:.2f}s")