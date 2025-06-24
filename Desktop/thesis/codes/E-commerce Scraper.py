# CODE WE FOLLOW
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup, SoupStrainer # Import SoupStrainer for efficient title check
from urllib.parse import urljoin, quote
import random
import time
import re
from fake_useragent import UserAgent
from typing import List, Dict, Optional, Any
import logging
import json
import os

# --- Configuration ---
LOGGING_LEVEL = logging.INFO
logging.basicConfig(level=LOGGING_LEVEL, format='%(asctime)s - %(levelname)s - [%(funcName)s] %(message)s')
SAVE_BLOCK_HTML = False # Set to True to save HTML of detected block pages for debugging

# --- Constants ---
DEFAULT_TIMEOUT = (10, 25)
MAX_RETRIES = 2 # Keep retries lower if blocks are common, rely on detection
BACKOFF_FACTOR = 1
BASE_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'cross-site',
    'Sec-Fetch-User': '?1',
    'TE': 'trailers',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': '',
    'DNT': '1',
}

# Keywords/patterns indicating CAPTCHA or block pages (lowercase)
BLOCK_INDICATORS = {
    'keywords': [
        'captcha', 'verify you are human', 'robot check', 'are you a robot',
        'access denied', 'security measure', 'unusual activity',
        'enter the characters', 'blocked', 'human verification',
        'prove you are human', 'automated queries', 'enable javascript',
        'enable cookies' # Often shown on simple block pages
    ],
    'url_patterns': {
        'amazon': ['/captcha/', '/errors/validatecaptcha'],
        'walmart': ['/blocked'],
        # Add specific URL patterns for other sites if known
        'ebay': ['/signin/s'], # Often redirects to signin on block
        'alibaba': ['/captcha', 'login.alibaba.com'] # Might redirect to login
    },
    'titles': [ # Common titles of block pages (lowercase)
        'robot check', 'verify your identity', 'security measure',
        'access denied', 'human verification', 'are you a robot?',
        'attention required!', 'cloudflare' # Cloudflare protection page
    ]
}


class RobustEcomSEMGenerator:
    def __init__(self):
        self.ua = UserAgent()
        self.session = self._create_session()
        self.ecom_sites = {
             'amazon': {
                'base_url': 'https://www.amazon.com',
                'search_url': 'https://www.amazon.com/s?k={query}',
                'product_url': 'https://www.amazon.com/dp/{id}',
                'selectors': { # Keep selectors as before
                    'products': [
                        'div.s-result-item[data-asin]',
                        'div[data-component-type="s-search-result"][data-asin]',
                        'div[data-component-type="s-search-result"]',
                    ],
                    'title': [
                        'span.a-size-medium.a-color-base.a-text-normal',
                        'h2 a span.a-text-normal',
                        'span[data-cy="title-recipe"] span.a-text-normal',
                        '.s-line-clamp-2 span',
                        'h2 a.a-link-normal',
                    ],
                    'price': [
                        'span.a-price[data-a-size="xl"] span.a-offscreen',
                        'span.a-price[data-a-size="l"] span.a-offscreen',
                        'span.a-price > span.a-offscreen',
                        'span[data-a-color="base"] span.a-offscreen',
                        '.a-price span.a-price-whole',
                        '.a-color-price',
                    ],
                    'link': [
                        'a.a-link-normal.s-underline-text.s-underline-link-text.s-link-style[href*="/dp/"]',
                        'h2 a.a-link-normal[href*="/dp/"]',
                        'a.a-link-normal[href*="/dp/"]',
                         '[data-component-type="s-product-image"] a[href*="/dp/"]',
                    ],
                    'rating': ['span.a-icon-alt', 'i.a-icon-star-small span.a-icon-alt', 'span[data-cy="reviews-ratings-slot"] span.a-size-base'],
                    'reviews': ['span.a-size-base.s-underline-text', 'span[data-cy="reviews-count"]', 'a.a-link-normal span.a-size-base[aria-label*="ratings"]', 'span.a-size-base[dir="auto"]']
                },
                'throttle': random.uniform(8, 15),
                'note': "Amazon scraping is unreliable due to anti-bot measures."
            },
            'walmart': {
                'base_url': 'https://www.walmart.com',
                'search_url': 'https://www.walmart.com/search?q={query}',
                'product_url': 'https://www.walmart.com/ip/{id}',
                'selectors': { # Keep selectors as before
                    'products': [
                        'div[data-item-id]',
                         'div.mb1.ph1.pa0-xl.bb.b--near-white',
                         'div[data-testid="list-view"] div[data-stack-index]',
                         'div.pa0.ph1.bg-white.pb1.pr2.pl2'
                         ],
                    'title': ['span[data-automation-id="product-title"]', 'a[data-testid="product-title-link"] span', 'a[link-identifier] span'],
                    'price': [
                        'div[data-testid="price-group"]',
                        '.mr2.lh-copy span[class*="f1 b"]',
                         'div[data-automation-id="product-price"] span.f1',
                         '.mr2.lh-copy span'
                         ],
                    'link': ['a[data-testid="product-title-link"]', 'a[link-identifier="itemClick"]', 'a[href*="/ip/"]'],
                    'rating': ['span.w_iUH7', 'span[data-testid="rating-module-average-rating"] span:first-child', 'span.rating-number span:first-child'],
                    'reviews': ['span[data-testid="rating-module-reviews-count"]', 'span.sans-serif.gray.f7', 'span.rating-number ~ span']
                },
                'throttle': random.uniform(4, 7),
            },
            'alibaba': {
                 'base_url': 'https://www.alibaba.com',
                 'search_url': 'https://www.alibaba.com/trade/search?SearchText={query}',
                 'product_url': 'https://www.alibaba.com/product-detail/{id}.html',
                 'selectors': { # Keep selectors as before
                     'products': [
                         'div.organic-gallery-offer-outter',
                         'div.list-gallery__item',
                         'div.item-main'
                     ],
                     'title': [
                         'h2.title > a',
                         'p.title > a',
                         'h2.title',
                         'p.title',
                         'div.title-text'
                     ],
                     'price': [
                         'div.price',
                         'span.price',
                         'p.price'
                     ],
                     'link': [
                         'h2.title a',
                         'p.title a',
                         'a.item-title'
                     ],
                     'rating': [
                         'span.seb-supplier-review-score',
                         'span.star-rate',
                         'div.star-rate',
                     ],
                     'reviews': [
                         'span.supplier-review-count',
                         'span.review-count',
                         'div.review-count'
                     ]
                 },
                 'throttle': random.uniform(6, 10),
                 'requires_proxy': False
             },
             'ebay': {
                 'base_url': 'https://www.ebay.com',
                 'search_url': 'https://www.ebay.com/sch/i.html?_nkw={query}',
                 'product_url': 'https://www.ebay.com/itm/{id}',
                 'selectors': { # Keep selectors as before
                    'products': ['li.s-item.s-item__pl-on-bottom[data-view]', 'li.s-item[data-view]', 'div.s-item__wrapper.clearfix', 'li.s-item'],
                    'title': [
                        'div.s-item__title > span[role="heading"]',
                        'h3.s-item__title span',
                        'h3.s-item__title',
                        'div.s-item__title'],
                    'price': [
                        'span.s-item__price',
                        'div.s-item__detail--primary span.s-item__price',
                        '.s-item__buy-it-now-price',
                    ],
                    'link': ['a.s-item__link[href]'],
                    'rating': ['span.clipped[aria-hidden="true"]', 'div.s-item__reviews span.clipped'],
                    'reviews': [
                        'span.s-item__reviews-count span:first-child',
                        'span.s-item__quantitySold',
                        'span.s-item__purchase-options-text',
                        'span.s-item__hotness span.BOLD',
                        'span.s-item__authorized-seller',
                        'span.SECONDARY_INFO',
                        'span.s-item__reviews-count',
                    ]
                 },
                 'throttle': random.uniform(4, 8),
             }
        }
        self._init_keyword_tools()

    def _get_request_headers(self) -> Dict[str, str]:
        """Generate request headers with a random User-Agent."""
        headers = BASE_HEADERS.copy()
        headers['User-Agent'] = self.ua.random
        return headers

    def _create_session(self) -> requests.Session:
        """Creates a requests session with retry logic."""
        session = requests.Session()
        retries = Retry(
            total=MAX_RETRIES,
            backoff_factor=BACKOFF_FACTOR,
            status_forcelist=[429, 503, 500, 502, 504], # Standard retry-worthy statuses
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            respect_retry_after_header=True
        )
        # Don't retry on 403 (Forbidden) as it's usually a hard block
        adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=retries)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        session.headers.update({k: v for k, v in BASE_HEADERS.items() if k != 'User-Agent'})
        return session

    # --- Helper Methods (_get_element_text, _get_element_attr, etc.) remain unchanged ---
    # --- Paste the helper methods from the previous response here ---
    def _get_element_text(self, item: Any, selectors: List[str]) -> Optional[str]:
        """Safely get text from an element using multiple selectors."""
        if not item: return None
        for selector in selectors:
            try:
                element = item.select_one(selector)
                if element:
                    text = element.get_text(strip=True)
                    if text:
                        return text
            except Exception as e:
                logging.debug(f"Error finding text with selector '{selector}': {e}")
                continue
        return None

    def _get_element_attr(self, item: Any, selectors: List[str], attribute: str) -> Optional[str]:
        """Safely get an attribute from an element using multiple selectors."""
        if not item: return None
        for selector in selectors:
            try:
                element = item.select_one(selector)
                if element:
                    attr_value = element.get(attribute)
                    if attr_value:
                        return attr_value
            except Exception as e:
                logging.debug(f"Error finding attribute '{attribute}' with selector '{selector}': {e}")
                continue
        return None

    def _clean_price(self, price_str: Optional[str]) -> str:
        """Extracts a numerical price from a string."""
        if not price_str:
            return "N/A"
        price_str = price_str.replace('\n', '').replace('\r', '') # Clean whitespace
        match = re.search(r'([\£\$\€]?\s?[\d,]+\.?\d*)', price_str)
        if match:
            price = match.group(1).strip()
            if '$' not in price and any(s in price_str.lower() for s in ['usd', '$']):
                 price = '$' + price.replace('$', '').strip()
            elif '£' not in price and any(s in price_str.lower() for s in ['gbp', '£']):
                 price = '£' + price.replace('£', '').strip()
            elif '€' not in price and any(s in price_str.lower() for s in ['eur', '€']):
                 price = '€' + price.replace('€', '').strip()
            return price
        match_num = re.search(r'([\d,]+\.?\d*)', price_str)
        if match_num:
             return match_num.group(1).strip()
        return "N/A"

    def _extract_rating(self, rating_str: Optional[str]) -> str:
        """Extracts a rating value (e.g., '4.5') from a string."""
        if not rating_str:
            return "N/A"
        match = re.search(r'(\d\.?\d*)\s*(?:out\s*of\s*5)?', rating_str)
        if match:
            return match.group(1)
        return "N/A"

    def _extract_reviews(self, reviews_str: Optional[str], site: str) -> Optional[int]:
        """Extracts the number of reviews from a string."""
        if not reviews_str:
            return None
        cleaned_str = re.sub(r'[^\d]', '', reviews_str)
        if cleaned_str.isdigit():
            try:
                return int(cleaned_str)
            except ValueError:
                return None
        match = re.search(r'([\d,]+)', reviews_str)
        if match:
            try:
                num_str = match.group(1).replace(',', '')
                return int(num_str)
            except ValueError:
                 pass
        if site == 'ebay':
             sold_match = re.search(r'(\d+)\s*sold', reviews_str, re.IGNORECASE)
             if sold_match:
                 try:
                     return int(sold_match.group(1))
                 except ValueError:
                     pass
             return None
        return None
    # --- End of Helper Methods ---


    def _format_url(self, url: str, site: str) -> str:
        """Formats URLs to match the exact requested format for each site"""
        if not url or url == "N/A":
            return "N/A"

        base_url = self.ecom_sites[site]['base_url']
        full_url = urljoin(base_url, url) # Ensure we have an absolute URL first

        # Amazon URL formatting
        if site == 'amazon':
            dp_match = re.search(r'/dp/([A-Z0-9]{10})', full_url)
            if dp_match:
                asin = dp_match.group(1)
                return f"https://www.amazon.com/dp/{asin}"
            gp_match = re.search(r'/gp/product/([A-Z0-9]{10})', full_url)
            if gp_match:
                 asin = gp_match.group(1)
                 return f"https://www.amazon.com/dp/{asin}"
            if '/s?' in full_url and 'k=' in full_url:
                return full_url
            logging.debug(f"Could not format Amazon URL: {full_url}")
            return full_url

        # Walmart URL formatting
        elif site == 'walmart':
            # Handle /ip/ G M W W / 7 7 7 8 4 3 6 4 1 format
            ip_match_variant = re.search(r'/ip/[^/]+/(\d+)', full_url)
            if ip_match_variant:
                product_id = ip_match_variant.group(1)
                return f"https://www.walmart.com/ip/{product_id}"
            # Handle standard /ip/ 12345 format
            ip_match_standard = re.search(r'/ip/(\d+)', full_url)
            if ip_match_standard:
                product_id = ip_match_standard.group(1)
                return f"https://www.walmart.com/ip/{product_id}"
            if '/search?' in full_url and 'q=' in full_url:
                 return full_url
            logging.debug(f"Could not format Walmart URL: {full_url}")
            return full_url

        # Alibaba URL formatting
        elif site == 'alibaba':
            pd_match = re.search(r'/product-detail/.*?(_\d+)\.html', full_url)
            if pd_match:
                 if '/product-detail/' in full_url:
                      clean_url = full_url.split('?')[0].split('.html')[0] + '.html'
                      return clean_url
            if '/trade/search?' in full_url and 'SearchText=' in full_url:
                return full_url
            logging.debug(f"Could not format Alibaba URL: {full_url}")
            return full_url

        # eBay URL formatting
        elif site == 'ebay':
            itm_match = re.search(r'/itm/(\d+)', full_url)
            if itm_match:
                item_id = itm_match.group(1)
                return f"https://www.ebay.com/itm/{item_id}"
            if '/sch/i.html?' in full_url and '_nkw=' in full_url:
                 return full_url
            logging.debug(f"Could not format eBay URL: {full_url}")
            return full_url

        # Fallback
        return full_url

    def _is_response_blocked(self, response: requests.Response, site: str) -> Optional[str]:
        """
        Checks if the response content or URL indicates a CAPTCHA or block page.
        Returns the reason string if blocked, None otherwise.
        """
        final_url_lower = response.url.lower()
        response_text_lower = response.text.lower() # Get text once for checks

        # 1. Check specific URL patterns for the site
        site_url_patterns = BLOCK_INDICATORS['url_patterns'].get(site, [])
        for pattern in site_url_patterns:
            if pattern in final_url_lower:
                return f"URL pattern '{pattern}' detected"

        # 2. Check common block page titles (efficient check)
        try:
            # Use SoupStrainer for efficiency - only parse the <title> tag
            strainer = SoupStrainer('title')
            soup_title_check = BeautifulSoup(response.content, 'lxml', parse_only=strainer) # Use lxml if installed
            if soup_title_check and soup_title_check.title and soup_title_check.title.string:
                page_title_lower = soup_title_check.title.string.lower().strip()
                for block_title in BLOCK_INDICATORS['titles']:
                    if block_title in page_title_lower:
                        return f"Title indicator '{block_title}' detected ('{soup_title_check.title.string.strip()}')"
        except Exception as e:
            logging.debug(f"Could not parse title for block check on {site}: {e}")


        # 3. Check common keywords in the response body (less efficient but broad)
        # Avoid checking huge responses completely if possible
        check_text_sample = response_text_lower[:10000] # Check first 10k characters
        for keyword in BLOCK_INDICATORS['keywords']:
            if keyword in check_text_sample:
                 # Further check: avoid false positives if keyword is part of product title/desc
                 # This is complex, so we accept some risk or refine keywords carefully
                 # Example refinement: ensure keyword isn't inside a product link/title tag nearby
                 # For now, we assume keyword presence is a strong indicator
                 return f"Content keyword '{keyword}' detected"

        # 4. Check for lack of expected product selectors (Advanced, optional)
        # If *none* of the primary product selectors are found, it *might* be a block page
        # config = self.ecom_sites[site]
        # soup_full = BeautifulSoup(response.content, 'lxml')
        # found_any_product = False
        # for selector in config['selectors']['products']:
        #      if soup_full.select_one(selector):
        #          found_any_product = True
        #          break
        # if not found_any_product:
        #      # Check if it's just a "no results" page vs a block page
        #      no_results_indicators = ["no results found", "did not match any products"]
        #      is_no_results = any(indicator in response_text_lower for indicator in no_results_indicators)
        #      if not is_no_results:
        #            return "No product selectors found, potentially a block/empty page"

        return None # Not blocked based on checks

    def _scrape_site(self, query: str, site: str, limit: int = 7) -> List[Dict[str, Any]]:
        """Scrape product data, now with CAPTCHA/block detection."""
        config = self.ecom_sites[site]
        # No need for specialized _scrape_alibaba if the main logic handles it
        # if site == 'alibaba':
        #     return self._scrape_alibaba(query, limit) # Keep if significant differences remain

        url = config['search_url'].format(query=quote(query))
        request_headers = self._get_request_headers()
        products = []
        html_debug_filename_base = f"debug_{site}_{query.replace(' ', '_')}"

        logging.info(f"Scraping {site.capitalize()} for '{query}'...")
        if config.get('note'):
            logging.warning(f"Note for {site.capitalize()}: {config['note']}")

        try:
            response = self.session.get(url, headers=request_headers, timeout=DEFAULT_TIMEOUT)
            logging.info(f"{site.capitalize()} Request URL: {url}")
            logging.info(f"{site.capitalize()} Final URL: {response.url} | Status: {response.status_code}")

            # --- Check for HTTP Errors first (4xx/5xx that weren't retried) ---
            if not response.ok: # Checks for status_code < 400
                 # Handle specific non-retried errors like 403 Forbidden
                 if response.status_code == 403:
                      logging.error(f"{site.capitalize()}: Access Forbidden (403). Likely a hard block. URL: {response.url}")
                 else:
                      logging.error(f"{site.capitalize()}: HTTP Error {response.status_code} {response.reason}. URL: {response.url}")
                 response.raise_for_status() # Raise error to be caught below

            # --- !! NEW: CAPTCHA/Block Detection !! ---
            block_reason = self._is_response_blocked(response, site)
            if block_reason:
                logging.warning(
                    f"{site.capitalize()}: CAPTCHA or explicit block page detected "
                    f"(Reason: {block_reason}, Status: {response.status_code}, URL: {response.url}). "
                    f"Skipping parsing."
                )
                if SAVE_BLOCK_HTML:
                    block_debug_filename = f"{html_debug_filename_base}_BLOCKED.html"
                    try:
                        with open(block_debug_filename, 'w', encoding='utf-8') as f:
                            f.write(response.text)
                        logging.info(f"Saved blocked page HTML to {block_debug_filename}")
                    except Exception as e_save:
                         logging.error(f"Failed to save blocked page HTML: {e_save}")
                return [] # Return empty list - crucial step

            # --- If not blocked, proceed with normal parsing ---
            logging.debug(f"Response OK and not blocked for {site}. Proceeding to parse.")
            # Optional: Save successful HTML for debugging parsing issues
            # success_debug_filename = f"{html_debug_filename_base}_SUCCESS.html"
            # with open(success_debug_filename, 'w', encoding='utf-8') as f:
            #    f.write(response.text)
            # logging.info(f"Saved successful HTML to {success_debug_filename}")


            soup = BeautifulSoup(response.content, 'lxml') # Use lxml parser if installed (often faster)
            items = []
            for selector in config['selectors']['products']:
                items = soup.select(selector)
                if items:
                    logging.info(f"Found {len(items)} potential items with selector: {selector}")
                    items = items[:limit]
                    break

            if not items:
                logging.warning(f"No product items found on {site.capitalize()} for '{query}' using selectors (check if page has results or if selectors need update).")


            for i, item in enumerate(items):
                try:
                    title = self._get_element_text(item, config['selectors']['title'])
                    price = self._clean_price(self._get_element_text(item, config['selectors']['price']))
                    raw_url = self._get_element_attr(item, config['selectors']['link'], 'href')
                    rating = self._extract_rating(self._get_element_text(item, config['selectors']['rating']))
                    reviews = self._extract_reviews(self._get_element_text(item, config['selectors']['reviews']), site)
                    formatted_url = self._format_url(raw_url, site) if raw_url else "N/A"

                    product_data = {
                        'site': site,
                        'title': title.strip() if title else "N/A",
                        'price': price,
                        'url': formatted_url,
                        'rating': rating if rating else "N/A",
                        'reviews': reviews if reviews is not None else "N/A",
                        'timestamp': pd.Timestamp.now().isoformat()
                    }

                    if product_data['title'] != "N/A" and product_data['url'] != "N/A":
                        products.append(product_data)
                        logging.debug(f"Successfully processed {site} item #{i+1}: {product_data['title'][:50]}...")
                    else:
                         logging.warning(f"Skipping {site} item #{i+1} due to missing title or URL.")

                except Exception as e:
                    logging.error(f"Error processing {site.capitalize()} item #{i+1}: {e}", exc_info=False)
                    continue

        except requests.exceptions.Timeout:
             logging.error(f"Request timed out for {site.capitalize()}. URL: {url}")
        except requests.exceptions.HTTPError as e:
             # Logged above if response.ok was False, but catch here just in case
             if not block_reason: # Avoid double logging if it was a detected block
                 logging.error(f"HTTP Error during request for {site.capitalize()}: {e}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed for {site.capitalize()}: {e}. URL: {url}")
        except Exception as e:
            logging.error(f"General error scraping {site.capitalize()}: {e}", exc_info=True)

        logging.info(f"Found {len(products)} valid products on {site.capitalize()} for '{query}'.")
        return products


    # --- _init_keyword_tools, search_products, interactive_search remain unchanged ---
    # --- Paste those methods from the previous response here ---
    def _init_keyword_tools(self):
        """Initialize keyword generation tools"""
        self.modifiers = {
            'commercial': ['buy', 'purchase', 'order', 'shop', 'price', 'deal', 'discount', 'cheap', 'affordable', 'for sale'],
            'informational': ['best', 'top', 'review', 'compare', 'guide', 'how to', 'vs', 'what is', 'features', 'alternatives', 'pros and cons'],
            'navigational': ['[brand name]', 'official site', 'store locator', '[product name] website', 'login', 'customer service'],
            'local': ['near me', 'in [city]', '[city]', 'local', 'nearby', 'store']
        }

    def search_products(self, query: str) -> Dict[str, Any]:
        """Search for products across configured e-commerce sites."""
        results: Dict[str, Any] = {'query': query, 'timestamp': pd.Timestamp.now().isoformat(), 'ecom_results': {}}
        # --- Make sure lxml is installed for parsing ---
        try:
            import lxml
        except ImportError:
             logging.warning("lxml parser not found. Install with 'pip install lxml' for potentially faster parsing. Falling back to 'html.parser'.")
        # ---
        for site, config in self.ecom_sites.items():
            results['ecom_results'][site] = self._scrape_site(query, site)
            # Add a small jitter to the throttle
            wait_time = config['throttle'] + random.uniform(-0.5, 0.5)
            wait_time = max(1.0, wait_time) # Ensure minimum wait time
            logging.info(f"Waiting {wait_time:.2f}s before next site...")
            time.sleep(wait_time)
        return results

    def interactive_search(self):
        """Interactive mode"""
        print("\n=== Robust E-commerce Scraper (Requests/BS4) ===")
        print("Enter product/keyword. 'sites' to list, 'exit' to quit.")
        print(f"(Log Level: {logging.getLevelName(logging.getLogger().level)})")
        if SAVE_BLOCK_HTML:
             print("Block page HTML saving is ENABLED.")

        while True:
            command = input("\nEnter command or query: ").strip()
            if not command: continue
            if command.lower() == 'exit': print("Exiting..."); break
            elif command.lower() == 'sites':
                print("\nSupported e-commerce sites:")
                for site, config in self.ecom_sites.items():
                     note = f" ({config['note']})" if config.get('note') else ""
                     print(f"- {site.capitalize()}{note}")
                continue
            else:
                query = command
                print(f"\n🔍 Searching for: '{query}' (This may take a while)...")
                start_time = time.time()
                results = self.search_products(query)
                elapsed = time.time() - start_time
                print(f"\n--- E-commerce Results ({elapsed:.2f} seconds) ---")
                total_found = 0
                for site, products in results['ecom_results'].items():
                    print(f"\n🛒 {site.capitalize()}:")
                    # Check if products is None or empty (could be block or actual no results)
                    if not products: # products will be [] if blocked or no items found
                        # Check log for specific reason (block detected vs. no items found)
                        print(f"  No products found or request was blocked/failed (check logs for details).")
                    else:
                        count = len(products)
                        total_found += count
                        print(f"  (Found {count} products)")
                        for i, product in enumerate(products[:7], 1):
                            title = product.get('title', 'N/A')
                            price = product.get('price', 'N/A')
                            url = product.get('url', 'N/A')
                            rating = product.get('rating', 'N/A')
                            reviews = product.get('reviews', 'N/A')

                            print(f"\n  {i}. {title}")
                            print(f"     Price: {price}")
                            print(f"     Rating: {rating} | Reviews: {reviews}")
                            print(f"     URL: {url[:120]}{'...' if len(url) > 120 else ''}")
                        if len(products) > 7: print(f"  ... and {len(products) - 7} more results.")
                print(f"\n✨ Total products found across sites where scraping succeeded: {total_found}")

if __name__ == "__main__":
    print("+" + "="*78 + "+")
    print("|" + " " * 78 + "|")
    print("|" + " E-commerce Scraper with CAPTCHA/Block Detection ".center(78) + "|")
    print("|" + " " * 78 + "|")
    print("+" + "="*78 + "+")

    generator = RobustEcomSEMGenerator()
    generator.interactive_search() 