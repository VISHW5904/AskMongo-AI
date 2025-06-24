# app.py
from flask import Flask, render_template_string, request, jsonify
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup, SoupStrainer
from urllib.parse import urljoin, quote
import random
import time
import re
from fake_useragent import UserAgent
from typing import List, Dict, Optional, Any
import logging
from datetime import datetime

app = Flask(__name__)

# --- Configuration ---
LOGGING_LEVEL = logging.INFO
logging.basicConfig(level=LOGGING_LEVEL, format='%(asctime)s - %(levelname)s - [%(funcName)s] %(message)s')
SAVE_BLOCK_HTML = False

# --- Constants ---
DEFAULT_TIMEOUT = (10, 25)
MAX_RETRIES = 2
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

BLOCK_INDICATORS = {
    'keywords': [
        'captcha', 'verify you are human', 'robot check', 'are you a robot',
        'access denied', 'security measure', 'unusual activity',
        'enter the characters', 'blocked', 'human verification',
        'prove you are human', 'automated queries', 'enable javascript',
        'enable cookies'
    ],
    'url_patterns': {
        'amazon': ['/captcha/', '/errors/validatecaptcha'],
        'walmart': ['/blocked'],
        'ebay': ['/signin/s'],
        'alibaba': ['/captcha', 'login.alibaba.com']
    },
    'titles': [
        'robot check', 'verify your identity', 'security measure',
        'access denied', 'human verification', 'are you a robot?',
        'attention required!', 'cloudflare'
    ]
}

class RobustEcomSEMGenerator:
    def __init__(self):
        self.ua = UserAgent()
        self.session = self._create_session()

scraper = RobustEcomSEMGenerator()

# HTML Template with embedded CSS and JavaScript
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>E-commerce Scraper</title>
    <style>
        :root {
            --primary-color: #4a6fa5;
            --secondary-color: #166088;
            --background-color: #f8f9fa;
            --card-color: #ffffff;
            --text-color: #333333;
            --error-color: #d32f2f;
            --success-color: #2e7d32;
        }

        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }

        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background-color: var(--background-color);
            color: var(--text-color);
            line-height: 1.6;
        }
        /* ...rest of the CSS remains the same */

    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>E-commerce Scraper</h1>
            <p class="subtitle">Search and compare products across multiple online stores</p>

            <form class="search-form" onsubmit="searchProducts(event)">
                <input type="text" id="searchQuery" placeholder="Enter product name..." required>
                <button type="submit" id="searchButton">Search</button>
            </form>
            <!-- ... rest of the HTML remains the same -->

            <script>
            // ... rest of javascript remains the same
            </script>

        </div>
    </div>
</body>
</html>
"""

@app.route('/')
def home():
    return render_template_string(HTML_TEMPLATE)

@app.route('/search', methods=['POST'])
def search():
    data = request.get_json()
    query = data.get('query')
    if not query:
        return jsonify({"error": "Query parameter is required"}), 400

    try:
        results = scraper.search_products(query)
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup, SoupStrainer
from urllib.parse import urljoin, quote
import random
import time
import re
from fake_useragent import UserAgent
from typing import List, Dict, Optional, Any
import logging
from datetime import datetime

app = Flask(__name__)

# --- Configuration ---
LOGGING_LEVEL = logging.INFO
logging.basicConfig(level=LOGGING_LEVEL, format='%(asctime)s - %(levelname)s - [%(funcName)s] %(message)s')
SAVE_BLOCK_HTML = False

# --- Constants ---
DEFAULT_TIMEOUT = (10, 25)
MAX_RETRIES = 2
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

BLOCK_INDICATORS = {
    'keywords': [
        'captcha', 'verify you are human', 'robot check', 'are you a robot',
        'access denied', 'security measure', 'unusual activity',
        'enter the characters', 'blocked', 'human verification',
        'prove you are human', 'automated queries', 'enable javascript',
        'enable cookies'
    ],
    'url_patterns': {
        'amazon': ['/captcha/', '/errors/validatecaptcha'],
        'walmart': ['/blocked'],
        'ebay': ['/signin/s'],
        'alibaba': ['/captcha', 'login.alibaba.com']
    },
    'titles': [
        'robot check', 'verify your identity', 'security measure',
        'access denied', 'human verification', 'are you a robot?',
        'attention required!', 'cloudflare'
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
                'selectors': {
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
                'selectors': {
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
                'selectors': {
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
                'selectors': {
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
        headers = BASE_HEADERS.copy()
        headers['User-Agent'] = self.ua.random
        return headers

    def _create_session(self) -> requests.Session:
        session = requests.Session()
        retries = Retry(
            total=MAX_RETRIES,
            backoff_factor=BACKOFF_FACTOR,
            status_forcelist=[429, 503, 500, 502, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            respect_retry_after_header=True
        )
        adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=retries)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        session.headers.update({k: v for k, v in BASE_HEADERS.items() if k != 'User-Agent'})
        return session

    def _get_element_text(self, item: Any, selectors: List[str]) -> Optional[str]:
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
        if not price_str:
            return "N/A"
        price_str = price_str.replace('\n', '').replace('\r', '')
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
        if not rating_str:
            return "N/A"
        match = re.search(r'(\d\.?\d*)\s*(?:out\s*of\s*5)?', rating_str)
        if match:
            return match.group(1)
        return "N/A"

    def _extract_reviews(self, reviews_str: Optional[str], site: str) -> Optional[int]:
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

    def _format_url(self, url: str, site: str) -> str:
        if not url or url == "N/A":
            return "N/A"

        base_url = self.ecom_sites[site]['base_url']
        full_url = urljoin(base_url, url)

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

        elif site == 'walmart':
            ip_match_variant = re.search(r'/ip/[^/]+/(\d+)', full_url)
            if ip_match_variant:
                product_id = ip_match_variant.group(1)
                return f"https://www.walmart.com/ip/{product_id}"
            ip_match_standard = re.search(r'/ip/(\d+)', full_url)
            if ip_match_standard:
                product_id = ip_match_standard.group(1)
                return f"https://www.walmart.com/ip/{product_id}"
            if '/search?' in full_url and 'q=' in full_url:
                return full_url
            logging.debug(f"Could not format Walmart URL: {full_url}")
            return full_url

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

        elif site == 'ebay':
            itm_match = re.search(r'/itm/(\d+)', full_url)
            if itm_match:
                item_id = itm_match.group(1)
                return f"https://www.ebay.com/itm/{item_id}"
            if '/sch/i.html?' in full_url and '_nkw=' in full_url:
                return full_url
            logging.debug(f"Could not format eBay URL: {full_url}")
            return full_url

        return full_url

    def _is_response_blocked(self, response: requests.Response, site: str) -> Optional[str]:
        final_url_lower = response.url.lower()
        response_text_lower = response.text.lower()

        site_url_patterns = BLOCK_INDICATORS['url_patterns'].get(site, [])
        for pattern in site_url_patterns:
            if pattern in final_url_lower:
                return f"URL pattern '{pattern}' detected"

        try:
            strainer = SoupStrainer('title')
            soup_title_check = BeautifulSoup(response.content, 'lxml', parse_only=strainer)
            if soup_title_check and soup_title_check.title and soup_title_check.title.string:
                page_title_lower = soup_title_check.title.string.lower().strip()
                for block_title in BLOCK_INDICATORS['titles']:
                    if block_title in page_title_lower:
                        return f"Title indicator '{block_title}' detected ('{soup_title_check.title.string.strip()}')"
        except Exception as e:
            logging.debug(f"Could not parse title for block check on {site}: {e}")

        check_text_sample = response_text_lower[:10000]
        for keyword in BLOCK_INDICATORS['keywords']:
            if keyword in check_text_sample:
                return f"Content keyword '{keyword}' detected"

        return None

    def _scrape_site(self, query: str, site: str, limit: int = 7) -> List[Dict[str, Any]]:
        config = self.ecom_sites[site]
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

            if not response.ok:
                if response.status_code == 403:
                    logging.error(f"{site.capitalize()}: Access Forbidden (403). Likely a hard block. URL: {response.url}")
                else:
                    logging.error(f"{site.capitalize()}: HTTP Error {response.status_code} {response.reason}. URL: {response.url}")
                response.raise_for_status()

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
                return []

            logging.debug(f"Response OK and not blocked for {site}. Proceeding to parse.")

            soup = BeautifulSoup(response.content, 'lxml')
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
            if not block_reason:
                logging.error(f"HTTP Error during request for {site.capitalize()}: {e}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed for {site.capitalize()}: {e}. URL: {url}")
        except Exception as e:
            logging.error(f"General error scraping {site.capitalize()}: {e}", exc_info=True)

        logging.info(f"Found {len(products)} valid products on {site.capitalize()} for '{query}'.")
        return products

    def _init_keyword_tools(self):
        self.modifiers = {
            'commercial': ['buy', 'purchase', 'order', 'shop', 'price', 'deal', 'discount', 'cheap', 'affordable', 'for sale'],
            'informational': ['best', 'top', 'review', 'compare', 'guide', 'how to', 'vs', 'what is', 'features', 'alternatives', 'pros and cons'],
            'navigational': ['[brand name]', 'official site', 'store locator', '[product name] website', 'login', 'customer service'],
            'local': ['near me', 'in [city]', '[city]', 'local', 'nearby', 'store']
        }

    def search_products(self, query: str) -> Dict[str, Any]:
        results: Dict[str, Any] = {'query': query, 'timestamp': pd.Timestamp.now().isoformat(), 'ecom_results': {}}
        try:
            import lxml
        except ImportError:
            logging.warning("lxml parser not found. Install with 'pip install lxml' for potentially faster parsing. Falling back to 'html.parser'.")

        for site, config in self.ecom_sites.items():
            results['ecom_results'][site] = self._scrape_site(query, site)
            wait_time = config['throttle'] + random.uniform(-0.5, 0.5)
            wait_time = max(1.0, wait_time)
            logging.info(f"Waiting {wait_time:.2f}s before next site...")
            time.sleep(wait_time)
        return results

scraper = RobustEcomSEMGenerator()

# HTML Template with embedded CSS and JavaScript
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>E-commerce Scraper</title>
    <style>
        :root {
            --primary-color: #4a6fa5;
            --secondary-color: #166088;
            --background-color: #f8f9fa;
            --card-color: #ffffff;
            --text-color: #333333;
            --error-color: #d32f2f;
            --success-color: #2e7d32;
        }

        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }

        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background-color: var(--background-color);
            color: var(--text-color);
            line-height: 1.6;
        }

        .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
        }

        .header {
            text-align: center;
            margin-bottom: 30px;
            padding: 30px;
            background-color: var(--card-color);
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
            background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
        }

        .header h1 {
            color: var(--primary-color);
            margin-bottom: 10px;
            font-size: 2.5rem;
        }

        .subtitle {
            color: #555;
            margin-bottom: 20px;
            font-size: 1.1rem;
        }

        .search-form {
            display: flex;
            justify-content: center;
            margin: 30px 0;
        }

        .search-form input {
            padding: 12px 20px;
            font-size: 16px;
            border: 1px solid #ddd;
            border-radius: 4px 0 0 4px;
            width: 60%;
            max-width: 500px;
            outline: none;
            transition: border 0.3s;
        }

        .search-form input:focus {
            border-color: var(--primary-color);
        }

        .search-form button {
            padding: 12px 25px;
            background-color: var(--primary-color);
            color: white;
            border: none;
            border-radius: 0 4px 4px 0;
            cursor: pointer;
            font-size: 16px;
            transition: all 0.3s;
        }

        .search-form button:hover {
            background-color: var(--secondary-color);
            transform: translateY(-2px);
            box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
        }

        .search-form button:disabled {
            background-color: #cccccc;
            cursor: not-allowed;
            transform: none;
            box-shadow: none;
        }

        .site-tags {
            display: flex;
            flex-wrap: wrap;
            justify-content: center;
            gap: 10px;
            margin-top: 20px;
        }

        .site-tag {
            background-color: #e0e0e0;
            padding: 5px 15px;
            border-radius: 20px;
            font-size: 14px;
            color: #555;
        }

        .site-tag.highlight {
            background-color: var(--primary-color);
            color: white;
        }

        .loading {
            text-align: center;
            margin: 30px 0;
        }

        .spinner {
            border: 4px solid rgba(0, 0, 0, 0.1);
            border-radius: 50%;
            border-top: 4px solid var(--primary-color);
            width: 40px;
            height: 40px;
            animation: spin 1s linear infinite;
            margin: 0 auto;
        }

        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }

        .error-message {
            background-color: #ffebee;
            color: var(--error-color);
            padding: 15px;
            border-radius: 4px;
            margin-bottom: 20px;
            text-align: center;
            animation: fadeIn 0.3s;
        }

        .success-message {
            background-color: #e8f5e9;
            color: var(--success-color);
            padding: 15px;
            border-radius: 4px;
            margin-bottom: 20px;
            text-align: center;
            animation: fadeIn 0.3s;
        }

        @keyframes fadeIn {
            from { opacity: 0; }
            to { opacity: 1; }
        }

        .results-container {
            margin-top: 20px;
            animation: fadeIn 0.5s;
        }

        .results-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
        }

        .results-title {
            color: var(--primary-color);
            font-size: 1.5rem;
        }

        .timestamp {
            color: #666;
            font-size: 14px;
        }

        .site-results {
            background-color: var(--card-color);
            border-radius: 8px;
            padding: 25px;
            margin-bottom: 30px;
            box-shadow: 0 3px 10px rgba(0, 0, 0, 0.1);
            transition: transform 0.3s;
        }

        .site-results:hover {
            transform: translateY(-5px);
        }

        .site-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
        }

        .site-name {
            color: var(--secondary-color);
            font-size: 1.3rem;
            display: flex;
            align-items: center;
        }

        .site-logo {
            width: 30px;
            height: 30px;
            margin-right: 10px;
            border-radius: 50%;
            object-fit: cover;
        }

        .product-count {
            font-size: 14px;
            color: #666;
            margin-left: 10px;
        }

        .note {
            font-size: 14px;
            color: #d32f2f;
            font-style: italic;
        }

        .no-products {
            color: #666;
            font-style: italic;
            text-align: center;
            padding: 20px;
        }

        .products-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
            gap: 25px;
            margin-top: 20px;
        }

        .product-card {
            background-color: white;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 5px rgba(0, 0, 0, 0.1);
            transition: all 0.3s;
            border: 1px solid #eee;
        }

        .product-card:hover {
            transform: translateY(-5px);
            box-shadow: 0 5px 15px rgba(0, 0, 0, 0.1);
            border-color: var(--primary-color);
        }

        .product-title {
            font-size: 16px;
            margin-bottom: 15px;
            color: var(--text-color);
            line-height: 1.4;
        }

        .product-price {
            font-weight: bold;
            color: var(--primary-color);
            margin-bottom: 15px;
            font-size: 1.2rem;
        }

        .product-meta {
            display: flex;
            justify-content: space-between;
            margin-bottom: 15px;
            font-size: 14px;
            color: #666;
        }

        .rating {
            display: flex;
            align-items: center;
        }

        .rating svg {
            color: #ffc107;
            margin-right: 5px;
        }

        .product-link {
            display: inline-block;
            width: 100%;
            text-align: center;
            background-color: var(--primary-color);
            color: white;
            padding: 10px;
            border-radius: 4px;
            text-decoration: none;
            font-size: 14px;
            transition: background-color 0.3s;
            margin-top: 10px;
        }

        .product-link:hover {
            background-color: var(--secondary-color);
        }

        .view-more {
            text-align: center;
            margin-top: 15px;
        }

        .view-more button {
            background: none;
            border: none;
            color: var(--primary-color);
            cursor: pointer;
            font-size: 14px;
            text-decoration: underline;
        }

        footer {
            text-align: center;
            margin-top: 50px;
            padding: 20px;
            color: #666;
            font-size: 14px;
        }

        @media (max-width: 768px) {
            .header h1 {
                font-size: 2rem;
            }

            .search-form input {
                width: 70%;
                padding: 10px 15px;
            }

            .search-form button {
                padding: 10px 20px;
            }

            .products-grid {
                grid-template-columns: 1fr;
            }

            .site-header {
                flex-direction: column;
                align-items: flex-start;
            }

            .site-name {
                margin-bottom: 10px;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>E-commerce Scraper</h1>
            <p class="subtitle">Search and compare products across multiple online stores</p>

            <form class="search-form" onsubmit="searchProducts(event)">
                <input type="text" id="searchQuery" placeholder="Enter product name..." required>
                <button type="submit" id="searchButton">Search</button>
            </form>

            <div class="site-tags" id="siteTags">
                <!-- Site tags will be added here by JavaScript -->
            </div>
        </div>

        <div id="loadingIndicator" class="loading" style="display: none;">
            <div class="spinner"></div>
            <p>Searching across e-commerce sites...</p>
        </div>

        <div id="errorMessage" class="error-message" style="display: none;"></div>
        <div id="successMessage" class="success-message" style="display: none;"></div>

        <div id="resultsContainer" class="results-container" style="display: none;">
            <div class="results-header">
                <h2 class="results-title" id="resultsTitle"></h2>
                <p class="timestamp" id="resultsTimestamp"></p>
            </div>

            <div id="resultsContent">
                <!-- Results will be added here by JavaScript -->
            </div>
        </div>

        <footer>
            <p>E-commerce Scraper &copy; 2023 | Data is collected in real-time from various e-commerce websites</p>
        </footer>
    </div>

    <script>
        const siteLogos = {
            'amazon': 'https://logo.clearbit.com/amazon.com',
            'walmart': 'https://logo.clearbit.com/walmart.com',
            'ebay': 'https://logo.clearbit.com/ebay.com',
            'alibaba': 'https://logo.clearbit.com/alibaba.com'
        };

        // Initialize site tags
        function initSiteTags() {
            const siteTags = document.getElementById('siteTags');
            const sites = ['amazon', 'walmart', 'ebay', 'alibaba'];

            sites.forEach(site => {
                const tag = document.createElement('div');
                tag.className = 'site-tag';
                tag.textContent = site.charAt(0).toUpperCase() + site.slice(1);
                tag.dataset.site = site;
                tag.onclick = () => toggleSiteSelection(site);
                siteTags.appendChild(tag);
            });
        }

        // Toggle site selection
        function toggleSiteSelection(site) {
            const tag = document.querySelector(`.site-tag[data-site="${site}"]`);
            tag.classList.toggle('highlight');
        }

        // Get selected sites
        function getSelectedSites() {
            const selected = [];
            document.querySelectorAll('.site-tag.highlight').forEach(tag => {
                selected.push(tag.dataset.site);
            });
            return selected.length > 0 ? selected : ['amazon', 'walmart', 'ebay', 'alibaba'];
        }

        // Search products
        async function searchProducts(event) {
            event.preventDefault();

            const query = document.getElementById('searchQuery').value.trim();
            if (!query) return;

            // Show loading indicator
            document.getElementById('loadingIndicator').style.display = 'block';
            document.getElementById('errorMessage').style.display = 'none';
            document.getElementById('successMessage').style.display = 'none';
            document.getElementById('resultsContainer').style.display = 'none';

            try {
                const response = await fetch('/search', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({ query })
                });

                if (!response.ok) {
                    throw new Error(await response.text());
                }

                const data = await response.json();
                displayResults(data);

                // Show success message
                const successMessage = document.getElementById('successMessage');
                successMessage.textContent = `Found ${Object.values(data.ecom_results).flat().length} products across sites`;
                successMessage.style.display = 'block';

            } catch (error) {
                // Show error message
                const errorMessage = document.getElementById('errorMessage');
                errorMessage.textContent = error.message || 'An error occurred during search';
                errorMessage.style.display = 'block';

            } finally {
                document.getElementById('loadingIndicator').style.display = 'none';
            }
        }

        // Display results
        function displayResults(data) {
            const resultsContainer = document.getElementById('resultsContainer');
            const resultsTitle = document.getElementById('resultsTitle');
            const resultsTimestamp = document.getElementById('resultsTimestamp');
            const resultsContent = document.getElementById('resultsContent');

            // Set header info
            resultsTitle.textContent = `Results for "${data.query}"`;
            resultsTimestamp.textContent = `Search performed at: ${new Date(data.timestamp).toLocaleString()}`;

            // Clear previous results
            resultsContent.innerHTML = '';

            // Add results for each site
            for (const [site, products] of Object.entries(data.ecom_results)) {
                const siteSection = document.createElement('div');
                siteSection.className = 'site-results';

                // Site header
                const siteHeader = document.createElement('div');
                siteHeader.className = 'site-header';

                const siteName = document.createElement('h3');
                siteName.className = 'site-name';

                const siteLogo = document.createElement('img');
                siteLogo.className = 'site-logo';
                siteLogo.src = siteLogos[site] || 'https://via.placeholder.com/30';
                siteLogo.alt = `${site} logo`;

                siteName.appendChild(siteLogo);
                siteName.appendChild(document.createTextNode(site.charAt(0).toUpperCase() + site.slice(1)));

                const productCount = document.createElement('span');
                productCount.className = 'product-count';
                productCount.textContent = `(${products.length} products)`;
                siteName.appendChild(productCount);

                siteHeader.appendChild(siteName);
                siteSection.appendChild(siteHeader);

                // Products grid
                if (products.length > 0) {
                    const productsGrid = document.createElement('div');
                    productsGrid.className = 'products-grid';

                    products.slice(0, 6).forEach(product => {
                        const productCard = document.createElement('div');
                        productCard.className = 'product-card';

                        const title = document.createElement('h4');
                        title.className = 'product-title';
                        title.textContent = product.title;

                        const price = document.createElement('p');
                        price.className = 'product-price';
                        price.textContent = product.price;

                        const meta = document.createElement('div');
                        meta.className = 'product-meta';

                        const rating = document.createElement('div');
                        rating.className = 'rating';
                        rating.innerHTML = `
                            <svg width="16" height="16" viewBox="0 0 24 24">
                                <path fill="currentColor" d="M12 17.27L18.18 21l-1.64-7.03L22 9.24l-7.19-.61L12 2 9.19 8.63 2 9.24l5.46 4.73L5.82 21z"/>
                            </svg>
                            ${product.rating}
                        `;

                        const reviews = document.createElement('div');
                        reviews.className = 'reviews';
                        reviews.textContent = `${product.reviews} reviews`;

                        meta.appendChild(rating);
                        meta.appendChild(reviews);

                        const link = document.createElement('a');
                        link.className = 'product-link';
                        link.href = product.url;
                        link.target = '_blank';
                        link.rel = 'noopener noreferrer';
                        link.textContent = `View on ${site.charAt(0).toUpperCase() + site.slice(1)}`;

                        productCard.appendChild(title);
                        productCard.appendChild(price);
                        productCard.appendChild(meta);
                        productCard.appendChild(link);

                        productsGrid.appendChild(productCard);
                    });

                    siteSection.appendChild(productsGrid);

                    // View more button if there are more products
                    if (products.length > 6) {
                        const viewMore = document.createElement('div');
                        viewMore.className = 'view-more';

                        const button = document.createElement('button');
                        button.textContent = `View all ${products.length} products`;
                        button.onclick = () => {
                            // In a real app, this would show all products
                            alert(`Showing all ${products.length} products from ${site}`);
                        };

                        viewMore.appendChild(button);
                        siteSection.appendChild(viewMore);
                    }
                } else {
                    const noProducts = document.createElement('p');
                    noProducts.className = 'no-products';
                    noProducts.textContent = 'No products found or request was blocked';
                    siteSection.appendChild(noProducts);
                }

                resultsContent.appendChild(siteSection);
            }

            // Show results container
            resultsContainer.style.display = 'block';
        }

        // Initialize the page
        window.onload = function() {
            initSiteTags();
        };
    </script>
</body>
</html>
"""

@app.route('/')
def home():
    return render_template_string(HTML_TEMPLATE)

@app.route('/search', methods=['POST'])
def search():
    data = request.get_json()
    query = data.get('query')
    if not query:
        return jsonify({"error": "Query parameter is required"}), 400

    try:
        results = scraper.search_products(query)
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)