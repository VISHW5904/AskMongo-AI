#SHOULD FOLLOW THIS CODE.

import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, quote
import random
import time
import re
from fake_useragent import UserAgent
from bs4 import SoupStrainer
from typing import List, Dict  # Import List and Dict from typing module



class RobustEcomSEMGenerator:
    def __init__(self):
        # Initialize with robust user agent rotation
        self.ua = UserAgent()

        # Enhanced e-commerce sites configuration with backup selectors
        self.ecom_sites = {
            'amazon': {
                'url': 'https://www.amazon.com/s?k={query}',
                'selectors': {
                    'products': ['div.s-result-item', 'div[data-component-type="s-search-result"]'],
                    'title': ['h2 a span', 'span.a-size-medium', 'span.a-text-normal'],
                    'price': ['span.a-price span', 'span.a-offscreen'],
                    'link': ['h2 a.a-link-normal', 'a.a-link-normal.a-text-normal'],
                    'rating': ['span.a-icon-alt', 'i.a-icon-star-small'],
                    'reviews': ['span.a-size-base', 'span.a-size-base.s-underline-text']
                },
                'throttle': 3,
                'cookies': {'session-id': str(random.randint(1000000, 9999999))}
            },
            'walmart': {
                'url': 'https://www.walmart.com/search?q={query}',
                'selectors': {
                    'products': ['div[data-item-id]', 'div.mb1'],
                    'title': ['span.lh-title', 'div[data-automation-id="product-title"]'],
                    'price': ['div.lh-copy', 'span[itemprop="price"]'],
                    'link': ['a[data-testid="product-title"]', 'a.absolute'],
                    'rating': ['span.w_iUH7', 'span.stars-small'],
                    'reviews': ['span.f7', 'span[data-testid="stars-reviews-count"]']
                },
                'throttle': 2,
                'headers': {'accept': 'text/html,application/xhtml+xml'}
            },
            'bestbuy': {
                'url': 'https://www.bestbuy.com/site/searchpage.jsp?st={query}',
                'selectors': {
                    'products': ['li.sku-item', 'div[data-testid="product"]'],
                    'title': ['h4.sku-title a', 'a[data-testid="product-title"]'],
                    'price': ['div.priceView-customer-price span', 'span[aria-hidden="true"]'],
                    'link': ['a.image-link', 'a[data-testid="product-title"]'],
                    'rating': ['span.c-review-average', 'div[data-testid="ratings"]'],
                    'reviews': ['span.c-total-reviews', 'span[data-testid="reviews"]']
                },
                'throttle': 2,
                'params': {'intl': 'nosplash'}
            },
            'ebay': {
                'url': 'https://www.ebay.com/sch/i.html?_nkw={query}',
                'selectors': {
                    'products': ['li.s-item', 'div.s-item__wrapper'],
                    'title': ['span[role="heading"]', 'div.s-item__title span'],
                    'price': ['span.s-item__price', 'span[itemprop="price"]'],
                    'link': ['a.s-item__link', 'a[class*="s-item__link"]'],
                    'rating': ['span.x-star-rating', 'div[class*="x-star-rating"]'],
                    'reviews': ['span.s-item__reviews-count', 'span[class*="s-item__reviews-count"]']
                },
                'throttle': 1
            }
        }

        # Keyword generation setup
        self._init_keyword_tools()

    def _init_keyword_tools(self):
        """Initialize keyword generation tools"""
        self.modifiers = {
            'commercial': ['buy', 'purchase', 'order', 'shop', 'price', 'deal', 'discount'],
            'informational': ['best', 'top', 'review', 'compare', 'guide', 'how to', 'vs'],
            'navigational': ['brand name', 'official site', 'store locator'],
            'local': ['near me', 'in [city]', '[city]', 'local']
        }

    def _get_request_config(self, site):
        """Generate request configuration with random headers"""
        base_headers = {
            'User-Agent': self.ua.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Referer': 'https://www.google.com/',
            'DNT': '1'
        }

        # Merge with site-specific headers if they exist
        site_headers = self.ecom_sites[site].get('headers', {})
        headers = {**base_headers, **site_headers}

        return {
            'headers': headers,
            'cookies': self.ecom_sites[site].get('cookies', {}),
            'params': self.ecom_sites[site].get('params', {}),
            'timeout': (3.05, 15)
        }

    def _scrape_site(self, query: str, site: str, limit: int = 5) -> List[Dict]:
        """Scrape product data from a specific e-commerce site with improved reliability"""
        config = self.ecom_sites[site]
        url = config['url'].format(query=quote(query))

        try:
            # Make request with configuration
            request_config = self._get_request_config(site)

            # Use a strainer to parse only relevant parts of the page
            strainer = SoupStrainer('div' if site != 'ebay' else 'li')
            response = requests.get(url, **request_config)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser', parse_only=strainer)
            selectors = config['selectors']

            products = []
            items = []

            # Try all possible product selectors
            for product_selector in selectors['products']:
                items = soup.select(product_selector)
                if items:
                    break

            for item in items[:limit]:
                try:
                    product_data = {
                        'site': site,
                        'title': self._get_element_text(item, selectors['title']),
                        'price': self._get_element_text(item, selectors['price']),
                        'url': self._get_element_attr(item, selectors['link'], 'href'),
                        'rating': self._get_element_text(item, selectors['rating']),
                        'reviews': self._get_element_text(item, selectors['reviews']),
                        'timestamp': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                    }

                    # Clean and validate data
                    if not product_data['title']:
                        continue

                    product_data['price'] = self._clean_price(product_data['price'])
                    if product_data['url']:
                        product_data['url'] = urljoin(url, product_data['url'])

                    products.append(product_data)

                except Exception as e:
                    print(f"Error processing product from {site}: {str(e)}")
                    continue

            return products

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error scraping {site}: {str(e)}")
        except requests.exceptions.RequestException as e:
            print(f"Request error scraping {site}: {str(e)}")
        except Exception as e:
            print(f"Unexpected error scraping {site}: {str(e)}")

        return []

    def _get_element_text(self, item, selectors) -> str:
        """Get text from element, handling multiple selector options"""
        if not isinstance(selectors, list):
            selectors = [selectors]

        for selector in selectors:
            try:
                element = item.select_one(selector)
                if element:
                    text = element.get_text(strip=True)
                    if text:
                        return text
            except:
                continue
        return ""

    def _get_element_attr(self, item, selectors, attr) -> str:
        """Get attribute from element, handling multiple selector options"""
        if not isinstance(selectors, list):
            selectors = [selectors]

        for selector in selectors:
            try:
                element = item.select_one(selector)
                if element and element.has_attr(attr):
                    return element[attr]
            except:
                continue
        return ""

    def _clean_price(self, price_text: str) -> str:
        """Extract and format price from text"""
        if not price_text:
            return "N/A"

        try:
            # Remove all non-numeric characters except decimal point
            clean_text = re.sub(r'[^\d.]', '', price_text)
            price = float(clean_text)
            return f"${price:.2f}"
        except:
            return price_text.strip()

    def generate_keywords(self, query: str) -> List[Dict]:
        """Generate keywords with metadata"""
        keywords = []

        # Base keyword
        keywords.append({
            'keyword': query,
            'intent': 'navigational',
            'match_type': 'exact',
            'landing_page': '/'
        })

        # Generate variations
        for intent, modifiers in self.modifiers.items():
            for modifier in modifiers:
                if '[city]' in modifier:
                    continue

                for combo in [f"{modifier} {query}", f"{query} {modifier}"]:
                    landing_page = f"/products/{query.replace(' ', '-').lower()}" if intent == 'commercial' else '/blog/'
                    keywords.append({
                        'keyword': combo,
                        'intent': intent,
                        'match_type': 'phrase',
                        'landing_page': landing_page
                    })

        return keywords

    def search_products(self, query: str) -> Dict:
        """Search for products across all e-commerce sites with retry logic"""
        results = {
            'query': query,
            'timestamp': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
            'keywords': self.generate_keywords(query),
            'ecom_results': {}
        }

        # Scrape each site with throttling and retry
        for site, config in self.ecom_sites.items():
            max_retries = 2
            for attempt in range(max_retries):
                try:
                    products = self._scrape_site(query, site)
                    if products or attempt == max_retries - 1:
                        results['ecom_results'][site] = products
                        break
                    print(f"Retrying {site}... (attempt {attempt + 1})")
                except Exception as e:
                    print(f"Error during attempt {attempt + 1} for {site}: {str(e)}")

                time.sleep(config['throttle'] * (attempt + 1))

            time.sleep(config['throttle'])

        return results

    # ... (rest of your methods remain the same)

    def interactive_search(self):
        """Interactive mode with improved output formatting"""
        print("=== Robust E-commerce SEM Generator ===")
        print("Available commands:")
        print("- search [query] - Generate keywords and ecom results")
        print("- sites - List supported e-commerce sites")
        print("- exit - Quit the program")

        while True:
            command = input("\nEnter command: ").strip().lower()

            if command.startswith('search '):
                query = command[7:]
                if not query:
                    print("Please enter a search query")
                    continue

                print(f"\nGenerating results for: {query}")

                start_time = time.time()
                results = self.search_products(query)
                elapsed = time.time() - start_time

                print(f"\nGenerated {len(results['keywords'])} keywords")
                print("\nSample Keywords:")
                for i, kw in enumerate(results['keywords'][:10], 1):
                    print(f"{i}. {kw['keyword']} ({kw['intent']}) -> {kw['landing_page']}")

                print("\nE-commerce Results:")
                for site, products in results['ecom_results'].items():
                    print(f"\n{site.capitalize()}:")
                    if not products:
                        print("No products found or site unavailable")
                        continue

                    for product in products:
                        print(f"\n- {product['title']}")
                        print(f"  Price: {product['price']}")
                        if product['rating']:
                            print(f"  Rating: {product['rating']}")
                        if product['reviews']:
                            print(f"  Reviews: {product['reviews']}")
                        if product['url']:
                            print(f"  URL: {product['url'][:100]}...")

                print(f"\nCompleted in {elapsed:.2f} seconds")

            elif command == 'sites':
                print("\nSupported e-commerce sites:")
                for site in self.ecom_sites:
                    print(f"- {site} (throttle: {self.ecom_sites[site]['throttle']}s)")

            elif command == 'exit':
                print("Exiting program...")
                break

            else:
                print("Invalid command. Try 'search [query]', 'sites', or 'exit'")


if __name__ == "__main__":
    generator = RobustEcomSEMGenerator()
    generator.interactive_search()