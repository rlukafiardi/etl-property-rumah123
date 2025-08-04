import requests
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
import re
import pandas as pd
import time
import logging
import random
from typing import List, Dict, Any


class RateLimiter:
    """Manages adaptive rate limiting for web scraping"""
    def __init__(self, base_sleep=1.0, min_sleep=1.0, max_sleep=600.0):
        self.base_sleep_time = base_sleep
        self.min_sleep_time = min_sleep
        self.max_sleep_time = max_sleep
        self.consecutive_429s = 0
        self.consecutive_successes = 0

    def sleep(self):
        """Sleep for a calculated duration before the next request"""
        jitter = random.uniform(0.8, 1.2)
        current_sleep = self.base_sleep_time * jitter
        time.sleep(current_sleep)

    def handle_success(self):
        """Adjust sleep time after a successful request"""
        self.consecutive_successes += 1
        self.consecutive_429s = 0 # Reset failure counter

        if self.consecutive_successes >= 5:
            reduction_factor = 0.5  # Aggressive reduction
        elif self.consecutive_successes >= 3:
            reduction_factor = 0.7  # Moderate reduction
        else:
            reduction_factor = 0.9  # Small reduction

        new_sleep_time = max(self.min_sleep_time, self.base_sleep_time * reduction_factor)
        if new_sleep_time < self.base_sleep_time:
            self.base_sleep_time = new_sleep_time
            logging.info(f"Reduced base sleep time to {self.base_sleep_time:.1f}s after {self.consecutive_successes} consecutive successes.")

    def handle_rate_limit(self):
        """Adjust sleep time after a 429 (Too Many Requests) error"""
        self.consecutive_successes = 0 # Reset success counter
        self.consecutive_429s += 1
        
        # Exponential backoff
        self.base_sleep_time = min(self.max_sleep_time, self.base_sleep_time * 1.5)
        backoff_time = self.base_sleep_time * random.uniform(1.0, 1.5)
        
        logging.warning(f"Rate limit hit. Backing off for {backoff_time:.2f} seconds.")
        time.sleep(backoff_time)

    def handle_other_error(self):
        """Handle other transient errors with a simple backoff"""
        self.consecutive_successes = 0
        time.sleep(self.base_sleep_time * 1.5)


def validate_input_params(ads_type: str, property_type: str, num_pages: int):
    """Validates the input parameters"""
    VALID_ADS_TYPES = {'jual', 'sewa'}
    VALID_PROPERTY_TYPES = {'rumah', 'apartemen', 'kost', 'villa', 'hotel'}
    
    if ads_type not in VALID_ADS_TYPES:
        raise ValueError(f"Invalid ads type: {ads_type}. Must be one of {VALID_ADS_TYPES}")
    if property_type not in VALID_PROPERTY_TYPES:
        raise ValueError(f"Invalid property type: {property_type}. Must be one of {VALID_PROPERTY_TYPES}")
    if not isinstance(num_pages, int) or num_pages <= 0:
        raise ValueError("num_pages must be a positive integer")


def clean_badge_text(badge_tag: BeautifulSoup) -> List[str]:
    """Cleans and splits the badge text into a list of additional features"""
    if not badge_tag:
        return []
    
    text = badge_tag.get_text(strip=True)
    text = re.sub(r'(?<=[a-z])([A-Z])', r', \1', text)
    text = re.sub(r'([A-Z]{2,})([A-Z][a-z])', r'\1, \2', text)
    text = re.sub(r'([^\w\s])([A-Za-z])', r'\1, \2', text)
    text = re.sub(r'\s*,\s*', ', ', text).strip(', ')
    
    features = text.split(', ')
    # Exclude the first item which is typically the property type
    return features[1:] if features else []


def parse_listing_card(listing: BeautifulSoup, admin_list: List[str]) -> Dict[str, Any]:
    """Extracts data from a single property listing card"""
    link_tag = listing.select_one('a:not(.quick-label-badge)')
    name_tag = listing.find('h2')
    price_tag = listing.find('div', class_='card-featured__middle-section__price')
    location_tag = listing.find_all('span')
    attribute_tags = listing.find_all('span', class_='attribute-text')
    size_tags = listing.find_all('div', class_='attribute-info')
    locations = next((tag.get_text(strip=True) for tag in location_tag
                     if any(admin.lower() in tag.get_text(strip=True).lower() for admin in admin_list)), '')
    badge_tags = listing.find('div', class_='card-featured__middle-section__header-badge')
    
    cards = {
        'link': "rumah123.com" + link_tag['href'] if link_tag else None,
        'name': name_tag.get_text(strip=True) if name_tag else None,
        'price_rp': price_tag.find('strong').get_text(strip=True) if price_tag and price_tag.find('strong') else None,
        'location': locations,
        'lot_size': size_tags[0].get_text(strip=True) if len(size_tags) > 0 else None,
        'building_size': size_tags[1].get_text(strip=True) if len(size_tags) > 1 else None,
        'n_bedroom': attribute_tags[0].get_text(strip=True) if len(attribute_tags) > 0 else None,
        'n_bathroom': attribute_tags[1].get_text(strip=True) if len(attribute_tags) > 1 else None,
        'n_carport': attribute_tags[2].get_text(strip=True) if len(attribute_tags) > 2 else None,
        'additional_features': clean_badge_text(badge_tags)
    }
    
    return cards


def extract_data(
    ads_type: str = 'jual',
    region: str = 'dki-jakarta',
    property_type: str = 'rumah',
    num_pages: int = 1,
    admin_list: List[str] = []
) -> pd.DataFrame:
    """
    Extracts property listing data from rumah123.com based on specified filters.

    Parameters:
        ads_type (str): Type of advertisement, either 'jual' (for sale) or 'sewa' (for rent). 
            Valid values: {'jual', 'sewa'}
        region (str): The region to search properties in.
        property_type (str): The type of property.
            Valid values: {'rumah', 'apartemen', 'kost', 'villa', 'hotel'}
        num_pages (int): Number of pages to scrape.
        admin_list (List[str], optional): List of administrative regions to mark administrative names.
    
    Returns:
        pd.DataFrame: A DataFrame containing extracted property data.
    """
    validate_input_params(ads_type, property_type, num_pages)
    
    base_url = f'https://www.rumah123.com/{ads_type}/{region}/{property_type}/?sort=posted-desc&page='
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 Safari/537.36'
    }

    limiter = RateLimiter()
    all_data = []
    
    try:
        with requests.Session() as session:
            session.headers.update(headers)
            
            for page_num in range(1, num_pages + 1):
                page_url = base_url + str(page_num)
                logging.info(f"Fetching data from: {page_url}")
                
                limiter.sleep()
                
                try:
                    response = session.get(page_url, timeout=30)
                    
                    if response.status_code == 200:
                        logging.info(f"Successfully retrieved page {page_num}")
                        limiter.handle_success()
                        
                        soup = BeautifulSoup(response.content, 'lxml')
                        listing_cards = soup.find_all('div', class_='card-featured__middle-section')
                        
                        if not listing_cards:
                            logging.info(f"No listings found on page {page_num}. Ending scrape.")
                            break
                        
                        for card in listing_cards:
                            listing_data = parse_listing_card(card, admin_list)
                            listing_data.update({'ads_type': ads_type, 'property_type': property_type})
                            all_data.append(listing_data)
                            
                    elif response.status_code == 429:
                        limiter.handle_rate_limit()
                        # Decrement to retry the current page after backoff
                        page_num -= 1 
                        continue

                    else:
                        logging.warning(f"Page {page_url} returned status code {response.status_code}")
                        limiter.handle_other_error()
                        
                except RequestException as e:
                    logging.error(f"Request error for {page_url}: {e}")
                    limiter.handle_other_error()
                except Exception as e:
                    logging.error(f"An unexpected error occurred on page {page_num}: {e}")

    except KeyboardInterrupt:
        logging.info("Extraction interrupted by user. Returning collected data.")
    
    df = pd.DataFrame(all_data)
    logging.info(f"Extracted {len(df)} records.")
    return df