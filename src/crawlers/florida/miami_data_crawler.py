import logging
import os
import requests
import json
from dotenv import load_dotenv
from .base_crawler import Crawler

logger = logging.getLogger(__name__)

class MiamiDadeCrawler(Crawler):
    """Crawler for Miami-Dade County paginated API."""
    
    def __init__(self):
        load_dotenv()
        self.base_url = os.getenv('MIAMI_DADE_API_URL', 'https://example.com/api/property_sales')
        self.api_key = os.getenv('MIAMI_DADE_API_KEY')
        self.output_dir = '/tmp/etl_project/data/miami_dade/'
        self.output_file = os.path.join(self.output_dir, 'miami_dade_data.json')
        self.page_size = 100  # Adjust based on API

    def crawl(self) -> str:
        """Crawl paginated API and save JSON data."""
        try:
            headers = {'Authorization': f'Bearer {self.api_key}'} if self.api_key else {}
            all_data = []
            page = 1

            while True:
                params = {'page': page, 'page_size': self.page_size}
                response = requests.get(self.base_url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()

                # Assume API returns a list of records or a 'results' key
                records = data.get('results', data)
                if not records:
                    break

                all_data.extend(records)
                logger.info(f"Fetched page {page} with {len(records)} records")

                # Check for pagination end (e.g., 'next' link or empty results)
                if 'next' not in data or not data['next']:
                    break

                page += 1

            if not all_data:
                logger.error("No data retrieved from Miami-Dade API")
                raise ValueError("No data retrieved")

            os.makedirs(self.output_dir, exist_ok=True)
            with open(self.output_file, 'w') as f:
                json.dump(all_data, f)

            logger.info(f"Successfully saved {len(all_data)} records to {self.output_file}")
            return self.output_file
        except Exception as e:
            logger.error(f"Error crawling Miami-Dade data: {str(e)}")
            raise