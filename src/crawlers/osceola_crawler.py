import logging
import os
import requests
from dotenv import load_dotenv
from .base_crawler import Crawler

logger = logging.getLogger(__name__)

class OsceolaCrawler(Crawler):
    """Crawler for Osceola County Excel file."""
    
    def __init__(self):
        load_dotenv()
        self.url = os.getenv('OSCEOLA_DATA_URL', 'https://example.com/osceola_property_sales.xlsx')
        self.api_key = os.getenv('OSCEOLA_API_KEY')
        self.output_dir = '/tmp/etl_project/data/osceola/'
        self.output_file = os.path.join(self.output_dir, 'osceola_data.xlsx')

    def crawl(self) -> str:
        """Download Osceola Excel file from URL."""
        try:
            headers = {'Authorization': f'Bearer {self.api_key}'} if self.api_key else {}
            response = requests.get(self.url, headers=headers, stream=True)
            response.raise_for_status()

            os.makedirs(self.output_dir, exist_ok=True)
            with open(self.output_file, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            logger.info(f"Successfully downloaded Osceola data to {self.output_file}")
            return self.output_file
        except Exception as e:
            logger.error(f"Error downloading Osceola data: {str(e)}")
            raise