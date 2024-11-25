import requests
from bs4 import BeautifulSoup
import os
import logging
from urllib.parse import urljoin
import time
from typing import List, Optional

class PatentsViewDownloader:
    """
    A class to download TSV zip files from PatentsView data download tables.
    """
    
    def __init__(self, base_url: str, download_dir: str = "patent_data"):
        """
        Initialize the downloader with base URL and download directory.
        
        Args:
            base_url: The URL of the PatentsView download page
            download_dir: Local directory to save downloaded files
        """
        self.base_url = base_url
        self.download_dir = download_dir
        self.downloaded_files = []
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Create download directory if it doesn't exist
        os.makedirs(download_dir, exist_ok=True)
    
    def get_download_links(self) -> List[str]:
        """
        Scrape the webpage for TSV zip file download links.
        
        Returns:
            List of URLs for TSV zip files
        """
        try:
            response = requests.get(self.base_url)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all links that end with tsv.zip
            zip_links = [
                urljoin(self.base_url, link['href'])
                for link in soup.find_all('a', href=True)
                if link['href'].endswith('tsv.zip')
            ]
            
            self.logger.info(f"Found {len(zip_links)} TSV zip files to download")
            return zip_links
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error fetching download links: {str(e)}")
            return []
    
    def download_file(self, url: str) -> Optional[str]:
        """
        Download a single file from the given URL.
        
        Args:
            url: URL of the file to download
            
        Returns:
            Path to the downloaded file or None if download failed
        """
        try:
            filename = url.split('/')[-1]
            filepath = os.path.join(self.download_dir, filename)
            
            # Skip if file already exists
            if os.path.exists(filepath):
                self.logger.info(f"File {filename} already exists, skipping")
                return filepath
            
            self.logger.info(f"Downloading {filename}")
            
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            # Get total file size
            total_size = int(response.headers.get('content-length', 0))
            
            with open(filepath, 'wb') as f:
                if total_size == 0:
                    f.write(response.content)
                else:
                    downloaded = 0
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            # Log progress for large files
                            if downloaded % (1024 * 1024) == 0:  # Log every MB
                                percent = (downloaded / total_size) * 100
                                self.logger.info(f"Downloaded {percent:.1f}% of {filename}")
            
            self.downloaded_files.append(filepath)
            self.logger.info(f"Successfully downloaded {filename}")
            return filepath
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error downloading {url}: {str(e)}")
            return None
    
    def download_all(self, delay: int = 2) -> List[str]:
        """
        Download all TSV zip files from the webpage.
        
        Args:
            delay: Delay between downloads in seconds to avoid overwhelming the server
            
        Returns:
            List of paths to successfully downloaded files
        """
        download_links = self.get_download_links()
        
        for link in download_links:
            self.download_file(link)
            time.sleep(delay)  # Be nice to the server
        
        self.logger.info(f"Downloaded {len(self.downloaded_files)} files successfully")
        return self.downloaded_files

    def get_downloaded_files(self) -> List[str]:
        """
        Get list of successfully downloaded files.
        
        Returns:
            List of paths to downloaded files
        """
        return self.downloaded_files
