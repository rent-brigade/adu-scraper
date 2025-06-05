import os
import requests
import logging
from datetime import datetime
from pathlib import Path
import json
import pandas as pd
from pdf_processor import PDFProcessor

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LACityPlanningScraper:
    def __init__(self, base_url="https://planning.lacity.gov", download_dir="pdfs", csv_dir="csvs"):
        self.base_url = base_url
        self.download_dir = Path(download_dir)
        self.csv_dir = Path(csv_dir)
        self.download_dir.mkdir(exist_ok=True)
        self.csv_dir.mkdir(exist_ok=True)
        self.api_base_url = f"{base_url}/dcpapi/general/biweeklycase"
        self.pdf_processor = PDFProcessor()
        
    def get_pdf_links(self, year):
        """Query the API for available PDF documents for a specific year."""
        try:
            url = f"{self.api_base_url}/CD/{year}"
            logger.info(f"Querying API: {url}")
            
            response = requests.get(url)
            response.raise_for_status()
            
            data = response.json()
            pdf_links = []
            
            if 'Entries' in data:
                for entry in data['Entries']:
                    if 'url' in entry:
                        pdf_links.append({
                            'url': entry['url'],
                            'date': entry['Date']
                        })
                        logger.info(f"Found PDF link: {entry['url']} from {entry['Date']}")
            
            logger.info(f"Found {len(pdf_links)} PDF links for year {year}")
            return pdf_links
            
        except Exception as e:
            logger.error(f"Error in get_pdf_links for year {year}: {e}")
            return []
    
    def download_pdf(self, url, date):
        """Download a PDF file from the given URL."""
        try:
            logger.info(f"Attempting to download PDF from: {url}")
            response = requests.get(url, stream=True)
            
            # Check if the response is actually a PDF
            content_type = response.headers.get('content-type', '')
            if 'application/pdf' not in content_type.lower():
                logger.info(f"Skipping {url} - not a PDF (content-type: {content_type})")
                return None
                
            response.raise_for_status()
            
            # Use the date in the filename
            filename = f"biweekly_case_report_{date.replace('/', '_')}.pdf"
            filepath = self.download_dir / filename
            
            # Save the PDF
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            logger.info(f"Successfully downloaded: {filename}")
            return filepath
            
        except requests.RequestException as e:
            logger.info(f"Error downloading {url}: {e}")
            return None
    
    def process_pdf_to_csv(self, pdf_path, date):
        """Convert a PDF to CSV format."""
        try:
            csv_filename = f"biweekly_case_report_{date.replace('/', '_')}.csv"
            csv_path = self.csv_dir / csv_filename
            
            # Process the PDF and save as CSV
            df = self.pdf_processor.process_pdf(pdf_path)
            if df is not None:
                df.to_csv(csv_path, index=False)
                logger.info(f"Successfully converted {pdf_path} to {csv_path}")
                return csv_path
            return None
            
        except Exception as e:
            logger.error(f"Error processing PDF {pdf_path}: {e}")
            return None
    
    def download_and_process_all_pdfs(self, start_year=2020):
        """Download and process all PDFs from 2020 to current year."""
        current_year = datetime.now().year
        all_csvs = []
        
        for year in range(start_year, current_year + 1):
            logger.info(f"Processing year {year}")
            pdf_links = self.get_pdf_links(str(year))
            
            for pdf_info in pdf_links:
                pdf_path = self.download_pdf(pdf_info['url'], pdf_info['date'])
                if pdf_path:
                    csv_path = self.process_pdf_to_csv(pdf_path, pdf_info['date'])
                    if csv_path:
                        all_csvs.append(csv_path)
        
        # Combine all CSVs
        if all_csvs:
            combined_df = pd.concat([pd.read_csv(csv) for csv in all_csvs])
            combined_csv_path = self.csv_dir / "combined_biweekly_reports.csv"
            combined_df.to_csv(combined_csv_path, index=False)
            logger.info(f"Successfully combined all CSVs into {combined_csv_path}")
            return combined_csv_path
        else:
            logger.warning("No CSVs were created")
            return None

def main():
    scraper = LACityPlanningScraper()
    combined_csv = scraper.download_and_process_all_pdfs()
    
    if combined_csv:
        logger.info(f"Successfully created combined CSV: {combined_csv}")
    else:
        logger.warning("Failed to create combined CSV")

if __name__ == "__main__":
    main() 