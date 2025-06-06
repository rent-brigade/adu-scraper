import os
import requests
import logging
from datetime import datetime
from pathlib import Path
import json
import pandas as pd
from pdf_processor import PDFProcessor
from typing import Optional

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
        
    def get_pdf_links(self):
        """Query the API for all available PDF documents."""
        try:
            url = f"{self.api_base_url}/CNC/"
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
            
            logger.info(f"Found {len(pdf_links)} PDF links")
            return pdf_links
            
        except Exception as e:
            logger.error(f"Error in get_pdf_links: {e}")
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
    
    def process_pdf_to_csv(self, pdf_path, date, pdf_url):
        """Convert a PDF to CSV format."""
        try:
            csv_filename = f"biweekly_case_report_{date.replace('/', '_')}.csv"
            csv_path = self.csv_dir / csv_filename
            
            # Process the PDF and save as CSV
            df = self.pdf_processor.process_pdf(pdf_path, pdf_url)
            if df is not None:
                df.to_csv(csv_path, index=False)
                logger.info(f"Successfully converted {pdf_path} to {csv_path}")
                return csv_path
            return None
            
        except Exception as e:
            logger.error(f"Error processing PDF {pdf_path}: {e}")
            return None
    
    def download_and_process_all_pdfs(self, 
                                    start_year: Optional[int] = None, 
                                    end_year: Optional[int] = None,
                                    start_month: Optional[int] = None,
                                    end_month: Optional[int] = None):
        """
        Download and process available PDFs, optionally filtered by year and month ranges.
        All ranges are inclusive (e.g., start_year=2023, end_year=2024 includes both 2023 and 2024).
        
        Args:
            start_year: Optional start year to filter by (inclusive, e.g., 2023)
            end_year: Optional end year to filter by (inclusive, e.g., 2024)
            start_month: Optional start month to filter by (inclusive, 1-12)
            end_month: Optional end month to filter by (inclusive, 1-12)
        """
        all_csvs = []
        
        pdf_links = self.get_pdf_links()
        
        # Filter links by year and month ranges if specified
        if any([start_year, end_year, start_month, end_month]):
            filtered_links = []
            for pdf_info in pdf_links:
                try:
                    date = datetime.strptime(pdf_info['date'], '%m/%d/%Y')
                    
                    # Check year range (inclusive)
                    if start_year and date.year < start_year:
                        continue
                    if end_year and date.year > end_year:
                        continue
                        
                    # Check month range (inclusive)
                    if start_month and end_month:
                        if not (start_month <= date.month <= end_month):  # Inclusive range check
                            continue
                    elif start_month and date.month < start_month:
                        continue
                    elif end_month and date.month > end_month:
                        continue
                        
                    filtered_links.append(pdf_info)
                except ValueError as e:
                    logger.warning(f"Could not parse date {pdf_info['date']}: {e}")
            pdf_links = filtered_links
            logger.info(f"Filtered to {len(pdf_links)} PDFs matching year range {start_year}-{end_year} (inclusive), month range {start_month}-{end_month} (inclusive)")
        
        for pdf_info in pdf_links:
            pdf_path = self.download_pdf(pdf_info['url'], pdf_info['date'])
            if pdf_path:
                csv_path = self.process_pdf_to_csv(pdf_path, pdf_info['date'], pdf_info['url'])
                if csv_path:
                    all_csvs.append(csv_path)
        
        # Combine all CSVs
        if all_csvs:
            # Read CSVs with string dtype for Council District
            dfs = []
            for csv in all_csvs:
                df = pd.read_csv(csv, dtype={'Council District': str})
                dfs.append(df)
            
            combined_df = pd.concat(dfs)
            
            # Add year and month ranges to filename if filtering
            filename = "combined_biweekly_reports"
            if start_year or end_year:
                filename += f"_{start_year or ''}-{end_year or ''}"
            if start_month or end_month:
                filename += f"_{start_month or ''}-{end_month or ''}"
            filename += ".csv"
            
            combined_csv_path = self.csv_dir / filename
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