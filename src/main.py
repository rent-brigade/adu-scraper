"""
Main orchestration module for PDF processing workflow.
"""
import logging
from pathlib import Path
from scraper import LACityPlanningScraper

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    # Initialize the scraper and process all PDFs
    scraper = LACityPlanningScraper()
    combined_csv = scraper.download_and_process_all_pdfs(start_year=2020)
    
    if combined_csv:
        logger.info(f"Successfully created combined CSV: {combined_csv}")
    else:
        logger.error("Failed to create combined CSV")

if __name__ == "__main__":
    main() 