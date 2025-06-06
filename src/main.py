"""
Main orchestration module for PDF processing workflow.
"""
import logging
import argparse
from pathlib import Path
from scraper import LACityPlanningScraper

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Scrape and process LA City Planning PDFs')
    parser.add_argument('--start-year', type=int, help='Start year to filter by (inclusive, e.g., 2023)')
    parser.add_argument('--end-year', type=int, help='End year to filter by (inclusive, e.g., 2024)')
    parser.add_argument('--start-month', type=int, help='Start month to filter by (inclusive, 1-12)')
    parser.add_argument('--end-month', type=int, help='End month to filter by (inclusive, 1-12)')
    args = parser.parse_args()
    
    # Validate month ranges if provided
    if args.start_month is not None and not 1 <= args.start_month <= 12:
        logger.error("Start month must be between 1 and 12")
        return
    if args.end_month is not None and not 1 <= args.end_month <= 12:
        logger.error("End month must be between 1 and 12")
        return
    if args.start_month and args.end_month and args.start_month > args.end_month:
        logger.error("Start month must be less than or equal to end month")
        return
    
    # Initialize the scraper and process PDFs
    scraper = LACityPlanningScraper()
    combined_csv = scraper.download_and_process_all_pdfs(
        start_year=args.start_year,
        end_year=args.end_year,
        start_month=args.start_month,
        end_month=args.end_month
    )
    
    if combined_csv:
        logger.info(f"Successfully created combined CSV: {combined_csv}")
    else:
        logger.error("Failed to create combined CSV")

if __name__ == "__main__":
    main() 