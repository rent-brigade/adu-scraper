"""
Module for processing PDF files and extracting data.
"""
import csv
import os
import re
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
import pandas as pd
import pdfplumber


# Define standard columns
STANDARD_COLUMNS = [
    "Council District",
    "Filing Date",
    "Case Number",
    "Address",
    "Community Plan Area",
    "Project Description",
    "Request Type",
    "Applicant Contact",
    "Is ADU",
    "PDF URL"
]


def clean_text(text: str) -> str:
    """
    Clean text by removing extra whitespace and newlines.
    
    Args:
        text: Text to clean
        
    Returns:
        Cleaned text
    """
    if not text:
        return ""
    # Replace newlines with spaces and normalize whitespace
    return " ".join(text.replace("\n", " ").split())


def map_column_name(header: str) -> str:
    """
    Map a header to a standard column name.
    
    Args:
        header: Original header text
        
    Returns:
        Standard column name
    """
    header = clean_text(header).lower()
    
    # Map variations to standard names
    header_map = {
        "filing date": "Filing Date",
        "application date": "Filing Date",
        "case number": "Case Number",
        "case": "Case Number",
        "address": "Address",
        "cd#": "Council District",
        "council district": "Council District",
        "community plan area": "Community Plan Area",
        "community plan": "Community Plan Area",
        "project description": "Project Description",
        "description": "Project Description",
        "request type": "Request Type",
        "request": "Request Type",
        "applicant contact": "Applicant Contact",
        "applicant": "Applicant Contact",
        "contact": "Applicant Contact"
    }
    
    # Try exact match first
    if header in header_map:
        return header_map[header]
    
    # Try partial match
    for key, value in header_map.items():
        if key in header:
            return value
    
    return None


def extract_cnc_from_title(title: str) -> str:
    """
    Extract CNC name from a title string.
    
    Args:
        title: Title string like "Certified Neighborhood Council -- Arroyo Seco"
        
    Returns:
        CNC name
    """
    match = re.search(r'Certified Neighborhood Council\s*--\s*(.+)', title)
    return match.group(1) if match else "Unknown"


def is_header_row(row: List[str]) -> bool:
    """
    Determine if a row is likely a header row.
    
    Args:
        row: List of cell values
        
    Returns:
        True if the row appears to be a header row
    """
    if not row:
        return False
        
    # Check if any cell contains common header terms
    header_terms = ['date', 'case', 'address', 'cnc', 'community', 'project', 'request', 'applicant', 'contact']
    row_text = ' '.join(str(cell).lower() for cell in row if cell)
    
    # Print row text for debugging
    print(f"Checking header row: {row_text}")
    
    return any(term in row_text for term in header_terms)


def find_header_row(table: List[List[str]]) -> Tuple[int, Dict[int, str]]:
    """
    Find the header row in a table and create column mapping.
    
    Args:
        table: 2D list of table data
        
    Returns:
        Tuple of (header_row_index, column_mapping)
    """
    # Print first few rows for debugging
    print("\nFirst few rows of table:")
    for i, row in enumerate(table[:5]):
        print(f"Row {i}: {[str(cell) for cell in row]}")
    
    for i, row in enumerate(table):
        if is_header_row(row):
            # Create column mapping
            column_mapping = {}
            for j, cell in enumerate(row):
                if cell:
                    std_name = map_column_name(str(cell))
                    if std_name:
                        column_mapping[j] = std_name
                        print(f"Found column mapping: {j} -> {std_name}")
            if column_mapping:  # Only return if we found valid headers
                return i, column_mapping
    return -1, {}


def clean_council_district(value: str) -> str:
    """
    Clean and validate a Council District value.
    If the full value is invalid but numeric, tries using all characters after the first one.
    
    Args:
        value: Raw Council District value
        
    Returns:
        Cleaned Council District value or empty string if invalid
    """
    if not value:
        return ""
        
    # Try the full value first
    try:
        district_num = int(value)
        if 1 <= district_num <= 15:
            return str(district_num)
    except ValueError:
        pass
        
    # If that fails and we have more than one character, try all but the first
    if len(value) > 1:
        try:
            district_num = int(value[1:])
            if 1 <= district_num <= 15:
                return str(district_num)
        except (ValueError, IndexError):
            pass
            
    return ""


def process_table(table: List[List[str]], column_mapping: Optional[Dict[int, str]] = None, pdf_url: Optional[str] = None) -> Tuple[List[Dict[str, Any]], Dict[int, str]]:
    """
    Process a single table, maintaining header information.
    
    Args:
        table: 2D list of table data
        column_mapping: Optional existing column mapping
        pdf_url: Optional URL of the source PDF
        
    Returns:
        Tuple of (list of processed rows, column mapping)
    """
    processed_rows = []
    
    # If no column mapping provided, try to find headers
    if not column_mapping:
        header_row_idx, column_mapping = find_header_row(table)
        if header_row_idx == -1:
            print("No valid headers found in table")
            return [], {}
        start_idx = header_row_idx + 1
        print(f"Found headers at row {header_row_idx}")
    else:
        start_idx = 0
        print("Using existing column mapping")
    
    # Process data rows (skip header row and last row)
    for row_idx, row in enumerate(table[start_idx:-1], start_idx):
        # Skip if row is empty or if it matches header pattern
        if not any(cell for cell in row) or is_header_row(row):
            continue
            
        # Skip CNC header rows and summary rows
        if len(row) > 0 and ("Certified Neighborhood Council" in str(row[0]) or "CNC Records:" in str(row[0])):
            continue
            
        row_data = {col: "" for col in STANDARD_COLUMNS}  # Initialize with empty values
        row_data["PDF URL"] = pdf_url if pdf_url else ""
        
        for i, cell in enumerate(row):
            if i in column_mapping:
                value = clean_text(str(cell))
                # Special handling for Council District
                if column_mapping[i] == "Council District":
                    value = clean_council_district(value)
                row_data[column_mapping[i]] = value
        
        # Check if Project Description contains ADU
        project_desc = row_data.get("Project Description", "").lower()
        row_data["Is ADU"] = "true" if re.search(r'\badu\b', project_desc) else "false"
        
        # Print first few processed rows for debugging
        if row_idx < start_idx + 3:
            print(f"Processed row {row_idx}: {row_data}")
        
        processed_rows.append(row_data)
    
    return processed_rows, column_mapping


def extract_tables_from_pdf(pdf_path: str, pdf_url: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Extract all tables from a PDF file.
    
    Args:
        pdf_path: Path to the PDF file
        pdf_url: Optional URL of the source PDF
        
    Returns:
        List of dictionaries containing the extracted data
    """
    all_data = []
    current_column_mapping = None
    
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, 1):
            print(f"\nProcessing page {page_num}")
            
            # Try different table extraction settings
            tables = page.extract_tables({
                'vertical_strategy': 'lines_strict',
                'horizontal_strategy': 'lines_strict',
                'intersection_tolerance': 3,
                'snap_tolerance': 3,
                'join_tolerance': 3,
                'edge_min_length': 3,
            })
            
            if not tables:
                print(f"No tables found on page {page_num}")
                continue
            
            print(f"Found {len(tables)} tables on page {page_num}")
            
            # Process each table
            for table_idx, table in enumerate(tables, 1):
                if not table or len(table) < 2:  # Skip empty tables or tables without enough rows
                    print(f"Skipping table {table_idx} on page {page_num} - too small")
                    continue
                
                print(f"\nProcessing table {table_idx} on page {page_num}")
                
                # Process the table
                rows, column_mapping = process_table(table, current_column_mapping, pdf_url)
                all_data.extend(rows)
                
                # Update current column mapping
                if rows:
                    current_column_mapping = column_mapping
    
    return all_data


def save_to_csv(data: List[Dict[str, Any]], output_path: str) -> None:
    """
    Save extracted data to a CSV file.
    
    Args:
        data: List of dictionaries containing the data
        output_path: Path where the CSV file should be saved
    """
    if not data:
        return
    
    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=STANDARD_COLUMNS)
        writer.writeheader()
        writer.writerows(data)


def process_pdf_directory(input_dir: str, output_dir: str) -> None:
    """
    Process all PDF files in a directory and save results as CSV files.
    
    Args:
        input_dir: Directory containing PDF files
        output_dir: Directory where CSV files should be saved
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Process each PDF file
    for pdf_file in Path(input_dir).glob('*.pdf'):
        try:
            print(f"\nProcessing file: {pdf_file.name}")
            # Extract data from PDF
            data = extract_tables_from_pdf(str(pdf_file))
            
            if data:
                # Use the PDF filename (without extension) for the CSV
                output_filename = pdf_file.stem + '.csv'
                output_path = os.path.join(output_dir, output_filename)
                save_to_csv(data, output_path)
                print(f"Saved {len(data)} records to {output_path}")
            else:
                print(f"No data was extracted from {pdf_file.name}")
                
        except Exception as e:
            print(f"Error processing {pdf_file.name}: {str(e)}")


class PDFProcessor:
    def __init__(self):
        pass
        
    def process_pdf(self, pdf_path: str, pdf_url: Optional[str] = None) -> pd.DataFrame:
        """
        Process a PDF file and return the extracted data as a DataFrame.
        
        Args:
            pdf_path: Path to the PDF file
            pdf_url: Optional URL of the source PDF
            
        Returns:
            DataFrame containing the extracted data
        """
        try:
            # Extract tables from PDF
            data = extract_tables_from_pdf(pdf_path, pdf_url)
            
            # Convert to DataFrame
            if data:
                df = pd.DataFrame(data)
                return df
            return None
            
        except Exception as e:
            print(f"Error processing PDF {pdf_path}: {e}")
            return None 