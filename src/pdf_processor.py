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
    "CNC",
    "Community Plan Area",
    "Project Description",
    "Request Type",
    "Applicant Contact",
    "Is ADU"
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
        "case number": "Case Number",
        "case": "Case Number",
        "address": "Address",
        "cnc": "CNC",
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


def extract_district_from_title(title: str) -> str:
    """
    Extract district number from a title string.
    
    Args:
        title: Title string like "Council District -- 1"
        
    Returns:
        District number as string
    """
    match = re.search(r'Council District\s*--\s*(\d+)', title)
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


def process_table(table: List[List[str]], district: str, column_mapping: Optional[Dict[int, str]] = None) -> Tuple[List[Dict[str, Any]], Dict[int, str]]:
    """
    Process a single table, maintaining header information.
    
    Args:
        table: 2D list of table data
        district: Council district number
        column_mapping: Optional existing column mapping
        
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
            
        # Skip district header rows (e.g. "Council District -- 1,None,None,None,None,None,None,None")
        if len(row) > 1 and "Council District" in str(row[0]) and all(str(cell).lower() == "none" for cell in row[1:]):
            continue
            
        row_data = {col: "" for col in STANDARD_COLUMNS}  # Initialize with empty values
        row_data["Council District"] = district
        
        for i, cell in enumerate(row):
            if i in column_mapping:
                row_data[column_mapping[i]] = clean_text(str(cell))
        
        # Check if Project Description contains ADU
        project_desc = row_data.get("Project Description", "").lower()
        row_data["Is ADU"] = "true" if re.search(r'\badu\b', project_desc) else "false"
        
        # Print first few processed rows for debugging
        if row_idx < start_idx + 3:
            print(f"Processed row {row_idx}: {row_data}")
        
        processed_rows.append(row_data)
    
    return processed_rows, column_mapping


def extract_tables_from_pdf(pdf_path: str) -> List[Dict[str, Any]]:
    """
    Extract all tables from a PDF file, adding district information.
    
    Args:
        pdf_path: Path to the PDF file
        
    Returns:
        List of dictionaries containing the extracted data
    """
    all_data = []
    current_district = None
    current_column_mapping = None
    current_table = []  # Keep track of the current table across pages
    
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
                print(f"Table has {len(table)} rows")
                
                # Print the entire table for debugging
                print("\nTable contents:")
                for i, row in enumerate(table):
                    print(f"Row {i}: {[str(cell) for cell in row]}")
                
                # Check if this is a new district table
                first_cell = str(table[0][0]) if table[0] and table[0][0] else ""
                if "Council District" in first_cell:
                    # Process any remaining rows from the previous table
                    if current_table and current_district and current_column_mapping:
                        rows, _ = process_table(current_table, current_district, current_column_mapping)
                        all_data.extend(rows)
                        current_table = []
                    
                    current_district = extract_district_from_title(first_cell)
                    current_column_mapping = None  # Reset column mapping for new district
                    print(f"Found new district: {current_district}")
                
                if current_district:
                    # If we have a column mapping, this might be a continuation of the previous table
                    if current_column_mapping:
                        # Check if this looks like a continuation (no headers, similar structure)
                        if not any(is_header_row(row) for row in table):
                            print("Detected table continuation")
                            current_table.extend(table)
                            continue
                    
                    # Process the table, maintaining column mapping across pages
                    rows, new_mapping = process_table(table, current_district, current_column_mapping)
                    if new_mapping:  # Update mapping if we found new headers
                        current_column_mapping = new_mapping
                        print(f"Found new column mapping: {new_mapping}")
                        current_table = table  # Start a new table
                    else:
                        current_table.extend(table)  # Add to current table
                    
                    print(f"Processed {len(rows)} rows for district {current_district}")
                    all_data.extend(rows)
    
    # Process any remaining rows from the last table
    if current_table and current_district and current_column_mapping:
        rows, _ = process_table(current_table, current_district, current_column_mapping)
        all_data.extend(rows)
    
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
        self.standard_columns = STANDARD_COLUMNS
    
    def process_pdf(self, pdf_path: str) -> pd.DataFrame:
        """
        Process a PDF file and return the data as a pandas DataFrame.
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            DataFrame containing the extracted data
        """
        data = extract_tables_from_pdf(pdf_path)
        if not data:
            return None
            
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Ensure all standard columns exist
        for col in self.standard_columns:
            if col not in df.columns:
                df[col] = ""
                
        # Reorder columns to match standard order
        df = df[self.standard_columns]
        
        return df 