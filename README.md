# ADU Scraper

Identify ADU permit applications with the LA Department of City Planning

## Setup

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

Run the scraper with optional year and month range filters:

```bash
# Process all available PDFs
python src/main.py

# Process PDFs from a specific year range (inclusive)
python src/main.py --start-year 2023 --end-year 2024

# Process PDFs from a specific month range (inclusive)
python src/main.py --start-month 3 --end-month 6

# Process PDFs from a specific year and month range (inclusive)
python src/main.py --start-year 2023 --end-year 2024 --start-month 1 --end-month 6
```

The script will:
1. Download PDFs from the LA City Planning website
2. Extract data from the PDFs
3. Convert the data to CSV format
4. Combine all CSVs into a single file

Output files will be saved in the `csvs` directory with filenames indicating the date range used.
