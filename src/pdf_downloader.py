"""
Module for downloading PDF files from URLs.
"""
import os
import re
import requests
from urllib.parse import urlparse, unquote
from typing import Optional


def get_filename_from_response(response: requests.Response, url: str) -> str:
    """
    Extract filename from response headers or URL.
    
    Args:
        response: HTTP response object
        url: Original URL
        
    Returns:
        Filename with .pdf extension
    """
    # Try to get filename from Content-Disposition header
    if 'Content-Disposition' in response.headers:
        content_disposition = response.headers['Content-Disposition']
        filename_match = re.search(r'filename="?([^"]+)"?', content_disposition)
        if filename_match:
            filename = unquote(filename_match.group(1))
            if not filename.endswith('.pdf'):
                filename += '.pdf'
            return filename
    
    # Try to get filename from URL
    filename = os.path.basename(urlparse(url).path)
    if not filename.endswith('.pdf'):
        filename = 'downloaded.pdf'
    
    return filename


def download_pdf(url: str, output_dir: str) -> Optional[str]:
    """
    Download a PDF from a URL and save it to the output directory.
    
    Args:
        url: URL of the PDF to download
        output_dir: Directory where the PDF should be saved
        
    Returns:
        Path to the downloaded PDF file, or None if download failed
    """
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Download the PDF
        print(f"Downloading PDF from {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        # Get filename from response
        filename = get_filename_from_response(response, url)
        output_path = os.path.join(output_dir, filename)
        
        # Save the PDF
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                
        print(f"PDF saved to {output_path}")
        return output_path
        
    except requests.exceptions.RequestException as e:
        print(f"Error downloading PDF: {str(e)}")
        return None 