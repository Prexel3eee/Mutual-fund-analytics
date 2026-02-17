import requests
from bs4 import BeautifulSoup
import os
import re
import time
from urllib.parse import unquote
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Constants
BASE_URL = "https://www.advisorkhoj.com/mutual-funds-research/mutual-fund-portfolio"
BASE_DIR = "Mutual_Fund_Portfolios"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
}

# List of all 43 AMCs extracted from advisorkhoj.com dropdown
AMC_LIST = [
    "360-ONE-Mutual-Fund",
    "Aditya-Birla-Sun-Life-Mutual-Fund",
    "Axis-Mutual-Fund",
    "Bajaj-Finserv-Mutual-Fund",
    "Bandhan-Mutual-Fund",
    "Bank-of-India-Mutual-Fund",
    "Baroda-BNP-Paribas-Mutual-Fund",
    "Canara-Robeco-Mutual-Fund",
    "DSP-Mutual-Fund",
    "Edelweiss-Mutual-Fund",
    "Franklin-Templeton-Mutual-Fund",
    "Groww-Mutual-Fund",
    "HDFC-Mutual-Fund",
    "Helios-Mutual-Fund",
    "HSBC-Mutual-Fund",
    "ICICI-Prudential-Mutual-Fund",
    "Invesco-Mutual-Fund",
    "ITI-Mutual-Fund",
    "JM-Financial-Mutual-Fund",
    "Kotak-Mahindra-Mutual-Fund",
    "LIC-Mutual-Fund",
    "Mahindra-Mutual-Fund",
    "Mirae-Asset-Mutual-Fund",
    "Motilal-Oswal-Mutual-Fund",
    "Navi-Mutual-Fund",
    "Nippon-India-Mutual-Fund",
    "NJ-Mutual-Fund",
    "Old-Bridge-Mutual-Fund",
    "PGIM-India-Mutual-Fund",
    "PPFAS-Mutual-Fund",
    "Quant-Mutual-Fund",
    "Quantum-Mutual-Fund",
    "Samco-Mutual-Fund",
    "SBI-Mutual-Fund",
    "Shriram-Mutual-Fund",
    "Sundaram-Mutual-Fund",
    "Tata-Mutual-Fund",
    "Taurus-Mutual-Fund",
    "Trust-Mutual-Fund",
    "Union-Mutual-Fund",
    "UTI-Mutual-Fund",
    "WhiteOak-Capital-Mutual-Fund",
    "Zerodha-Mutual-Fund",
]

YEARS = [2025, 2026]

# Track downloaded files to avoid duplicates within the same run
downloaded_files = set()
downloaded_files_lock = threading.Lock()


def extract_month_year(link_text):
    """
    Extract month and year from link text like:
    - "Monthly Portfolio Disclosure - January 2026"
    - "January 2026 Portfolio"
    Returns: (month, year) or (None, None) if not found
    """
    # Common month names
    months = [
        "January", "February", "March", "April", "May", "June",
        "July", "August", "September", "October", "November", "December"
    ]
    
    # Try to find month and year pattern
    for month in months:
        # Pattern: "Month YYYY" or "Month YY"
        pattern = rf"{month}\s+(\d{{4}})"
        match = re.search(pattern, link_text, re.IGNORECASE)
        if match:
            year = int(match.group(1))
            return (month, year)
    
    return (None, None)


def download_portfolio(amc_name, year, lock):
    """
    Download portfolio files for a given AMC and year.
    Args:
        amc_name: Name of the AMC
        year: Year to download
        lock: Threading lock for thread-safe access to downloaded_files
    Returns number of files downloaded.
    """
    url = f"{BASE_URL}/{amc_name}/{year}"
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all links that contain "Monthly Portfolio Disclosure" or similar
        portfolio_links = soup.find_all('a', href=True, string=re.compile(r'Monthly Portfolio|Portfolio Disclosure', re.IGNORECASE))
        
        if not portfolio_links:
            print(f"    ⚠ No portfolio links found for {amc_name} ({year})")
            return 0
        
        downloaded_count = 0
        
        for link in portfolio_links:
            link_text = link.get_text(strip=True)
            download_url = link['href']
            
            # Make URL absolute if it's relative
            if not download_url.startswith('http'):
                download_url = f"https://www.advisorkhoj.com{download_url}"
            
            # Extract month and year from link text
            month, detected_year = extract_month_year(link_text)
            
            if not month or not detected_year:
                print(f"    ⚠ Could not parse month/year from: {link_text[:50]}...")
                continue
            
            # Create unique identifier for this file
            file_id = f"{amc_name}_{detected_year}_{month}"
            
            # Skip if already downloaded in this run (thread-safe check)
            with lock:
                if file_id in downloaded_files:
                    print(f"    ⊘ Duplicate skipped: {month} {detected_year}")
                    continue
                # Mark as being processed
                downloaded_files.add(file_id)
            
            # Create directory structure based on detected year
            save_dir = os.path.join(BASE_DIR, amc_name, str(detected_year))
            os.makedirs(save_dir, exist_ok=True)
            
            # Determine filename - extract extension from URL
            # Some AMCs use .xls (old Excel), .xlsx (modern Excel), or .zip (compressed)
            url_path = download_url.split('?')[0]  # Remove query parameters
            if url_path.endswith('.xls'):
                file_extension = '.xls'
            elif url_path.endswith('.xlsx'):
                file_extension = '.xlsx'
            elif url_path.endswith('.zip'):
                file_extension = '.zip'
            else:
                # Default to .xlsx if extension not clear
                file_extension = '.xlsx'
            
            filename = f"{month}{file_extension}"
            full_path = os.path.join(save_dir, filename)
            
            # Check if file already exists (check .xls, .xlsx, and extracted files from .zip)
            if file_extension == '.zip':
                # For ZIP files, check if we already have the extracted folder with files
                month_folder = os.path.join(save_dir, month)
                if os.path.exists(month_folder) and os.path.isdir(month_folder):
                    # Check if folder has files
                    has_files = any(os.listdir(month_folder))
                    if has_files:
                        print(f"    ⊘ Skipped (already extracted): {month} {detected_year}")
                        continue
                # Also check if ZIP file exists (not yet extracted)
                elif os.path.exists(full_path):
                    print(f"    ⊘ Skipped (exists): {month} {detected_year}")
                    continue
            else:
                # For Excel files, check both .xls and .xlsx extensions
                alt_extension = '.xlsx' if file_extension == '.xls' else '.xls'
                alt_filename = f"{month}{alt_extension}"
                alt_full_path = os.path.join(save_dir, alt_filename)
                
                if os.path.exists(full_path):
                    print(f"    ⊘ Skipped (exists): {month} {detected_year}")
                    continue
                elif os.path.exists(alt_full_path):
                    # File exists with different extension - skip
                    print(f"    ⊘ Skipped (exists with different extension): {month} {detected_year}")
                    continue
            
            # Download the file
            try:
                # First, check if this is a direct download link (Excel file) or redirects to a website
                print(f"    ↓ Checking: {month} {detected_year}...", end=" ")
                head_response = requests.head(download_url, headers=HEADERS, timeout=10, allow_redirects=True)
                content_type = head_response.headers.get('Content-Type', '').lower()
                
                # Check if it's actually an Excel or ZIP file
                # Valid content types: application/vnd.ms-excel, application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, application/zip
                is_valid_file = any([
                    'excel' in content_type,
                    'spreadsheet' in content_type,
                    'application/vnd.ms-excel' in content_type,
                    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' in content_type,
                    'application/zip' in content_type,
                    'application/x-zip' in content_type,
                    download_url.endswith('.xlsx'),
                    download_url.endswith('.xls'),
                    download_url.endswith('.zip')
                ])
                
                # Skip if it's an HTML page or other non-Excel/ZIP content
                if 'text/html' in content_type or not is_valid_file:
                    print(f"⊘ Skipped (not direct download - links to website)")
                    continue
                
                print(f"Downloading...", end=" ")
                file_response = requests.get(download_url, headers=HEADERS, stream=True, timeout=30)
                file_response.raise_for_status()
                
                with open(full_path, 'wb') as f:
                    for chunk in file_response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                print("✓")
                # File already added to downloaded_files set before download attempt
                downloaded_count += 1
                
                # Be respectful - add delay between downloads
                time.sleep(1)
                
            except Exception as e:
                print(f"✗ Failed: {e}")
        
        return downloaded_count
        
    except requests.exceptions.RequestException as e:
        print(f"    ✗ Error fetching page: {e}")
        return 0
    except Exception as e:
        print(f"    ✗ Unexpected error: {e}")
        return 0


def extract_all_zip_files(base_dir="Mutual_Fund_Portfolios"):
    """
    Extract all ZIP files in the downloaded portfolios.
    Each month's ZIP is extracted into a subfolder: {Month}/
    This prevents conflicts when multiple sheets are in different months.
    """
    import zipfile
    
    print("\n" + "=" * 70)
    print("Extracting ZIP Files")
    print("=" * 70)
    print()
    
    zip_count = 0
    extracted_count = 0
    error_count = 0
    
    # Walk through all directories
    for root, dirs, files in os.walk(base_dir):
        for filename in files:
            if filename.endswith('.zip'):
                zip_path = os.path.join(root, filename)
                zip_count += 1
                
                # Create extraction folder: remove .zip extension to get month name
                month_name = filename.replace('.zip', '')
                extract_dir = os.path.join(root, month_name)
                
                # Skip if already extracted
                if os.path.exists(extract_dir) and os.path.isdir(extract_dir):
                    # Check if folder has files
                    has_files = any(os.listdir(extract_dir))
                    if has_files:
                        print(f"⊘ Skipped (already extracted): {zip_path}")
                        # Remove the ZIP file if extraction folder exists
                        try:
                            os.remove(zip_path)
                            print(f"   🗑 Removed ZIP file (already extracted)\n")
                        except:
                            print(f"   ⚠ Could not remove ZIP file\n")
                        continue
                
                print(f"📦 Extracting: {zip_path}")
                print(f"   → Into: {extract_dir}/")
                
                try:
                    # Create extraction directory
                    os.makedirs(extract_dir, exist_ok=True)
                    
                    # Extract ZIP
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        # Extract only Excel files, skip macOS metadata
                        excel_files = [f for f in zip_ref.namelist() 
                                     if f.endswith(('.xls', '.xlsx')) and not f.startswith('__MACOSX')]
                        
                        if excel_files:
                            for excel_file in excel_files:
                                zip_ref.extract(excel_file, extract_dir)
                                extracted_count += 1
                                print(f"   ✓ Extracted: {excel_file}")
                        else:
                            print(f"   ⚠ No Excel files found in ZIP")
                    
                    # Close the ZIP file and delete it
                    # Important: close happens automatically with 'with' statement
                    # Wait a moment to ensure file handle is released
                    import time
                    time.sleep(0.1)
                    
                    try:
                        os.remove(zip_path)
                        print(f"   🗑 Removed ZIP file\n")
                    except Exception as delete_error:
                        print(f"   ⚠ Could not delete ZIP: {delete_error}")
                        print(f"   (You can manually delete: {zip_path})\n")
                        
                except Exception as e:
                    print(f"   ✗ Extraction failed: {e}\n")
                    error_count += 1
    
    print("=" * 70)
    print(f"ZIP Extraction Complete!")
    print(f"  ZIP files found: {zip_count}")
    print(f"  Excel files extracted: {extracted_count}")
    if error_count > 0:
        print(f"  Errors: {error_count}")
    print("=" * 70)
    print()


def convert_xls_to_xlsx(base_dir="Mutual_Fund_Portfolios"):
    """
    Convert all .xls files to .xlsx format using Excel COM automation.
    This preserves all formatting and data without loss.
    Requires: pip install pywin32
    """
    print("\n" + "=" * 70)
    print("Converting XLS to XLSX Format")
    print("=" * 70)
    print()
    
    try:
        import win32com.client
    except ImportError:
        print("⚠ pywin32 not installed. Installing...")
        import subprocess
        subprocess.check_call(['pip', 'install', 'pywin32'])
        import win32com.client
    
    # Excel file format constants
    xlOpenXMLWorkbook = 51  # .xlsx format
    
    xls_count = 0
    converted_count = 0
    error_count = 0
    
    # Collect all .xls files first
    xls_files = []
    for root, dirs, files in os.walk(base_dir):
        for filename in files:
            if filename.endswith('.xls') and not filename.endswith('.xlsx'):
                xls_path = os.path.join(root, filename)
                xls_files.append(xls_path)
                xls_count += 1
    
    if xls_count == 0:
        print("No .xls files found to convert")
        print("=" * 70)
        return
    
    print(f"Found {xls_count} .xls files to convert\n")
    
    # Initialize Excel application
    try:
        excel = win32com.client.Dispatch("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False
        
        for xls_path in xls_files:
            try:
                # Convert to absolute path for Excel
                abs_xls_path = os.path.abspath(xls_path)
                xlsx_path = abs_xls_path.replace('.xls', '.xlsx')
                
                print(f"📄 Converting: {os.path.basename(xls_path)}")
                
                # Open the .xls file
                workbook = excel.Workbooks.Open(abs_xls_path)
                
                # Save as .xlsx
                workbook.SaveAs(xlsx_path, FileFormat=xlOpenXMLWorkbook)
                workbook.Close(SaveChanges=False)
                
                print(f"   ✓ Saved as: {os.path.basename(xlsx_path)}")
                
                # Delete original .xls file
                try:
                    os.remove(xls_path)
                    print(f"   🗑 Removed: {os.path.basename(xls_path)}\n")
                except Exception as delete_error:
                    print(f"   ⚠ Could not delete .xls: {delete_error}\n")
                
                converted_count += 1
                
            except Exception as e:
                print(f"   ✗ Conversion failed: {e}\n")
                error_count += 1
                try:
                    workbook.Close(SaveChanges=False)
                except:
                    pass
        
        # Quit Excel
        excel.Quit()
        
    except Exception as e:
        print(f"✗ Excel automation failed: {e}")
        print("Make sure Microsoft Excel is installed on your system.")
        return
    
    print("=" * 70)
    print(f"XLS to XLSX Conversion Complete!")
    print(f"  Files found: {xls_count}")
    print(f"  Successfully converted: {converted_count}")
    if error_count > 0:
        print(f"  Errors: {error_count}")
    print("=" * 70)
    print()


def cleanup_empty_directories(base_dir="Mutual_Fund_Portfolios"):
    """
    Remove empty directories (AMCs with no downloaded files).
    This cleans up folders for AMCs that redirect to their websites.
    """
    print("\n" + "=" * 70)
    print("Cleaning Up Empty Directories")
    print("=" * 70)
    print()
    
    removed_count = 0
    
    if not os.path.exists(base_dir):
        print(f"Directory {base_dir} not found")
        print("=" * 70)
        return
    
    # Check all AMC directories
    for amc_name in os.listdir(base_dir):
        amc_path = os.path.join(base_dir, amc_name)
        
        if not os.path.isdir(amc_path):
            continue
        
        # Check if directory is empty or contains only empty subdirectories
        has_files = False
        
        for root, dirs, files in os.walk(amc_path):
            if files:
                has_files = True
                break
        
        if not has_files:
            # Directory is empty or has only empty subdirectories
            display_name = amc_name.replace('-', ' ')
            print(f"🗑 Removing: {display_name}")
            import shutil
            shutil.rmtree(amc_path)
            removed_count += 1
    
    print()
    print("=" * 70)
    print(f"Cleanup Complete!")
    print(f"  Empty directories removed: {removed_count}")
    print("=" * 70)
    print()


def process_amc(amc_info):
    """Process a single AMC across all years. Thread worker function."""
    idx, amc_name, lock = amc_info
    display_name = amc_name.replace('-', ' ')
    print(f"[{idx}/{len(AMC_LIST)}] Processing: {display_name}")
    
    amc_downloaded = 0
    for year in YEARS:
        downloaded = download_portfolio(amc_name, year, lock)
        amc_downloaded += downloaded
    
    if amc_downloaded > 0:
        print(f"    ✓ Total downloaded for {display_name}: {amc_downloaded} files")
    
    print()
    return amc_downloaded


def main():
    print("=" * 70)
    print("AMC Portfolio Downloader - Advisorkhoj.com (PARALLEL)")
    print("=" * 70)
    print(f"Total AMCs: {len(AMC_LIST)}")
    print(f"Years: {YEARS}")
    
    # Determine number of worker threads (use CPU count)
    import os as os_cpu
    max_workers = os_cpu.cpu_count() or 4  # Default to 4 if cpu_count() returns None
    print(f"Parallel Workers: {max_workers}")
    print("=" * 70)
    print()
    
    total_downloaded = 0
    lock = downloaded_files_lock
    
    # Create list of work items with index
    work_items = [(idx, amc_name, lock) for idx, amc_name in enumerate(AMC_LIST, 1)]
    
    # Use ThreadPoolExecutor for parallel downloads
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks and get futures
        futures = {executor.submit(process_amc, item): item for item in work_items}
        
        # Process completed tasks as they finish
        for future in as_completed(futures):
            try:
                downloaded_count = future.result()
                total_downloaded += downloaded_count
            except Exception as e:
                print(f"Error processing AMC: {e}")
    
    print("=" * 70)
    print(f"Download Complete!")
    print(f"Total files downloaded: {total_downloaded}")
    print("=" * 70)
    
    # Phase 2: Extract all ZIP files
    extract_all_zip_files()
    
    # Phase 3: Convert all .xls to .xlsx format
    convert_xls_to_xlsx()
    
    # Phase 4: Clean up empty directories
    cleanup_empty_directories()


if __name__ == "__main__":
    main()
