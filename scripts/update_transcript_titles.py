#!/usr/bin/env python3
"""
Script to standardize transcript filenames in 'podcast-transcripts/' directory and update 'master.csv'.

Key Features:
- Processes all files in 'podcast-transcripts/', even those not listed in 'master.csv'.
- Renames files to match the format:
  [source]_[candidate_name_clean]_[podcast_title_clean]_[date_posted_clean][_partX].txt
- Replaces the 'episode title' in the filename with the 'Podcast title' from 'master.csv'.
- Uses 'Date posted' from 'master.csv' when available.
- Removes any extra extensions (e.g., '.rtf') from 'Date posted' and filenames.
- Collects files with missing or invalid 'Date posted' into 'unknown_date.csv' without renaming them.
- Adds '_partX' to filenames when multiple instances exist for the same candidate and podcast.
- Updates 'master.csv' with new filenames, adding entries for files not originally present.
"""

import os
import pandas as pd
from pathlib import Path
import shutil
from collections import defaultdict
from datetime import datetime
import logging

# --------------------------- Configuration ---------------------------

# Path to the master CSV file
MASTER_CSV_PATH = 'master.csv'

# Directory containing the transcript files
TRANSCRIPTS_DIR = 'podcast-transcripts'

# Backup file name for the original master.csv
BACKUP_CSV_PATH = 'master_backup.csv'

# Path to the unknown_date CSV file
UNKNOWN_DATE_CSV_PATH = 'unknown_date.csv'

# ------------------------- Script Execution -------------------------
def setup_logger():
    """Set up logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('transcript_processing.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def standardize_master_csv(df_master, logger):
    """Standardize filenames in master.csv"""
    # Create a copy to avoid warnings
    df = df_master.copy()
    
    # Process each row
    for idx, row in df.iterrows():
        filename = row['Transcript title (Adam transcribed through GitHub)']
        if pd.isna(filename) or not isinstance(filename, str):
            continue
            
        # Get base name without extension
        base_name = remove_extension(filename)
        parts = base_name.split('_')
        
        if len(parts) >= 4:  # Ensure enough parts
            # Change source to applepodcasts
            parts[0] = 'applepodcasts'
            
            # Get clean values
            candidate = clean_string(row['Candidate name'])
            podcast = clean_string(row['Podcast title'])
            date = clean_date(row['Date posted'])
            
            # Build new filename
            new_name = f"applepodcasts_{candidate}_{podcast}_{date}"
            
            # Add part number if needed
            if '_part' in filename:
                part_num = filename.split('_part')[-1].split('.')[0]
                new_name += f"_part{part_num}"
            
            new_name += ".txt"
            
            # Update DataFrame
            df.at[idx, 'Transcript title (Adam transcribed through GitHub)'] = new_name
            logger.info(f"Updated master.csv entry: {filename} -> {new_name}")
    
    return df

def validate_file(file_path, required_columns):
    """Validate CSV file and required columns"""
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    df = pd.read_csv(file_path)
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in {file_path}: {missing_cols}")
    return df

def process_transcripts(logger, df_master, transcript_files):
    """Process transcript files and update master CSV"""
    part_counter = defaultdict(int)
    unknown_date_files = []
    
    # Build mapping of existing files using Podcast title
    master_file_map = {
        row['Transcript title (Adam transcribed through GitHub)']: row
        for _, row in df_master.iterrows()
        if pd.notna(row['Transcript title (Adam transcribed through GitHub)'])
    }

    for filename in transcript_files:
        if not filename.endswith('.txt'):
            continue
            
        file_path = os.path.join(TRANSCRIPTS_DIR, filename)
        base_name = os.path.splitext(filename)[0]

        # Always use applepodcasts as source
        source = 'applepodcasts'
        metadata = get_file_metadata(filename, master_file_map.get(filename))

        if not metadata['candidate_name']:
            logger.warning(f"Unable to extract candidate name from '{filename}'. Skipping.")
            continue

        # Clean data
        cleaned_data = clean_metadata(metadata)

        # Process date - first try the date from metadata, then try extracting from filename
        date_clean = None
        
        if metadata['date_posted'] and metadata['date_posted'] != 'unknown':
            date_clean = clean_date(metadata['date_posted'])
        
        # If date is still unknown, try to extract from filename
        if not date_clean or date_clean == 'unknowndate':
            _, _, _, date_from_filename = extract_info_from_filename(filename)
            if date_from_filename:
                date_clean = date_from_filename
            else:
                unknown_date_files.append({
                    'Filename': filename,
                    **metadata
                })
                logger.info(f"Unable to extract valid date from '{filename}'. Added to unknown_date.csv.")
                continue

        # Generate new filename
        new_filename = generate_filename(
            source,
            cleaned_data['candidate_name'],
            cleaned_data['podcast_title'],
            date_clean,
            part_counter
        )

        # Rename file
        if new_filename != filename:
            new_path = os.path.join(TRANSCRIPTS_DIR, new_filename)
            os.rename(file_path, new_path)
            logger.info(f"Renamed '{filename}' to '{new_filename}'")

            # Update master CSV
            if filename in master_file_map:
                mask = df_master['Transcript title (Adam transcribed through GitHub)'] == filename
                df_master.loc[mask, 'Transcript title (Adam transcribed through GitHub)'] = new_filename
            else:
                new_row = {
                    'Transcript title (Adam transcribed through GitHub)': new_filename,
                    **metadata
                }
                df_master = pd.concat([df_master, pd.DataFrame([new_row])], ignore_index=True)
                logger.info(f"Added new entry to master.csv for '{new_filename}'")

    return df_master, unknown_date_files

def get_file_metadata(filename, master_entry=None):
    """Extract metadata from filename or master entry"""
    if master_entry is not None and pd.notna(master_entry['Date posted']):
        return {
            'candidate_name': master_entry['Candidate name'],
            'podcast_title': master_entry['Podcast title'],
            'date_posted': master_entry['Date posted']
        }
    
    # If no master entry or date is missing, try to extract from filename
    source, candidate_name, podcast_title, date_from_filename = extract_info_from_filename(filename)
    
    if date_from_filename:
        return {
            'candidate_name': candidate_name,
            'podcast_title': podcast_title,
            'date_posted': date_from_filename
        }
    
    # If we have a master entry but no date, use other info from master
    if master_entry is not None:
        return {
            'candidate_name': master_entry['Candidate name'],
            'podcast_title': master_entry['Podcast title'],
            'date_posted': 'unknown'
        }
    
    return {
        'candidate_name': candidate_name,
        'podcast_title': podcast_title,
        'date_posted': 'unknown'
    }

def clean_metadata(metadata):
    """Clean metadata values for filename use"""
    return {
        'candidate_name': clean_string(metadata['candidate_name']).lower(),
        'podcast_title': clean_string(metadata['podcast_title']).lower(),
        'date_posted': metadata['date_posted']
    }

def generate_filename(source, candidate, podcast, date, part_counter):
    """Generate standardized filename"""
    key = (candidate, podcast)
    part_counter[key] += 1
    
    base_name = f"{source}_{candidate}_{podcast}_{date}"
    if part_counter[key] > 1:
        base_name += f"_part{part_counter[key]}"
    return f"{base_name}.txt"

# Helper function to remove extensions from filenames or strings
def remove_extension(s):
    if isinstance(s, str):
        return os.path.splitext(s)[0]
    else:
        return s
    
# Helper function to extract the source from the filename
def get_source_from_filename():
    """Always return 'applepodcasts' as source"""
    return 'applepodcasts' #static since we got all from applepodcasts

# Helper function to clean strings for filenames
def clean_string(s):
    if pd.isna(s) or not str(s).strip():
        return 'unknown'
    # Remove or replace invalid filename characters
    invalid_chars = '<>:"/\\|?*\''
    for char in invalid_chars:
        s = s.replace(char, '')
    s = s.replace(' ', '')
    s = s.replace(',', '')
    s = s.strip('_')
    return s

# Helper function to clean date format
def clean_date(date_str):
    if pd.isna(date_str) or not str(date_str).strip():
        return 'unknowndate'
    # Remove extension if any
    date_str = remove_extension(date_str)
    # Expected formats: MM/DD/YYYY or YYYYMMDD
    try:
        if '/' in date_str:
            # Format: MM/DD/YYYY
            month, day, year = date_str.strip().split('/')
            if len(year) == 2:
                year = '20' + year  # Handle YY format
            return f"{year}{month.zfill(2)}{day.zfill(2)}"
        elif len(date_str) == 8 and date_str.isdigit():
            # Format: YYYYMMDD
            return date_str
        else:
            # Try parsing with datetime
            parsed_date = datetime.strptime(date_str.strip(), '%Y%m%d')
            return parsed_date.strftime('%Y%m%d')
    except ValueError:
        return 'unknowndate'

# Helper function to extract information from filename when not in master.csv
def extract_info_from_filename(filename):
    """Extract information from filename when not in master.csv"""
    filename = remove_extension(filename)
    parts = filename.split('_')
    
    if len(parts) >= 4:  # We expect at least 4 parts: source_name_podcast_date
        source = get_source_from_filename()  # static
        candidate_name = parts[1]
        podcast_title = parts[2]
        
        # Extract date from the filename
        date_part = parts[3]
        # If there's a part number, remove it to get the date
        if '_part' in date_part:
            date_part = date_part.split('_part')[0]
        
        # Validate the date format (YYYYMMDD)
        if len(date_part) == 8 and date_part.isdigit():
            try:
                # Verify it's a valid date
                datetime.strptime(date_part, '%Y%m%d')
                return source, candidate_name, podcast_title, date_part
            except ValueError:
                pass
        
        return source, candidate_name, podcast_title, None
    else:
        return None, None, None, None

def main():
    logger = setup_logger()
    
    try:
        # Validate master CSV
        required_columns = [
            'Transcript title (Adam transcribed through GitHub)',
            'Podcast title',
            'Candidate name',
            'Date posted'
        ]
        df_master = validate_file(MASTER_CSV_PATH, required_columns)
        
        # Create backup
        shutil.copy(MASTER_CSV_PATH, BACKUP_CSV_PATH)
        logger.info(f"Created backup: {BACKUP_CSV_PATH}")

        # First standardize master.csv
        df_master = standardize_master_csv(df_master, logger)
        df_master.to_csv(MASTER_CSV_PATH, index=False)
        logger.info("Updated master.csv with standardized filenames")

        # Get transcript files
        if not os.path.exists(TRANSCRIPTS_DIR):
            os.makedirs(TRANSCRIPTS_DIR)
            logger.info(f"Created directory: {TRANSCRIPTS_DIR}")

        transcript_files = [
            f for f in os.listdir(TRANSCRIPTS_DIR)
            if os.path.isfile(os.path.join(TRANSCRIPTS_DIR, f))
            and f.endswith('.txt')
        ]

        if not transcript_files:
            logger.warning("No transcript files found.")
            return

        # Process files
        df_master, unknown_date_files = process_transcripts(
            logger, df_master, transcript_files
        )

        # Save final results
        df_master.to_csv(MASTER_CSV_PATH, index=False)
        logger.info(f"Updated {MASTER_CSV_PATH}")

        if unknown_date_files:
            pd.DataFrame(unknown_date_files).to_csv(UNKNOWN_DATE_CSV_PATH, index=False)
            logger.info(f"Created {UNKNOWN_DATE_CSV_PATH} with {len(unknown_date_files)} files")
        else:
            logger.info("No files with unknown dates")

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise

if __name__ == '__main__':
    main()
