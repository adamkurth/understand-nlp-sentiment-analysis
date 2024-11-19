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
import shutil
from collections import defaultdict
from datetime import datetime

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

def main():
    # Check if master.csv exists
    if not os.path.isfile(MASTER_CSV_PATH):
        print(f"Error: '{MASTER_CSV_PATH}' does not exist.")
        return

    # Read the CSV file into a pandas DataFrame
    df_master = pd.read_csv(MASTER_CSV_PATH)

    # Ensure required columns exist
    required_columns = [
        'Transcript title (Adam transcribed through GitHub)',
        'Podcast title',
        'Candidate name',
        'Date posted'
    ]
    for col in required_columns:
        if col not in df_master.columns:
            print(f"Error: Column '{col}' is missing from '{MASTER_CSV_PATH}'.")
            return

    # Backup the original master.csv
    shutil.copy(MASTER_CSV_PATH, BACKUP_CSV_PATH)
    print(f"Backup of '{MASTER_CSV_PATH}' created as '{BACKUP_CSV_PATH}'.")

    # Create a copy of the DataFrame to avoid SettingWithCopyWarning
    df_master = df_master.copy()

    # Remove extra extensions from 'Transcript title' in master.csv
    df_master['Transcript title (Adam transcribed through GitHub)'] = df_master['Transcript title (Adam transcribed through GitHub)'].apply(remove_extension)

    # Remove extra extensions from 'Date posted' in master.csv
    df_master['Date posted'] = df_master['Date posted'].apply(remove_extension)

    # Build a mapping from old filenames to master.csv entries
    master_file_map = {}
    for idx, row in df_master.iterrows():
        old_title = row['Transcript title (Adam transcribed through GitHub)']
        if pd.isna(old_title) or not str(old_title).strip():
            continue
        master_file_map[old_title] = row

    # Get a list of all files in the transcript directory
    transcript_files = [f for f in os.listdir(TRANSCRIPTS_DIR) if os.path.isfile(os.path.join(TRANSCRIPTS_DIR, f))]

    # Remove extra extensions from filenames in transcript directory
    for filename in transcript_files:
        new_filename = remove_extension(filename)
        if new_filename != filename:
            os.rename(os.path.join(TRANSCRIPTS_DIR, filename), os.path.join(TRANSCRIPTS_DIR, new_filename))
            print(f"Renamed '{filename}' to '{new_filename}'.")
    # Update the transcript_files list
    transcript_files = [f for f in os.listdir(TRANSCRIPTS_DIR) if os.path.isfile(os.path.join(TRANSCRIPTS_DIR, f))]

    # Dictionary to keep track of part numbers for each candidate and podcast
    part_counter = defaultdict(int)

    # List to collect files with unknown or invalid dates
    unknown_date_files = []

    # Process each transcript file
    for filename in transcript_files:
        file_path = os.path.join(TRANSCRIPTS_DIR, filename)

        # Initialize variables
        source = get_source_from_filename(filename)
        candidate_name = None
        podcast_title = None
        date_posted = None

        # Check if the file is in master.csv
        if filename in master_file_map:
            # Get data from master.csv
            row = master_file_map[filename]
            candidate_name = row['Candidate name']
            podcast_title = row['Podcast title']
            date_posted = row['Date posted']
        else:
            # Try to extract data from the filename
            source, candidate_name, _, _ = extract_info_from_filename(filename)
            # Since we cannot reliably get the podcast title from the filename, set as 'unknown'
            podcast_title = 'unknown'
            # Date posted is unknown since it's not in master.csv
            date_posted = 'unknown'

        # If candidate_name is still None, skip this file
        if not candidate_name:
            print(f"Warning: Unable to extract candidate name from '{filename}'. Skipping.")
            continue

        # Clean the data
        candidate_name_clean = clean_string(candidate_name).lower()
        podcast_title_clean = clean_string(podcast_title).lower()

        # Use 'Date posted' from master.csv
        if date_posted and date_posted != 'unknown':
            date_posted_clean = clean_date(date_posted)
            if date_posted_clean == 'unknowndate':
                # Collect the file's details and skip renaming
                unknown_date_files.append({
                    'Filename': filename,
                    'Candidate name': candidate_name,
                    'Podcast title': podcast_title,
                    'Date posted': date_posted
                })
                print(f"Date posted is invalid for '{filename}'. Adding to 'unknown_date.csv'.")
                continue
        else:
            # Collect the file's details and skip renaming
            unknown_date_files.append({
                'Filename': filename,
                'Candidate name': candidate_name,
                'Podcast title': podcast_title,
                'Date posted': date_posted
            })
            print(f"Date posted is missing for '{filename}'. Adding to 'unknown_date.csv'.")
            continue

        # Replace the 'episode title' in the filename with the 'podcast_title_clean'
        # Build the new filename
        key = (candidate_name_clean, podcast_title_clean)
        part_counter[key] += 1
        part_number = part_counter[key]

        new_filename = f"{source}_{candidate_name_clean}_{podcast_title_clean}_{date_posted_clean}"
        # Add '_partX' if there are multiple instances
        if part_counter[key] > 1:
            new_filename += f"_part{part_number}"
        new_filename += ".txt"

        # Rename the file
        new_file_path = os.path.join(TRANSCRIPTS_DIR, new_filename)
        os.rename(file_path, new_file_path)
        print(f"Renamed '{filename}' to '{new_filename}'.")

        # Update master.csv entry
        if filename in master_file_map:
            df_master.loc[df_master['Transcript title (Adam transcribed through GitHub)'] == filename, 'Transcript title (Adam transcribed through GitHub)'] = new_filename
        else:
            # Add a new row to master.csv
            new_row = {
                'Transcript title (Adam transcribed through GitHub)': new_filename,
                'Podcast title': podcast_title,
                'Candidate name': candidate_name,
                'Date posted': date_posted,
                # You can fill other columns as needed or leave them empty
            }
            df_master = pd.concat([df_master, pd.DataFrame([new_row])], ignore_index=True)
            print(f"Added new entry to master.csv for '{new_filename}'.")

    # Save the updated DataFrame back to master.csv
    df_master.to_csv(MASTER_CSV_PATH, index=False)
    print(f"Updated '{MASTER_CSV_PATH}' with new transcript titles.")

    # Save the unknown_date_files to unknown_date.csv
    if unknown_date_files:
        df_unknown = pd.DataFrame(unknown_date_files)
        df_unknown.to_csv(UNKNOWN_DATE_CSV_PATH, index=False)
        print(f"Saved files with unknown or invalid dates to '{UNKNOWN_DATE_CSV_PATH}'.")
    else:
        print("No files with unknown or invalid dates.")

# Helper function to remove extensions from filenames or strings
def remove_extension(s):
    if isinstance(s, str):
        return os.path.splitext(s)[0]
    else:
        return s
# Helper function to extract the source from the filename
def get_source_from_filename(filename):
    # Assume the source is the first segment before the first underscore
    filename = remove_extension(filename)
    return filename.split('_')[0]

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
    filename = remove_extension(filename)
    parts = filename.split('_')
    if len(parts) >= 3:
        source = parts[0]
        candidate_name = parts[1]
        # Since we cannot reliably get the podcast title or date from the filename, return None
        return source, candidate_name, None, None
    else:
        return None, None, None, None

if __name__ == '__main__':
    main()