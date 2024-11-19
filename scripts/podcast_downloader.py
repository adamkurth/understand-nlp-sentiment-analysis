#!/usr/bin/env python3
import os
import sys
import logging
import argparse
import csv
import tempfile
import shutil
from datetime import datetime
from pathlib import Path
import requests
import re
import pandas as pd

def clean_string(s):
    if not s or not str(s).strip():
        return 'unknown'
    invalid_chars = '<>:"/\\|?*\''
    for char in invalid_chars:
        s = s.replace(char, '')
    s = s.replace(' ', '_')
    s = s.replace(',', '')
    s = s.strip('_')
    return s.lower()

def format_filename(source, candidate_name, podcast_title, date_str):
    candidate_clean = clean_string(candidate_name)
    podcast_clean = clean_string(podcast_title)
    try:
        if '/' in date_str:
            date_obj = datetime.strptime(date_str, '%m/%d/%Y')
        else:
            date_obj = datetime.strptime(date_str, '%Y%m%d')
        date_clean = date_obj.strftime('%Y%m%d')
    except:
        date_clean = datetime.now().strftime('%Y%m%d')
    
    return f"{source}_{candidate_clean}_{podcast_clean}_{date_clean}"

class PodcastBatchDownloader:
    def __init__(self, master_csv, output_dir, temp_dir, verbose=False):
        self.master_csv = Path(master_csv)
        self.output_dir = Path(output_dir)
        self.temp_dir = Path(temp_dir)
        self.logger = self._setup_logger(verbose)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        })

    def _setup_logger(self, verbose):
        logger = logging.getLogger('PodcastDownloader')
        logger.setLevel(logging.DEBUG if verbose else logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def _get_episode_url(self, hyperlink):
        """Extract episode info from Apple Podcast URL"""
        try:
            response = self.session.get(hyperlink)
            response.raise_for_status()
            
            # Look for audio URL in page content
            audio_url_pattern = r'https://[^"\']*\.mp3'
            audio_urls = re.findall(audio_url_pattern, response.text)
            
            if audio_urls:
                return audio_urls[0]
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting episode URL: {e}")
            return None

    def _download_episode(self, row):
        try:
            if not row['Hyperlink'] or 'podcasts.apple.com' not in row['Hyperlink']:
                self.logger.error(f"Invalid or missing Apple Podcast URL for: {row['Episode title']}")
                return False

            # Get audio URL
            audio_url = self._get_episode_url(row['Hyperlink'])
            if not audio_url:
                self.logger.error(f"No audio URL found for: {row['Episode title']}")
                return False

            # Format filename
            filename = format_filename(
                'applepodcasts',
                row['Candidate name'],
                row['Podcast title'],
                row['Date posted']
            )
            temp_path = self.temp_dir / f"{filename}.mp3"
            final_path = self.output_dir / f"{filename}.mp3"

            # Download to temp directory
            self.logger.info(f"Downloading to temp: {temp_path}")
            response = self.session.get(audio_url, stream=True)
            response.raise_for_status()

            total_size = int(response.headers.get('content-length', 0))
            block_size = 8192
            progress = 0

            with open(temp_path, 'wb') as f:
                for chunk in response.iter_content(block_size):
                    if chunk:
                        f.write(chunk)
                        progress += len(chunk)
                        if total_size:
                            percent = (progress / total_size) * 100
                            print(f"\rDownload Progress: {percent:.1f}%", end='')

            print()
            
            # Move to final location
            shutil.move(temp_path, final_path)
            self.logger.info(f"Moved to final location: {final_path}")
            
            return True

        except Exception as e:
            self.logger.error(f"Download error: {e}")
            return False

    def process_all(self):
        """Process all episodes in master.csv"""
        try:
            df = pd.read_csv(self.master_csv)
            
            # Create temp directory
            os.makedirs(self.temp_dir, exist_ok=True)
            os.makedirs(self.output_dir, exist_ok=True)
            
            # Process each row
            for _, row in df.iterrows():
                if pd.isna(row['Hyperlink']) or not row['Hyperlink'].strip():
                    continue
                    
                self.logger.info(f"Processing: {row['Episode title']}")
                success = self._download_episode(row)
                if success:
                    self.logger.info("Download complete!")
                else:
                    self.logger.error("Download failed!")
                    
        except Exception as e:
            self.logger.error(f"Error processing master.csv: {e}")
        finally:
            # Cleanup temp directory
            shutil.rmtree(self.temp_dir, ignore_errors=True)

def main():
    parser = argparse.ArgumentParser(description='Batch Apple Podcast Downloader')
    parser.add_argument('--master', '-m', default='master.csv',
                       help='Path to master CSV file')
    parser.add_argument('--output', '-o', 
                       default='podcasts',
                       help='Output directory for downloaded episodes')
    parser.add_argument('--temp', '-t',
                       default=tempfile.mkdtemp(),
                       help='Temporary directory for downloads')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    args = parser.parse_args()

    downloader = PodcastBatchDownloader(
        args.master,
        args.output,
        args.temp,
        args.verbose
    )
    downloader.process_all()

if __name__ == "__main__":
    main()