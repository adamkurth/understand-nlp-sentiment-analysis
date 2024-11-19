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
import unicodedata
import urllib.parse
import glob

def clean_search_term(text):
    """Clean text for search purposes"""
    if not text or not isinstance(text, str):
        return ''
    # Remove special characters and extra spaces
    text = text.lower()
    text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
    text = re.sub(r'[^\w\s-]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def clean_filename(text):
    """Clean text for filename purposes"""
    if not text or not isinstance(text, str):
        return 'unknown'
    text = text.lower()
    text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
    text = re.sub(r'[^\w\s-]', '_', text)
    text = re.sub(r'[-\s]+', '_', text)
    text = re.sub(r'_+', '_', text).strip('_')
    return text

def format_filename(source, candidate_name, podcast_title, date_str):
    try:
        if '/' in date_str:
            date_obj = datetime.strptime(date_str, '%m/%d/%Y')
        else:
            date_obj = datetime.strptime(date_str, '%Y%m%d')
        date_clean = date_obj.strftime('%Y%m%d')
    except:
        date_clean = datetime.now().strftime('%Y%m%d')
    
    return f"{clean_filename(source)}_{clean_filename(candidate_name)}_{clean_filename(podcast_title)}_{date_clean}"


class DownloadTracker:
    def __init__(self, output_dir, status_file='download_status.csv', retry_file='retry_list.csv'):
        self.output_dir = Path(output_dir)
        self.status_file = status_file
        self.retry_file = retry_file
        self.results = []
        self.existing_files = self._get_existing_files()
        self.fieldnames = [
            'timestamp', 'episode_title', 'podcast_title', 'candidate_name',
            'date_posted', 'status', 'error_message', 'output_path', 'file_size'
        ]

    def _get_existing_files(self):
        """Get list of already downloaded files"""
        existing = {}
        for file in glob.glob(str(self.output_dir / "*.mp3")):
            path = Path(file)
            existing[path.name] = path.stat().st_size
        return existing

    def add_result(self, row, status, error_message='', output_path=''):
        file_size = ''
        if output_path:
            path = Path(output_path)
            if path.exists():
                file_size = f"{path.stat().st_size / (1024*1024):.1f}M"

        self.results.append({
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'episode_title': row['Episode title'],
            'podcast_title': row['Podcast title'],
            'candidate_name': row['Candidate name'],
            'date_posted': row['Date posted'],
            'status': status,
            'error_message': error_message,
            'output_path': output_path,
            'file_size': file_size
        })

    def is_downloaded(self, filename):
        """Check if file already exists"""
        return filename in self.existing_files

    def save(self):
        """Save status and create retry list"""
        # Save full status
        with open(self.status_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames)
            writer.writeheader()
            writer.writerows(self.results)

        # Create retry list
        retry_rows = []
        for result in self.results:
            if result['status'] == 'FAILED':
                retry_rows.append({
                    'episode_title': result['episode_title'],
                    'podcast_title': result['podcast_title'],
                    'candidate_name': result['candidate_name'],
                    'date_posted': result['date_posted'],
                    'error_message': result['error_message']
                })

        if retry_rows:
            with open(self.retry_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=['episode_title', 'podcast_title', 
                                                     'candidate_name', 'date_posted', 
                                                     'error_message'])
                writer.writeheader()
                writer.writerows(retry_rows)
class PodcastBatchDownloader:
    def __init__(self, master_csv, output_dir, temp_dir, verbose=False):
        self.master_csv = Path(master_csv)
        self.output_dir = Path(output_dir)
        self.temp_dir = Path(temp_dir)
        self.logger = self._setup_logger(verbose)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
        })
        self.tracker = DownloadTracker(output_dir)

    def _setup_logger(self, verbose):
        logger = logging.getLogger('PodcastDownloader')
        logger.setLevel(logging.DEBUG if verbose else logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def _get_audio_url(self, url):
        """Extract audio URL from podcast page"""
        try:
            response = self.session.get(url)
            response.raise_for_status()
            
            # Look for audio URL
            audio_patterns = [
                r'https://[^"\']*\.mp3',
                r'https://pdst\.fm/[^"\']*',
                r'https://chrt\.fm/[^"\']*',
                r'https://traffic\.megaphone\.fm/[^"\']*',
                r'https://dts\.podtrac\.com/[^"\']*\.mp3',
                r'https://[^"\']*\.libsyn\.com/[^"\']*'
            ]
            
            for pattern in audio_patterns:
                urls = re.findall(pattern, response.text)
                if urls:
                    # Clean up URL
                    audio_url = urls[0].split('"')[0].split("'")[0]
                    return audio_url
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error extracting audio URL: {e}")
            return None

    def _search_episode(self, episode_title, hyperlink):
        """Search for episode using iTunes API if direct URL fails"""
        try:
            # First try direct URL if it's an Apple Podcast URL
            if hyperlink and 'podcasts.apple.com' in str(hyperlink):
                audio_url = self._get_audio_url(hyperlink)
                if audio_url:
                    return hyperlink, audio_url

            # Search using iTunes API
            search_term = clean_search_term(episode_title)
            encoded_term = urllib.parse.quote(search_term)
            search_url = f"https://itunes.apple.com/search?term={encoded_term}&entity=podcastEpisode&limit=20"
            
            self.logger.debug(f"Searching iTunes: {search_url}")
            response = self.session.get(search_url)
            response.raise_for_status()
            data = response.json()

            # Try exact match first
            for result in data.get('results', []):
                if clean_search_term(result.get('trackName', '')) == search_term:
                    episode_url = result.get('trackViewUrl')
                    if episode_url:
                        audio_url = self._get_audio_url(episode_url)
                        if audio_url:
                            return episode_url, audio_url

            # If no exact match, try partial match
            for result in data.get('results', []):
                if search_term in clean_search_term(result.get('trackName', '')):
                    episode_url = result.get('trackViewUrl')
                    if episode_url:
                        audio_url = self._get_audio_url(episode_url)
                        if audio_url:
                            return episode_url, audio_url

            return None, None

        except Exception as e:
            self.logger.error(f"Search error: {e}")
            return None, None

    def _download_episode(self, row):
        try:
            # Format filename first to check if already downloaded
            filename = format_filename(
                'applepodcasts',
                row['Candidate name'],
                row['Podcast title'],
                row['Date posted']
            ) + '.mp3'

            if self.tracker.is_downloaded(filename):
                self.logger.info(f"Already downloaded: {filename}")
                final_path = self.output_dir / filename
                self.tracker.add_result(row, 'SKIPPED', 'Already downloaded', str(final_path))
                return True

            if pd.isna(row['Hyperlink']):
                self.tracker.add_result(row, 'FAILED', 'Missing URL')
                return False

            episode_url, audio_url = self._search_episode(row['Episode title'], row['Hyperlink'])
            if not audio_url:
                self.tracker.add_result(row, 'FAILED', 'No audio URL found')
                return False

            temp_path = self.temp_dir / filename
            final_path = self.output_dir / filename

            # Download to temp
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
            self.tracker.add_result(row, 'SUCCESS', output_path=str(final_path))
            return True

        except Exception as e:
            self.tracker.add_result(row, 'FAILED', str(e))
            return False

    def process_all(self):
        try:
            df = pd.read_csv(self.master_csv)
            os.makedirs(self.temp_dir, exist_ok=True)
            os.makedirs(self.output_dir, exist_ok=True)
            
            for _, row in df.iterrows():
                if pd.isna(row['Episode title']):
                    continue
                    
                self.logger.info(f"Processing: {clean_search_term(row['Episode title'])}")
                self._download_episode(row)
                    
        except Exception as e:
            self.logger.error(f"Error processing master.csv: {e}")
        finally:
            self.tracker.save()
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