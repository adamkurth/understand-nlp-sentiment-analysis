#!/usr/bin/env python3
import os
import sys
import logging
import argparse
import tempfile
import shutil
from datetime import datetime
from pathlib import Path
import requests
import re
import pandas as pd
import json
import urllib.parse
import unicodedata
from time import sleep

class PodcastDownloader:
    def __init__(self, master_csv, podcast_dir, verbose=False):
        self.master_csv = Path(master_csv)
        self.output_dir = Path(podcast_dir) / 'downloads'
        self.metadata_file = self.output_dir / 'metadata.csv'
        self.logger = self._setup_logger(verbose)
        self.session = self._setup_session()
        self.metadata_df = pd.DataFrame(columns=[
            'original_url', 'audio_urls', 'title', 'description', 'duration',
            'extracted_at', 'candidate_name', 'podcast_title', 'episode_title',
            'date_posted', 'downloaded_at', 'download_path', 'status', 'error_message'
        ])
        self._load_existing_metadata()

    def _setup_logger(self, verbose):
        logger = logging.getLogger('PodcastDownloader')
        logger.setLevel(logging.DEBUG if verbose else logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def _setup_session(self):
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Origin': 'https://podcasts.apple.com'
        })
        return session

    def _load_existing_metadata(self):
        if self.metadata_file.exists():
            try:
                existing_metadata = pd.read_csv(self.metadata_file)
                # Ensure all columns exist
                for col in self.metadata_df.columns:
                    if col not in existing_metadata.columns:
                        existing_metadata[col] = None
                self.metadata_df = existing_metadata
                self.logger.info(f"Loaded {len(existing_metadata)} existing metadata records")
            except Exception as e:
                self.logger.error(f"Error loading existing metadata: {e}")

    def clean_string(self, text):
        if not text or not isinstance(text, str):
            return 'unknown'
        text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')
        text = re.sub(r'[^\w\s-]', ' ', text.lower())
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def format_filename(self, source, candidate_name, podcast_title, date_str):
        try:
            if pd.isna(date_str):
                date_clean = datetime.now().strftime('%Y%m%d')
            elif '/' in str(date_str):
                date_obj = datetime.strptime(date_str, '%m/%d/%Y')
                date_clean = date_obj.strftime('%Y%m%d')
            else:
                date_obj = datetime.strptime(str(date_str), '%Y%m%d')
                date_clean = date_obj.strftime('%Y%m%d')
        except Exception as e:
            self.logger.warning(f"Date parsing error: {e}, using current date")
            date_clean = datetime.now().strftime('%Y%m%d')
        
        return f"{source}_{self.clean_string(candidate_name)}_{self.clean_string(podcast_title)}_{date_clean}"

    def extract_metadata(self, url, content):
        try:
            if not url or pd.isna(url) or url.lower() == 'unavailable':
                return None

            # Audio URL patterns
            audio_patterns = [
                r'https://[^"\']*\.mp3[^"\']*',
                r'https://[^"\']*audio[^"\']*\.m4a[^"\']*',
                r'https://[^"\']*\.aac[^"\']*',
                r'https://dts\.podtrac\.com/[^"\']*',
                r'https://chrt\.fm/track/[^"\']*',
                r'https://pdst\.fm/[^"\']*',
                r'https://traffic\.megaphone\.fm/[^"\']*',
                r'https://play\.podtrac\.com/[^"\']*',
                r'https://www\.podtrac\.com/pts/redirect\.mp3/[^"\']*'
            ]
            
            all_audio_urls = []
            seen_urls = set()
            for pattern in audio_patterns:
                matches = re.findall(pattern, content)
                for url in matches:
                    clean_url = url.split('"')[0].split("'")[0]
                    if clean_url not in seen_urls:
                        all_audio_urls.append(clean_url)
                        seen_urls.add(clean_url)

            # Extract metadata with multiple fallback patterns
            title = self._extract_with_patterns(content, [
                r'<title>(.*?)</title>',
                r'<meta property="og:title" content="(.*?)"',
                r'<h1[^>]*>(.*?)</h1>',
                r'<meta name="title" content="(.*?)"'
            ])

            description = self._extract_with_patterns(content, [
                r'<meta name="description" content="(.*?)"',
                r'<meta property="og:description" content="(.*?)"',
                r'<div[^>]*class="[^"]*description[^"]*"[^>]*>(.*?)</div>',
                r'<meta name="twitter:description" content="(.*?)"'
            ])

            duration = self._extract_with_patterns(content, [
                r'duration.*?(\d+:\d+)',
                r'"duration":\s*"(.*?)"',
                r'itemprop="duration"[^>]*>(.*?)<',
                r'data-duration="(.*?)"'
            ])

            return {
                'original_url': url,
                'audio_urls': json.dumps(all_audio_urls),
                'title': title,
                'description': description,
                'duration': duration,
                'extracted_at': datetime.now().isoformat()
            }
        except Exception as e:
            self.logger.error(f"Metadata extraction error for {url}: {str(e)}")
            return None

    def _extract_with_patterns(self, content, patterns):
        """Helper method to extract content using multiple patterns"""
        for pattern in patterns:
            try:
                match = re.search(pattern, content, re.IGNORECASE)
                if match:
                    return match.group(1).strip()
            except Exception:
                continue
        return None

    def get_audio_url_with_retries(self, url, episode_title, retries=3):
        if not url or pd.isna(url) or url.lower() == 'unavailable':
            self.logger.warning(f"No valid URL for: {episode_title}")
            return None, None

        for attempt in range(retries):
            try:
                # Try direct URL first
                response = self.session.get(url)
                if response.status_code == 200:
                    metadata = self.extract_metadata(url, response.text)
                    if metadata and metadata['audio_urls']:
                        audio_urls = json.loads(metadata['audio_urls'])
                        return audio_urls[0], metadata

                # If no direct URL found, try iTunes search
                if attempt < retries - 1:
                    search_term = self.clean_string(episode_title)
                    words = search_term.split()
                    if len(words) > attempt + 1:
                        search_term = ' '.join(words[:-(attempt)])
                    
                    encoded_term = urllib.parse.quote(search_term)
                    search_url = f"https://itunes.apple.com/search?term={encoded_term}&entity=podcastEpisode&limit=20"
                    
                    self.logger.info(f"Trying iTunes search: {search_term}")
                    search_response = self.session.get(search_url)
                    if search_response.status_code == 200:
                        data = search_response.json()
                        for result in data.get('results', []):
                            episode_url = result.get('episodeUrl')
                            if episode_url:
                                # Try this URL with only one retry
                                return self.get_audio_url_with_retries(episode_url, episode_title, retries=1)
                
                sleep(1 + attempt)  # Progressive delay between retries
                
            except Exception as e:
                self.logger.error(f"Error getting audio URL (attempt {attempt+1}/{retries}): {str(e)}")
                if attempt < retries - 1:
                    sleep(2 ** attempt)  # Exponential backoff
        
        return None, None

    def update_metadata(self, row, metadata=None, download_path=None, status='pending', error_message=None):
        """Update metadata with status tracking"""
        try:
            # Create base record
            record = {
                'candidate_name': row['Candidate name'],
                'podcast_title': row['Podcast title'],
                'episode_title': row['Episode title'],
                'date_posted': row['Date posted'],
                'status': status,
                'error_message': error_message,
                'downloaded_at': datetime.now().isoformat() if download_path else None,
                'download_path': str(download_path) if download_path else None,
                'original_url': None,
                'audio_urls': None,
                'title': None,
                'description': None,
                'duration': None,
                'extracted_at': None
            }

            # Update with metadata if provided
            if metadata:
                for key in record.keys():
                    if key in metadata:
                        record[key] = metadata[key]

            # Check for existing entry
            mask = (self.metadata_df['episode_title'] == row['Episode title']) & \
                  (self.metadata_df['podcast_title'] == row['Podcast title'])
            
            if mask.any():
                # Only update if new status is 'completed' or existing status is 'failed'
                existing_status = self.metadata_df.loc[mask, 'status'].iloc[0]
                if status == 'completed' or existing_status == 'failed':
                    # Update row by row to avoid alignment issues
                    idx = self.metadata_df[mask].index[0]
                    for key, value in record.items():
                        self.metadata_df.at[idx, key] = value
            else:
                # Add new entry
                self.metadata_df = pd.concat([
                    self.metadata_df,
                    pd.DataFrame([record])
                ], ignore_index=True)
            
            # Save updated metadata
            self.metadata_df.to_csv(self.metadata_file, index=False)
            
        except Exception as e:
            self.logger.error(f"Error updating metadata: {str(e)}")
            # Print debugging information
            self.logger.debug(f"Record keys: {list(record.keys())}")
            self.logger.debug(f"DataFrame columns: {list(self.metadata_df.columns)}")

    def retry_failed_downloads(self, max_retries=3):
        """Retry all failed downloads"""
        retry_count = 0
        while retry_count < max_retries:
            retry_count += 1
            failed_entries = self.metadata_df[self.metadata_df['status'] == 'failed']
            
            if failed_entries.empty:
                self.logger.info("No failed downloads to retry")
                break
                
            self.logger.info(f"Retry attempt {retry_count}/{max_retries} for {len(failed_entries)} failed downloads")
            
            # Match failed entries with master CSV data
            df_master = pd.read_csv(self.master_csv)
            for _, failed_row in failed_entries.iterrows():
                matching_rows = df_master[
                    (df_master['Episode title'] == failed_row['episode_title']) &
                    (df_master['Podcast title'] == failed_row['podcast_title'])
                ]
                
                if not matching_rows.empty:
                    row = matching_rows.iloc[0]
                    self.logger.info(f"Retrying download for: {row['Episode title']}")
                    
                    filename = self.format_filename(
                        'applepodcasts',
                        row['Candidate name'],
                        row['Podcast title'],
                        row['Date posted']
                    )
                    mp3_path = self.output_dir / f"{filename}.mp3"
                    
                    # Try with different variations of the episode title
                    episode_variations = [
                        row['Episode title'],
                        row['Episode title'].split('|')[0].strip(),
                        ' '.join(row['Episode title'].split()[:3]),
                        row['Podcast title'] + ' ' + row['Episode title']
                    ]
                    
                    for variation in episode_variations:
                        audio_url, metadata = self.get_audio_url_with_retries(
                            row.get('Hyperlink'),
                            variation,
                            retries=5  # More retries for failed downloads
                        )
                        
                        if audio_url:
                            try:
                                response = self.session.get(audio_url, stream=True)
                                response.raise_for_status()
                                
                                with open(mp3_path, 'wb') as f:
                                    for chunk in response.iter_content(chunk_size=8192):
                                        if chunk:
                                            f.write(chunk)
                                
                                self.logger.info(f"Successfully downloaded on retry: {mp3_path}")
                                self.update_metadata(row, metadata, mp3_path, status='completed')
                                break
                                
                            except Exception as e:
                                self.logger.error(f"Retry download error: {str(e)}")
                                continue
                    
                    sleep(2)  # Add delay between retries
            
            # Check if all downloads are complete
            still_failed = len(self.metadata_df[self.metadata_df['status'] == 'failed'])
            if still_failed == 0:
                self.logger.info("All downloads completed successfully")
                break
            else:
                self.logger.info(f"{still_failed} downloads still failed after retry {retry_count}")
                sleep(5)  # Wait before next retry batch
        
        return len(self.metadata_df[self.metadata_df['status'] == 'failed'])

    def process_all(self):
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            
            df = pd.read_csv(self.master_csv)
            total_rows = len(df)
            processed = 0
            
            for _, row in df.iterrows():
                try:
                    if pd.isna(row['Episode title']):
                        continue
                    
                    processed += 1
                    self.logger.info(f"Processing [{processed}/{total_rows}]: {row['Episode title']}")
                    
                    filename = self.format_filename(
                        'applepodcasts',
                        row['Candidate name'],
                        row['Podcast title'],
                        row['Date posted']
                    )
                    mp3_path = self.output_dir / f"{filename}.mp3"
                    
                    if mp3_path.exists():
                        self.logger.info(f"File exists: {mp3_path}")
                        try:
                            response = self.session.get(row['Hyperlink'])
                            metadata = self.extract_metadata(row['Hyperlink'], response.text)
                            self.update_metadata(row, metadata, mp3_path, status='completed')
                        except Exception as e:
                            self.logger.error(f"Metadata update error for existing file: {str(e)}")
                        continue
                    
                    audio_url, metadata = self.get_audio_url_with_retries(
                        row.get('Hyperlink'),
                        row['Episode title']
                    )
                    
                    if audio_url:
                        try:
                            response = self.session.get(audio_url, stream=True)
                            response.raise_for_status()
                            
                            with open(mp3_path, 'wb') as f:
                                for chunk in response.iter_content(chunk_size=8192):
                                    if chunk:
                                        f.write(chunk)
                            
                            self.logger.info(f"Downloaded: {mp3_path}")
                            self.update_metadata(row, metadata, mp3_path, status='completed')
                            
                        except Exception as e:
                            error_msg = f"Download error: {str(e)}"
                            self.logger.error(error_msg)
                            self.update_metadata(row, metadata, status='failed', error_message=error_msg)
                    else:
                        error_msg = f"No audio URL found: {row['Episode title']}"
                        self.logger.error(error_msg)
                        self.update_metadata(row, status='failed', error_message=error_msg)
                    
                except Exception as e:
                    self.logger.error(f"Error processing row: {str(e)}")
                    continue  # Continue with next row even if current fails
            
        except Exception as e:
            self.logger.error(f"Process error: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description='Enhanced Podcast Downloader with Metadata Collection')
    parser.add_argument('--master', '-m', default='master.csv',
                       help='Path to master CSV file')
    parser.add_argument('--dir', '-d', default='podcasts',
                       help='Base directory for downloads')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    parser.add_argument('--max-retries', '-r', type=int, default=3,
                       help='Maximum number of retry attempts for failed downloads')
    args = parser.parse_args()

    downloader = PodcastDownloader(
        args.master,
        args.dir,
        args.verbose
    )
    
    # Process all files
    downloader.process_all()
    
    # Retry failed downloads
    failed_count = downloader.retry_failed_downloads(max_retries=args.max_retries)
    
    if failed_count > 0:
        print(f"\nWARNING: {failed_count} downloads still failed after all retries")
        sys.exit(1)
    else:
        print("\nAll downloads completed successfully!")
        sys.exit(0)

if __name__ == "__main__":
    main()