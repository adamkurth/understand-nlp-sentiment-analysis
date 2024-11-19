#!/usr/bin/env python3
import os
import sys
import logging
import argparse
import json
import time
import pandas as pd
from pathlib import Path
from datetime import datetime
import speech_recognition as sr
from pydub import AudioSegment
from pydub.silence import split_on_silence
import concurrent.futures
import tempfile
import shutil
from tqdm.auto import tqdm
import threading

class FileProgress:
    def __init__(self, desc, total, position):
        self.pbar = tqdm(
            total=total,
            desc=f"[{position+1}] {desc}",
            position=position,
            leave=False,
            unit='%'
        )
        self._last_update = 0

    def update(self, amount):
        current = min(amount, 100)
        increment = current - self._last_update
        if increment > 0:
            self.pbar.update(increment)
            self._last_update = current

    def close(self):
        self.pbar.close()
class TranscriptionManager:
    def __init__(self, podcast_dir):
        self.podcast_dir = Path(podcast_dir)
        if not self.podcast_dir.exists():
            raise ValueError(f"Directory not found: {podcast_dir}")
            
        # Create directory for status files if it doesn't exist
        self.status_dir = self.podcast_dir / 'status'
        self.status_dir.mkdir(exist_ok=True)
        
        self.status_file = self.status_dir / 'transcription_status.csv'
        self.status_lock = threading.Lock()
        self.position_lock = threading.Lock()
        self.current_position = 0
        
        # Initialize DataFrame with columns
        self.status_df = pd.DataFrame(columns=[
            'mp3_file', 'txt_file', 'status', 'duration', 'start_time',
            'end_time', 'error', 'word_count'
        ])
        
        self.load_status()

    def load_status(self):
        """Load or create status file"""
        try:
            if self.status_file.exists():
                try:
                    df = pd.read_csv(self.status_file)
                    # Verify required columns exist
                    missing_cols = set(self.status_df.columns) - set(df.columns)
                    if missing_cols:
                        print(f"Warning: Missing columns in status file: {missing_cols}")
                        # Add missing columns
                        for col in missing_cols:
                            df[col] = ''
                    self.status_df = df
                except pd.errors.EmptyDataError:
                    print("Creating new status file...")
                    self.save_status_file()
                except Exception as e:
                    print(f"Error reading status file: {e}")
                    print("Creating new status file...")
                    self.save_status_file()
            else:
                print("Creating new status file...")
                self.save_status_file()
        except Exception as e:
            print(f"Error initializing status: {e}")
            raise

    def save_status_file(self):
        """Save status DataFrame to file"""
        try:
            self.status_df.to_csv(self.status_file, index=False)
        except Exception as e:
            print(f"Error saving status file: {e}")

    def save_status(self, mp3_file, txt_file, status, **kwargs):
        with self.status_lock:
            try:
                row = {
                    'mp3_file': str(mp3_file),
                    'txt_file': str(txt_file),
                    'status': status,
                    **kwargs
                }
                
                # Update existing or append new
                idx = self.status_df.index[
                    self.status_df['mp3_file'] == str(mp3_file)
                ].tolist()
                
                if idx:
                    self.status_df.loc[idx[0]] = row
                else:
                    self.status_df = pd.concat([
                        self.status_df, 
                        pd.DataFrame([row])
                    ], ignore_index=True)
                
                self.save_status_file()
                
            except Exception as e:
                print(f"Error updating status: {e}")

    def get_pending_files(self):
        """Get list of MP3 files that need processing"""
        try:
            completed = set(
                self.status_df[self.status_df['status'] == 'completed']['mp3_file']
            )
            
            # Get all MP3 files from podcast directory
            all_files = []
            for mp3_file in self.podcast_dir.glob('*.mp3'):
                try:
                    if mp3_file.stat().st_size > 0:  # Check if file is not empty
                        all_files.append(str(mp3_file))
                except Exception as e:
                    print(f"Error checking file {mp3_file}: {e}")
            
            # Get files that need processing
            pending = sorted(list(set(all_files) - completed))
            print(f"Found {len(pending)} files to process out of {len(all_files)} total files")
            return pending
            
        except Exception as e:
            print(f"Error getting pending files: {e}")
            return []

    def get_next_position(self):
        """Get next position for progress bar"""
        with self.position_lock:
            pos = self.current_position
            self.current_position += 1
            return pos

class Transcriber:
    def __init__(self, manager, max_workers=2):
        self.manager = manager
        self.max_workers = max_workers
        self.recognizer = sr.Recognizer()

    def transcribe_chunk(self, audio_chunk, retries=3, delay=1):
        for attempt in range(retries):
            try:
                text = self.recognizer.recognize_google(audio_chunk)
                return text.strip()
            except sr.UnknownValueError:
                return ""
            except sr.RequestError:
                if attempt < retries - 1:
                    time.sleep(delay)
                    delay *= 2
                else:
                    raise

    def process_file(self, mp3_path):
        mp3_path = Path(mp3_path)
        txt_path = mp3_path.with_suffix('.txt')
        temp_path = mp3_path.with_suffix('.temp.txt')
        position = self.manager.get_next_position()

        progress = FileProgress(
            desc=mp3_path.name[:40],
            total=100,
            position=position
        )

        try:
            start_time = datetime.now()
            self.manager.save_status(
                mp3_path, txt_path, 'processing',
                start_time=start_time.isoformat()
            )

            # Load audio file
            progress.update(5)
            audio = AudioSegment.from_mp3(mp3_path)
            if audio.channels > 1:
                audio = audio.set_channels(1)

            # Split into chunks
            progress.update(10)
            chunks = split_on_silence(
                audio,
                min_silence_len=500,
                silence_thresh=-40,
                keep_silence=150
            )
            
            # Process chunks
            total_chunks = len(chunks)
            chunk_texts = []
            chunk_progress = 80 / total_chunks if total_chunks else 80

            with tempfile.TemporaryDirectory() as temp_dir:
                for i, chunk in enumerate(chunks):
                    # Export chunk
                    chunk_path = Path(temp_dir) / f"chunk_{i}.wav"
                    chunk.export(chunk_path, format="wav")
                    
                    # Transcribe
                    with sr.AudioFile(str(chunk_path)) as source:
                        audio_data = self.recognizer.record(source)
                        text = self.transcribe_chunk(audio_data)
                        if text:
                            chunk_texts.append(text)
                    
                    # Save progress
                    current_progress = 10 + (i + 1) * chunk_progress
                    progress.update(current_progress)
                    
                    with open(temp_path, 'w') as f:
                        f.write('\n'.join(chunk_texts))

            # Finalize
            if os.path.exists(temp_path):
                shutil.move(temp_path, txt_path)

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            word_count = len(' '.join(chunk_texts).split())

            self.manager.save_status(
                mp3_path, txt_path, 'completed',
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat(),
                duration=duration,
                word_count=word_count
            )

            progress.update(100)

        except Exception as e:
            self.manager.save_status(
                mp3_path, txt_path, 'failed',
                error=str(e)
            )
            raise
        finally:
            progress.close()

    def process_all(self):
        pending = self.manager.get_pending_files()
        if not pending:
            print("No files to process")
            return

        print(f"\nProcessing {len(pending)} files...")
        print(f"Using {self.max_workers} workers\n")

        # Sort by size for better parallelization
        pending.sort(key=lambda x: os.path.getsize(x), reverse=True)

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(self.process_file, file)
                for file in pending
            ]
            concurrent.futures.wait(futures)

        print("\nProcessing complete!")

def main():
    parser = argparse.ArgumentParser(description='Batch Podcast Transcriber')
    parser.add_argument('--dir', '-d', default='podcasts',
                       help='Directory containing podcast MP3 files')
    parser.add_argument('--workers', '-w', type=int, default=2,
                       help='Number of concurrent workers')
    args = parser.parse_args()

    manager = TranscriptionManager(args.dir)
    transcriber = Transcriber(manager, args.workers)
    transcriber.process_all()

if __name__ == "__main__":
    main()