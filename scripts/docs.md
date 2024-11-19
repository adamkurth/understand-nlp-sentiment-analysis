# Podcast Processing Scripts

A collection of scripts for downloading, transcribing, and managing podcast episodes. These scripts handle the complete workflow from downloading episodes to generating transcripts.

## Directory Structure

```bash
scripts/
├── podcast_batch_downloader.py   # Batch downloader for podcast episodes
├── podcast_downloader.py         # Single episode downloader
├── podcast_transcriber.py        # Audio to text transcription
├── update_transcript_titles.py   # Filename standardization
└── master.csv                    # Master tracking file
```

## Prerequisites

```bash
# Install required packages
pip install requests pandas tqdm pydub SpeechRecognition librosa numpy scipy

# For audio processing
brew install ffmpeg    # macOS
sudo apt install ffmpeg # Linux
```

## Using the Scripts

### 1. Download Podcast Episodes

#### Batch Downloader (Recommended)
```bash
# Download all episodes from master.csv
python scripts/podcast_batch_downloader.py -m master.csv -o podcasts -v

Options:
  -m, --master    Path to master CSV file (default: master.csv)
  -o, --output    Output directory for MP3 files (default: podcasts)
  -v, --verbose   Enable verbose logging
```

#### Single Episode Downloader
```bash
# Download specific episode
python scripts/podcast_downloader.py -m master.csv -o podcasts -v

# Download episode by title
python scripts/podcast_downloader.py "Episode Title" -o podcasts
```

### 2. Transcribe Episodes

```bash
# Transcribe downloaded episodes
python scripts/podcast_transcriber.py -d podcasts -w 4 -v

Options:
  -d, --dir       Directory containing MP3 files (default: podcasts)
  -w, --workers   Number of concurrent workers (default: 2)
  -v, --verbose   Enable verbose logging
```

### 3. Standardize Filenames

```bash
# Update transcript filenames to match standard format
python scripts/update_transcript_titles.py
```

## File Formats

### master.csv
Required columns:
- Episode title
- Podcast title
- Candidate name
- Date posted
- Hyperlink

### Output Files
- **MP3 Files**: `podcasts/*.mp3`
- **Transcripts**: `podcasts/*.txt`
- **Status Files**:
  - `transcription_status.csv`: Transcription progress
  - `download_status.csv`: Download status
  - `retry_list.csv`: Failed downloads

## Progress Tracking

Each script maintains its own status tracking:

- **Downloads**: Check `download_status.csv`
- **Transcription**: Check `transcription_status.csv`
- **Failed Items**: Check `retry_list.csv`

## Best Practices

1. **Download First, Transcribe Later**
   ```bash
   # Step 1: Download all episodes
   python scripts/podcast_batch_downloader.py -m master.csv -o podcasts -v
   
   # Step 2: Transcribe downloaded episodes
   python scripts/podcast_transcriber.py -d podcasts -w 4
   
   # Step 3: Standardize filenames
   python scripts/update_transcript_titles.py
   ```

2. **Handling Large Batches**
   - Start with a few episodes to test
   - Use 2-4 workers for transcription
   - Monitor system resources

3. **Resuming Interrupted Jobs**
   - Scripts automatically track progress
   - Can be safely stopped and resumed
   - Will skip already completed files

## Troubleshooting

1. **Download Issues**
   - Check network connection
   - Verify URLs in master.csv
   - Look in retry_list.csv for failed downloads

2. **Transcription Issues**
   - Ensure ffmpeg is installed
   - Check available memory
   - Reduce worker count if needed

3. **Common Errors**
   - "No audio URL found": URL might be invalid
   - "File not found": Check file paths
   - "Memory error": Reduce concurrent workers

## Output Example

```plaintext
Processing 32 files...
Using 4 workers

[1] applepodcasts_harris_60min... |██████████----------| 50% [02:15<02:15]
[2] applepodcasts_trump_rogan... |████████------------| 40% [01:30<02:15]
[3] applepodcasts_harris_view... |██████--------------| 30% [01:45<04:05]
[4] applepodcasts_trump_beck... |████----------------| 20% [00:45<03:00]
```

## Maintenance

- Regular backups of master.csv are created
- Failed downloads are logged for retry
- Progress is saved incrementally
