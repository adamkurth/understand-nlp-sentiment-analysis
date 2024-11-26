# understand-nlp-sentiment-analysis



## Using `scripts/` Directory

### Podcast Download and Transcription Pipeline

A simple pipeline for downloading podcast episodes and generating their transcripts. This project consists of two main scripts: `download.py` for fetching podcast episodes and `transcribe.py` for converting them to text.

## Directory Structure

```
project_root/
├── scripts/
│   ├── download.py         # Downloads podcast episodes
│   └── transcribe.py       # Transcribes audio to text
├── podcasts/
│   ├── downloads/          # Downloaded MP3 files
│   ├── txt/               # Generated transcripts
│   └── status/            # Progress tracking files
└── master.csv             # Master tracking file
```

## Prerequisites

```bash
# Install required packages
pip install requests pandas tqdm pydub SpeechRecognition

# Install ffmpeg (required for audio processing)
# macOS
brew install ffmpeg
# Linux
sudo apt install ffmpeg
```

## Setup

1. Create the required directories:
```bash
mkdir -p podcasts/downloads
mkdir -p podcasts/txt
mkdir -p podcasts/status
```

2. Ensure your master.csv has the required columns:
- Episode title
- Podcast title
- Candidate name
- Date posted
- Hyperlink

## Usage

### 1. Download Podcast Episodes

Download all episodes listed in master.csv:

```bash
python scripts/download.py --dir podcasts --verbose
```

Options:
- `--dir`: Base directory for downloads (default: 'podcasts')
- `--verbose`: Enable detailed logging
- `--max-retries`: Maximum retry attempts for failed downloads (default: 3)

The script will:
- Download MP3 files to `podcasts/downloads/`
- Track download status in `podcasts/status/download_status.csv`
- Automatically retry failed downloads
- Maintain metadata about each episode

### 2. Transcribe Episodes

After downloading, transcribe the audio files to text:

```bash
python scripts/transcribe.py --dir podcasts --workers 8 --verbose
```

Options:
- `--dir`: Base directory containing downloads (default: 'podcasts')
- `--workers`: Number of concurrent workers (default: 2)
- `--verbose`: Enable detailed logging

The script will:
- Process all MP3 files in `podcasts/downloads/`
- Generate transcripts in `podcasts/txt/`
- Track progress in `podcasts/status/transcription_status.csv`
- Use multiple workers for faster processing

## Status Tracking

Each script maintains its own status tracking:

- Downloads: `podcasts/status/download_status.csv`
- Transcription: `podcasts/status/transcription_status.csv`

You can check these files to:
- Monitor progress
- Identify failed items
- Resume interrupted operations

## Best Practices

1. Start Small:
   - Test with a few episodes first
   - Use verbose mode to monitor progress
   - Check output quality before full batch

2. Resource Management:
   - Start with 2-4 transcription workers
   - Increase if your system can handle more
   - Monitor system memory usage

3. Handling Large Batches:
   - Scripts can be safely stopped/resumed
   - Progress is saved incrementally
   - Failed items are logged for retry

## Troubleshooting

### Common Issues

1. "No files to process" error:
```bash
# Verify MP3 files exist
ls podcasts/downloads/*.mp3

# Check directory permissions
chmod -R 755 podcasts/
```

2. Download failures:
- Check network connection
- Verify URLs in master.csv
- Look in download_status.csv for error messages

3. Transcription errors:
- Ensure ffmpeg is installed
- Reduce worker count if out of memory
- Check transcription_status.csv for specific errors

### Example Status Check

```bash
# Check download status
cat podcasts/status/download_status.csv

# Check transcription progress
cat podcasts/status/transcription_status.csv
```

### Output Example

```plaintext
Processing 32 files...
Using 4 workers

[1] applepodcasts_harris_60min... |██████████----------| 50% [02:15<02:15]
[2] applepodcasts_trump_rogan... |████████------------| 40% [01:30<02:15]
[3] applepodcasts_harris_view... |██████--------------| 30% [01:45<04:05]
[4] applepodcasts_trump_beck... |████----------------| 20% [00:45<03:00]
```

### Troubleshooting

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
   - 
## Notes

- Both scripts track progress and can be safely interrupted/resumed
- Failed downloads are automatically retried
- Transcripts maintain the same naming convention as source files
- Status files provide detailed logging of all operations

For questions or issues, check the status files in `podcasts/status/` first, as they contain detailed error messages and progress information.