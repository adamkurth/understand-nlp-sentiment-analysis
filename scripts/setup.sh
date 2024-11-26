#!/bin/bash

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}Setting up directory structure...${NC}"

# Create required directories
mkdir -p podcasts/downloads
mkdir -p podcasts/txt
mkdir -p podcasts/status

# Check for MP3 files
MP3_COUNT=$(find podcasts/downloads -name "*.mp3" | wc -l)

echo -e "\n${YELLOW}Directory structure:${NC}"
tree podcasts/

if [ $MP3_COUNT -eq 0 ]; then
    echo -e "\n${RED}No MP3 files found in podcasts/downloads!${NC}"
    echo -e "Please ensure your MP3 files are in the podcasts/downloads directory"
    exit 1
else
    echo -e "\n${GREEN}Found $MP3_COUNT MP3 files in podcasts/downloads${NC}"
fi

# Check transcribe.py exists
if [ ! -f "scripts/transcribe.py" ]; then
    echo -e "\n${RED}transcribe.py not found in scripts directory!${NC}"
    exit 1
fi

echo -e "\n${GREEN}Setup complete! You can now run:${NC}"
echo -e "python scripts/transcribe.py --dir podcasts --workers 8 --verbose"