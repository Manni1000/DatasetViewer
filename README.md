# e621 Dataset Viewer

A desktop application for browsing and searching the e621 captioned dataset. Downloads data directly from HuggingFace, stores it efficiently in SQLite, and provides a fast searchable interface with e621-style search syntax.

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![License](https://img.shields.io/badge/License-MIT-green.svg)

## Features

- **Direct HuggingFace Download**: Streams the dataset directly to your database - no intermediate files needed
- **Efficient Storage**: All data stored in SQLite, no raw JSON bloat
- **Progressive Loading**: Start browsing immediately while download continues in background
- **e621-Style Search**: Full search syntax including tags, ratings, scores, wildcards, and more
- **Image Preview**: View images directly in the app (downloaded from e621 with proper API compliance)
- **Multiple Caption Types**: View tags, summaries, individual parts, Midjourney prompts, DeviantArt commission text
- **Copy to Clipboard**: Easy copy buttons for all text fields
- **Responsive UI**: Scales with window size

## Screenshots

```
┌─────────────────────────────────────────────────────────────────────┐
│ [Download Dataset] [Refresh List]  Status: Ready. 50000 items.     │
│ ─────────────────────────────────────────────────────────────────── │
│ Search: [canine -fox rating:s score:>50          ] [Search] [?]    │
│ ─────────────────────────────────────────────────────────────────── │
│ │ Posts: 1523 shown │                                              │
│ │ ID: 12345        ◄│  ┌─────────────────────────────────┐         │
│ │ ID: 12344         │  │                                 │         │
│ │ ID: 12343         │  │         [Image Preview]         │         │
│ │ ID: 12342         │  │                                 │         │
│ │ ...               │  └─────────────────────────────────┘         │
│ │                   │  [Tags] [Summary] [Parts] [Midjourney] [DA]  │
│ │                   │  ┌─────────────────────────────────┐         │
│ │                   │  │ [Copy Tags]                     │         │
│ │                   │  │ canine                          │         │
│ │                   │  │ wolf                            │         │
│ │                   │  │ male                            │         │
│ │                   │  │ ...                             │         │
│ │                   │  └─────────────────────────────────┘         │
└─────────────────────────────────────────────────────────────────────┘
```

## Requirements

- Python 3.9 or higher
- ~20GB disk space for full dataset (or use test mode for ~100MB)
- Internet connection for initial download

## Installation

### 1. Clone or Download

```bash
# Clone the repository
git clone https://github.com/Manni1000/DatasetViewer.git
cd e621-dataset-viewer

# Or just download the files directly
```

### 2. Create Python Virtual Environment

**Windows:**
```bash
# Create virtual environment
python -m venv venv

# Activate it
venv\Scripts\activate
```

**macOS / Linux:**
```bash
# Create virtual environment
python3 -m venv venv

# Activate it
source venv/bin/activate
```

### 3. Install Dependencies

```bash
# Make sure your virtual environment is activated, then:
pip install -r requirements.txt
```

### 4. Run the Application

```bash
python datasetviewer.py
```

## First Run

1. When you first start the app, a dialog will ask if you want to download the dataset
2. **Test Mode** (checked by default): Downloads only 3 files (~100MB) for testing
3. **Full Download**: Uncheck test mode to download the complete dataset (~20GB)
4. You can start browsing as soon as the first file is processed
5. Download progress is shown at the top of the window

## Search Syntax

Click the **[?]** button next to the search bar for full documentation. Here's a quick reference:

### Basic Tags
```
wolf dog           # Posts must have ALL tags (AND)
~wolf ~dog         # Posts must have ANY of these tags (OR)  
-fox               # Exclude posts with this tag (NOT)
```

### Rating
```
rating:s           # Safe only
rating:q           # Questionable only
rating:e           # Explicit only
-rating:e          # Exclude explicit
```

### Score & Favorites
```
score:>100         # Score greater than 100
score:<50          # Score less than 50
favcount:>500      # More than 500 favorites
```

### File Properties
```
type:png           # PNG files only
type:gif           # GIF files only
-type:webm         # Exclude videos
width:>1920        # Width greater than 1920px
height:>1080       # Height greater than 1080px
filesize:>1mb      # Larger than 1MB
```

### ID & Wildcards
```
id:>100000         # Posts with ID > 100000
id:12345           # Exact post ID
cat*               # Tags starting with "cat"
*cat               # Tags ending with "cat"
*cat*              # Tags containing "cat"
```

### Example Searches
```
canine -fox rating:s score:>50
~wolf ~dog rating:e favcount:>100
dragon type:png width:>1920
anthro male solo score:>200 -young
```

## Data Storage

- **Database**: `dataset_v3.db` (SQLite) - Created in the same directory as the script
- **No raw JSON**: Only essential fields are stored to minimize disk usage

## Troubleshooting

### "Required libraries not found"
Make sure you've activated your virtual environment and installed requirements:
```bash
source venv/bin/activate  # or venv\Scripts\activate on Windows
pip install -r requirements.txt
```

### Images not loading
- e621 has a rate limit of 2 requests/second
- Some images may be deleted or unavailable
- Check your internet connection

### Database errors after update
If you get schema errors after updating the code, delete `dataset_v3.db` and re-download:
```bash
rm dataset_v3.db
python datasetviewer.py
```


## Acknowledgments

- Dataset: [lodestones/e621-captions](https://huggingface.co/datasets/lodestones/e621-captions)
- Images: [e621.net](https://e621.net) - Please follow their API guidelines
- UI Framework: [DearPyGui](https://github.com/hoffstadt/DearPyGui)


---

**Note**: This tool is for research and personal use. Please respect e621's Terms of Service and API rate limits.
