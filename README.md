# Food Metabolomics Data Pipeline

This project provides a pipeline to scrape, filter, and analyze Mass Spectrometry (MS/MS) datasets from the Global Natural Products Social Molecular Networking (GNPS) MassIVE repository, MetabolomicsWorkbench and Metabolights, with a focus on identifying relevant data from raw, unprocessed food items.

## Pipeline Overview

The pipeline consists of several Python scripts that are run sequentially:

1.  **`gnps_scrape.py`**: Scrapes dataset metadata from GNPS MassIVE based on scientific organism names.
2.  **`metabolomics_scrape.py`**: Scrapes study context from MetabolomicsWorkbench based on a given search term.
3.  **`metabolights_scrape.py`**: Similar to 2, but scrapes from Metabolights based on scientfic organism names.
4.  **`decision.py`**: Uses an OpenAI LLM (GPT model) to assess each scraped study and categorize its potential to contain raw food MS/MS data as "ACCEPTED", "MAYBE", or "REJECTED".
5.  **`ms_filter.py`**: Processes the LLM-assessed studies.
    *   "REJECTED" studies are skipped.
    *   "ACCEPTED" studies have their files (excluding common QC files) processed for MS2 spectra.
    *   "MAYBE" studies undergo a detailed filename-based heuristic keyword filter to identify potentially relevant raw food files, which are then processed for MS2 spectra.
    *   It counts MS2 spectra in text-based MS files (mzML, mzXML, MGF) and can optionally use `msconvert` via Docker for proprietary formats like `.raw`, `.wiff`, `.d` (currently disabled by default).
4.  **`summary.py`**: Aggregates the results from all processed food items and generates a summary Excel file with overall analytics, per-food analytics, and detailed study data.

## File Structure

```
.
├── data/                      # Directory for all input, intermediate, and final JSON data
│   ├── oryza_gnps_datasets.json         # Raw scraped data for Oryza
│   ├── decided_oryza_gnps_datasets.json # Scraped data + LLM decisions for Oryza
│   ├── final_oryza_gnps_datasets.json   # LLM decisions + MS2 counts for Oryza
│   ├── ... (similar files for triticum, zea, etc.)
├── .env                        # Stores API keys (e.g., OPENAI_API_KEY) - NOT COMMITTED
├── gnps_scrape.py              # Script 1: Scrapes GNPS
├── metabolomics_scrape.py      # Script 2: Scrapes MetabolomicsWorkbench
├── metabolights_scrape.py      # Script 3: Scrapes Metabolights
├── decision.py                 # Script 4: LLM-based study categorization
├── ms_filter.py                # Script 5: File filtering and MS2 counting
├── summary.py                  # Script 6: Generates final Excel summary
├── README.md                   # This file
└── requirements.txt            # (Recommended) Python package dependencies
```

## Setup Instructions

### 1. Create a Virtual Environment

It's highly recommended to use a virtual environment to manage project dependencies.

```bash
# Navigate to the project root directory (e.g., foodome)
cd /path/to/your/foodome

# Create a virtual environment (e.g., named 'venv')
python3 -m venv venv

# Activate the virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
# venv\Scripts\activate
```
You should see `(venv)` at the beginning of your terminal prompt.

### 2. Install Dependencies

Create a `requirements.txt` file in your project root with the following content:

```
pandas
openpyxl # For writing Excel files
python-dotenv
openai
requests # (gnps_scrape.py might need it, or selenium if it's more complex)
beautifulsoup4 # For scrapers if using BeautifulSoup
selenium # For scrapers if using Selenium
lftp # System dependency, not a Python package (see below)
```

Then install these packages:
```bash
pip install -r requirements.txt
```

**System Dependencies:**
*   **`lftp`**: The `ms_filter.py` script uses `lftp` for recursively listing files from FTP servers.
*   **macOS (using Homebrew):** `brew install lftp`
*   **Debian/Ubuntu:** `sudo apt-get update && sudo apt-get install lftp`
*   **Other Linux:** Use your system's package manager.
*   **Docker (Optional, for `.raw`/`.wiff`/`.d` processing):**
*   If you plan to enable `msconvert` for processing proprietary raw files, you need Docker Desktop installed and running.
*   Pull the required ProteoWizard image:
    ```bash
    docker pull chambm/pwiz-skyline-i-agree-to-the-vendor-licenses
    ```

### 3. Configure Environment Variables (`.env` file)

This pipeline uses an OpenAI API key for the `decision.py` script.

1.  Create a file named `.env` in the project root directory (`gnps2/.env`).
2.  Add your OpenAI API key to this file:
    ```env
    OPENAI_API_KEY="your_openai_api_key_here"
    ```
    Replace `"your_openai_api_key_here"` with your actual API key.
3.  **Important:** Add `.env` to your `.gitignore` file to prevent committing your API key to version control. If you don't have a `.gitignore` file, create one in the project root and add `.env` as a line.

## Running the Pipeline

Run the scripts in the following order. Ensure your virtual environment is activated.

### Step 1: `gnps_scrape.py` - Scrape GNPS Data

*   **Purpose:** Fetches initial dataset metadata from GNPS MassIVE for specified organisms.
*   **How to modify:** Edit the `run_scraper("ORGANISM_NAME")` call within the `if __name__ == "__main__":` block of `gnps_scrape.py` to specify the scientific name (e.g., "Zea", "Oryza sativa", "Triticum aestivum"). You might need to run this multiple times for different organisms.
*   **Input:** Scientific organism name.
*   **Output:** Creates JSON files in the `data/` directory, e.g., `data/zea_gnps_datasets.json`, `data/oryza_gnps_datasets.json`. Each file contains a list of dataset metadata dictionaries.
*   **Command (Example for "Zea"):**
    ```bash
    # Ensure gnps_scrape.py is configured to scrape "Zea"
    python3 gnps_scrape.py
    ```

### Step 2: `metabolomics_scrape.py` - Scrape MetabolomicsWorkbench Data

*   **Purpose:** Fetches study metadata from MetabolomicsWorkbench for a given scientific term.
*   **How to modify:** Run the script with the desired search term as a command-line argument.
*   **Input:** Scientific term (e.g., "Zea", "Oryza sativa").
*   **Output:** Creates a JSON file in the project root, e.g., `zea_metabolomics_workbench.json`, containing a list of study metadata dictionaries.
*   **Command (Example for "Zea"):**
    ```bash
    python3 metabolomics_scrape.py Zea
    ```


### Step 3: `metabolights_scrape.py` - Scrape Metabolights Data

*   **Purpose:** Fetches study metadata from Metabolights for a given scientific term.
*   **How to modify:** Run the script with the desired search term as a command-line argument.
*   **Input:** Scientific term (e.g., "Zea", "Oryza sativa").
*   **Output:** Creates a JSON file in the project root, e.g., `zea_metabolomics_workbench.json`, containing a list of study metadata dictionaries.
*   **Command (Example for "Zea"):**
    ```bash
    python3 metabolights_scrape.py Zea
    ```

### Step 3: `decision.py` - LLM-Based Study Categorization

*   **Purpose:** Reads the raw scraped JSON files, sends metadata for each study to an OpenAI LLM, and categorizes each study as "ACCEPTED", "MAYBE", or "REJECTED" based on its potential to contain raw food MS/MS data.
*   **Input:** `data/{food_item_key}_gnps_datasets.json` files (e.g., `data/zea_gnps_datasets.json`).
*   **Output:** Creates new JSON files in the `data/` directory, prepended with "decided_", e.g., `data/decided_zea_gnps_datasets.json`. These files contain the original metadata plus `llm_assessment` and `llm_reason` fields for each study.
*   **Command:**
    ```bash
    python3 decision.py
    ```
    (The script automatically finds `*_gnps_datasets.json` files in `data/` that don't start with "decided_" or "filtered_").

### Step 4: `ms_filter.py` - File Filtering and MS2 Counting

*   **Purpose:** Processes the "decided" JSON files.
    *   Skips "REJECTED" studies.
    *   For "ACCEPTED" studies, it processes (almost) all non-QC files from FTP for MS2 spectra.
    *   For "MAYBE" studies, it performs a detailed heuristic keyword search on filenames from FTP to select relevant raw food files before MS2 counting.
    *   Counts MS2 spectra from selected, processable files. Text-based files (`.mzML`, `.mzXML`, `.mgF`, `.cdf`) are processed by streaming and `grep`. Proprietary files (`.raw`, `.wiff`, `.d`) are processed via `msconvert` in Docker *only if* `ENABLE_MSCONVERT_PROCESSING` is set to `True` in the script and Docker is set up.
*   **Input:** `data/decided_{food_item_key}_gnps_datasets.json` files.
*   **Output:** Creates new JSON files in the `data/` directory, prepended with "final_", e.g., `data/final_zea_gnps_datasets.json`. These files include:
    *   `selected_files_for_ms2_analysis`: A list of files that were actually processed for MS2 counts, including their individual MS2 counts and the heuristic reason for selection.
    *   `total_ms2_spectra_from_selected_files`: The sum of MS2 spectra from the selected files for that study.
    *   `unsupported_files_skipped_count`: Count of files relevant by heuristics but skipped because `ENABLE_MSCONVERT_PROCESSING` was `False`.
    *   `ms2_count_source_type`: Describes how the MS2 count was obtained or why it's zero.
*   **Configuration:**
    *   `ENABLE_MSCONVERT_PROCESSING` (boolean at the top of `ms_filter.py`): Set to `True` to enable processing of `.raw`, `.wiff`, `.d` files. Requires Docker and the pwiz image.
    *   `MAX_FILES_FOR_MS2_COUNTING` (integer at the top of `ms_filter.py`): Limits how many files are processed for MS2 spectra per study after heuristic selection/QC filtering.
*   **Command:**
    ```bash
    python3 ms_filter.py
    ```

### Step 4: `summary.py` - Generate Final Excel Summary

*   **Purpose:** Reads all `final_*.json` files, aggregates the data, calculates overall and per-food item analytics, and outputs a comprehensive summary.
*   **Input:** `data/final_{food_item_key}_gnps_datasets.json` files.
*   **Output:** Creates a timestamped Excel (`.xlsx`) file in the project root directory (e.g., `gnps_pipeline_summary_YYYYMMDD_HHMMSS.xlsx`). The Excel file contains three sheets:
    1.  `Overall Analytics`: High-level statistics for the entire pipeline run.
    2.  `Per Food Analytics`: Statistics broken down by each processed food item key.
    3.  `Detailed Study Data`: A table with one row per study, showing key metadata, LLM assessment, and final MS2 counts.
*   **Command:**
    ```bash
    python3 summary.py
    ```

## Script Details

### `gnps_scrape.py`
*   **Input:** Scientific name of the organism to search on GNPS MassIVE.
*   **Output:** `data/{organism_name_key}_gnps_datasets.json`
    *   A JSON list of dictionaries, where each dictionary contains metadata for a dataset (e.g., `study_id`, `title`, `description`, `ftp_link`, `species`).

### `decision.py`
*   **Input:** `data/{food_item_key}_gnps_datasets.json`
*   **Output:** `data/decided_{food_item_key}_gnps_datasets.json`
    *   Same structure as input, but each dataset dictionary is augmented with:
        *   `llm_assessment`: "ACCEPTED", "MAYBE", or "REJECTED"
        *   `llm_reason`: Justification from the LLM.

### `metabolomics_scrape.py`
*   **Input:** Scientific term to search on MetabolomicsWorkbench.
*   **Output:** `{scientific_term}_metabolomics_workbench.json`
    *   A JSON list of dictionaries, each containing metadata for a study (e.g., `study_id`, `study_title`, `download_link`, and other extracted attributes).

### `ms_filter.py`
*   **Input:** `data/decided_{food_item_key}_gnps_datasets.json`
*   **Output:** `data/final_{food_item_key}_gnps_datasets.json`
    *   Same structure as input, but each dataset dictionary is further augmented with:
        *   `selected_files_for_ms2_analysis`: List of dicts, each detailing a file processed for MS2 spectra (filename, path, heuristic reason, MS2 count).
        *   `total_ms2_spectra_from_selected_files`: Integer sum of MS2 spectra.
        *   `unsupported_files_skipped_count`: Integer count of files skipped due to requiring disabled msconvert.
        *   `ms2_count_source_type`: String indicating how MS2 processing was handled (e.g., "rejected_by_llm", "ftp_analysis_maybe_heuristic_filtered").

### `summary.py`
*   **Input:** All `data/final_{food_item_key}_gnps_datasets.json` files.
*   **Output:** `gnps_pipeline_summary_{timestamp}.xlsx` in the project root.
    *   Contains sheets for overall analytics, per-food analytics, and detailed study-by-study data.

## Notes
*   The keyword lists within `ms_filter.py` for heuristic filename filtering are crucial and may need ongoing refinement based on observed results for different food types.
*   Processing can be time-consuming, especially the `lftp` calls and MS2 counting for large datasets.
*   Ensure your OpenAI API key has sufficient quota if processing a large number of studies with `decision.py`.