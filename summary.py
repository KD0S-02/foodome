#!/usr/bin/env python3
import json
import os
import glob
import pandas as pd
from datetime import datetime
import logging
import re # Added for extract_food_item_key_from_filename

# --- Configuration ---
DATA_DIR = "data"
OUTPUT_FILENAME_BASE = "gnps_pipeline_summary"
# Define the exact columns and order for the main data table
OUTPUT_COLUMNS_STUDIES = [
    'Food Item Key',
    'Study ID',
    'Title',
    'Species (Original)',
    'LLM Assessment',
    'LLM Reason',
    'MS2 Spectra (Selected Files)',
    'MS2 Count Source',
    'Files Processed for MS2',
    'Unsupported Files Skipped (msconvert off)',
    'Original Num Files (Metadata)',
    'Original Spectra (Metadata)',
    'Dataset URL'
]

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_food_item_key_from_filename(filename):
    """Extracts 'food_item' from 'final_food_item_gnps_datasets.json'"""
    base = os.path.basename(filename)
    match = re.match(r"final_([a-zA-Z0-9_.-]+)_gnps_datasets.json", base)
    if match:
        return match.group(1)
    return "unknown_food_item"

def process_final_json_file(filepath):
    logging.info(f"Processing summary for file: {filepath}")
    food_item_key = extract_food_item_key_from_filename(filepath)
    
    extracted_studies = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            datasets = json.load(f) 
    except Exception as e:
        logging.error(f"Error reading or parsing JSON from {filepath}: {e}")
        return []

    if not isinstance(datasets, list):
        logging.warning(f"File {filepath} does not contain a list at the root. Skipping.")
        return []

    for study_data in datasets:
        if not isinstance(study_data, dict):
            logging.warning(f"Skipping non-dictionary item in {filepath}: {study_data}")
            continue

        num_files_processed_for_ms2 = 0
        if isinstance(study_data.get('selected_files_for_ms2_analysis'), list):
            num_files_processed_for_ms2 = len(study_data.get('selected_files_for_ms2_analysis'))

        record = {
            'Food Item Key': food_item_key,
            'Study ID': study_data.get('study_id', 'N/A'),
            'Title': study_data.get('title', 'N/A'),
            'Species (Original)': study_data.get('species', 'N/A'),
            'LLM Assessment': study_data.get('llm_assessment', 'N/A'),
            'LLM Reason': study_data.get('llm_reason', 'N/A'),
            'MS2 Spectra (Selected Files)': study_data.get('total_ms2_spectra_from_selected_files', 0),
            'MS2 Count Source': study_data.get('ms2_count_source_type', 'N/A'),
            'Files Processed for MS2': num_files_processed_for_ms2,
            'Unsupported Files Skipped (msconvert off)': study_data.get('unsupported_files_skipped_count', 0),
            'Original Num Files (Metadata)': study_data.get('num_files', 'N/A'),
            'Original Spectra (Metadata)': study_data.get('spectra', 'N/A'),
            'Dataset URL': study_data.get('url', 'N/A')
        }
        extracted_studies.append(record)
        
    logging.info(f"Extracted {len(extracted_studies)} studies from {filepath}")
    return extracted_studies

def generate_analytics(all_studies_df):
    if all_studies_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    analytics_overall = {
        "Total Studies Processed": len(all_studies_df),
        "Total ACCEPTED by LLM": len(all_studies_df[all_studies_df['LLM Assessment'] == 'ACCEPTED']),
        "Total MAYBE by LLM": len(all_studies_df[all_studies_df['LLM Assessment'] == 'MAYBE']),
        "Total REJECTED by LLM": len(all_studies_df[all_studies_df['LLM Assessment'] == 'REJECTED']),
        "Studies with >0 Useful MS2 Spectra": len(all_studies_df[all_studies_df['MS2 Spectra (Selected Files)'] > 0]),
        "Total Useful MS2 Spectra Found": all_studies_df['MS2 Spectra (Selected Files)'].sum(),
        "Total Unsupported Files Skipped (msconvert off)": all_studies_df['Unsupported Files Skipped (msconvert off)'].sum(),
    }
    useful_spectra_studies = all_studies_df[all_studies_df['MS2 Spectra (Selected Files)'] > 0]
    analytics_overall["Avg Useful MS2 Spectra (for studies with >0)"] = useful_spectra_studies['MS2 Spectra (Selected Files)'].mean() if not useful_spectra_studies.empty else 0
    
    overall_analytics_df = pd.DataFrame([analytics_overall]).T
    overall_analytics_df.columns = ["Value"]

    analytics_by_food = []
    for food_key, group in all_studies_df.groupby('Food Item Key'):
        useful_spectra_group = group[group['MS2 Spectra (Selected Files)'] > 0]
        avg_useful_spectra = useful_spectra_group['MS2 Spectra (Selected Files)'].mean() if not useful_spectra_group.empty else 0
        food_analytics = {
            "Food Item Key": food_key,
            "Total Studies": len(group),
            "ACCEPTED by LLM": len(group[group['LLM Assessment'] == 'ACCEPTED']),
            "MAYBE by LLM": len(group[group['LLM Assessment'] == 'MAYBE']),
            "REJECTED by LLM": len(group[group['LLM Assessment'] == 'REJECTED']),
            "Studies with >0 Useful MS2": len(useful_spectra_group),
            "Total Useful MS2 Spectra": group['MS2 Spectra (Selected Files)'].sum(),
            "Total Unsupported Skipped": group['Unsupported Files Skipped (msconvert off)'].sum(),
            "Avg Useful MS2 Spectra (>0)": avg_useful_spectra,
        }
        analytics_by_food.append(food_analytics)
        
    per_food_analytics_df = pd.DataFrame(analytics_by_food)
    if not per_food_analytics_df.empty:
        per_food_analytics_df = per_food_analytics_df.set_index("Food Item Key")

    return overall_analytics_df, per_food_analytics_df

# --- Main Script Execution ---
if __name__ == "__main__":
    logging.info(f"Starting summary generation from '{DATA_DIR}' directory...")
    
    input_file_pattern = os.path.join(DATA_DIR, "final_*_gnps_datasets.json")
    final_json_files = glob.glob(input_file_pattern)

    if not final_json_files:
        logging.warning(f"No 'final_*.json' files found in '{DATA_DIR}'. No summary will be generated.")
        exit(0)

    logging.info(f"Found {len(final_json_files)} 'final_*.json' files to process:")
    for f_path in final_json_files: logging.info(f"  - {os.path.basename(f_path)}")

    all_studies_data = []
    for filepath in final_json_files:
        studies_from_file = process_final_json_file(filepath)
        all_studies_data.extend(studies_from_file)

    if not all_studies_data:
        logging.warning("No study data extracted from any file. Exiting.")
        exit(0)

    all_studies_df = pd.DataFrame(all_studies_data)
    all_studies_df = all_studies_df.reindex(columns=OUTPUT_COLUMNS_STUDIES)

    overall_analytics_df, per_food_analytics_df = generate_analytics(all_studies_df.copy())

    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_excel_filename = f"{OUTPUT_FILENAME_BASE}_{timestamp_str}.xlsx"
    
    logging.info(f"Preparing to write summary to Excel: {output_excel_filename}")

    try:
        with pd.ExcelWriter(output_excel_filename, engine='openpyxl') as writer:
            if not overall_analytics_df.empty:
                overall_analytics_df.to_excel(writer, sheet_name='Overall Analytics', index=True, header=True)
            else:
                pd.DataFrame(["No overall analytics to display."]).to_excel(writer, sheet_name='Overall Analytics', index=False, header=False)

            if not per_food_analytics_df.empty:
                per_food_analytics_df.to_excel(writer, sheet_name='Per Food Analytics', index=True, header=True)
            else:
                pd.DataFrame(["No per-food item analytics to display."]).to_excel(writer, sheet_name='Per Food Analytics', index=False, header=False)
            
            if not all_studies_df.empty:
                all_studies_df.to_excel(writer, sheet_name='Detailed Study Data', index=False, header=True)
            else:
                pd.DataFrame(["No detailed study data to display."]).to_excel(writer, sheet_name='Detailed Study Data', index=False, header=False)
                
        logging.info(f"Summary Excel file saved successfully: {os.path.abspath(output_excel_filename)}")

    except Exception as e:
        logging.error(f"ERROR: Failed to save data to {output_excel_filename}. Error: {e}")
        logging.error("Make sure you have 'openpyxl' installed: pip install openpyxl")


    logging.info("Summary generation finished.")