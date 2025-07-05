import json
import time
import logging
from openai import OpenAI
from dotenv import load_dotenv
import os
import glob # For finding files

load_dotenv()
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    logging.error("OPENAI_API_KEY not found in .env file or environment variables.")
client = OpenAI(api_key=api_key)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_prompt(entry):
    return f"""
You are an expert bioinformatician specializing in food metabolomics. You are reviewing dataset metadata from the MassIVE repository.
Your critical task is to categorize this study's potential to contain Mass Spectrometry (MS/MS) data files derived DIRECTLY from the RAW, UNPROCESSED, EDIBLE portion of the target organism/species mentioned in the metadata below. Your assessment should be conservative: if there's a reasonable possibility that raw material was analyzed, even as a minor component or unstated control within a larger study on processed products or other biological aspects, lean towards "MAYBE" rather than "REJECTED".

Dataset Details:
----------------------------------
Title: {entry.get("title", "N/A")}
Species: {entry.get("species", "N/A")}
Study ID: {entry.get("study_id", "N/A")}
Description: {entry.get("description", "N/A")}
Principal Investigators: {entry.get("principal_investigators", "N/A")}
File Count: {entry.get("num_files", "N/A")}
Spectra Count: {entry.get("spectra", "N/A")}
Size: {entry.get("size", "N/A")}
FTP Link: {entry.get("ftp_link", "N/A")}
----------------------------------

Categorization Guidelines (focus on the stated 'Species'):

1.  **ACCEPTED:** Assign this category ONLY if:
    *   The study's *primary and explicit focus* is unequivocally on the analysis of the raw, unprocessed, edible part of the stated Species.
    *   The description *clearly and unambiguously states* the inclusion and separate analysis of raw material from the stated Species as controls or baseline, and this analysis is a significant, described part of the study.
    *   Example: "Metabolomic profiling of raw wheat grains" or "Comparison of raw and cooked rice metabolomes where raw rice analysis is detailed."

2.  **MAYBE:** Assign this category if:
    *   The study primarily focuses on processed versions of the stated Species (e.g., derived food products like beer, bread, fermented items), or on non-edible parts, or on biological interactions/treatments.
    *   HOWEVER, the stated Species (raw material) is mentioned as an ingredient, starting material, or host, AND there is *any plausible reason to suspect* that the raw, unprocessed, edible form *might* have been analyzed as a control, baseline, time-zero point, or reference, even if this is not explicitly detailed or is a minor part of the overall study.
    *   The description doesn't explicitly rule out the analysis of the raw edible form.
    *   Keywords suggesting processing (e.g., "fermentation", "brewing", "cooking") are present, but the raw ingredients are also named.
    *   The study involves comparing treated vs. untreated samples of the stated Species, implying an unprocessed control *could* exist.
    *   The number of files is large, and the description is broad, allowing for the possibility of unmentioned control/raw samples.
    *   Use this category if you are uncertain but cannot confidently reject.

3.  **REJECTED:** Assign this category ONLY if:
    *   It is *highly improbable or explicitly clear* that raw, unprocessed, edible material from the stated Species was analyzed.
    *   The study *exclusively* focuses on heavily processed products with *no mention or plausible implication* of analyzing the raw starting material (e.g., "Analysis of commercial beer aroma compounds" with no reference to grains).
    *   The study *exclusively* focuses on non-edible parts of the stated Species (e.g., "Transcriptomics of corn leaves only") with no indication of edible part analysis.
    *   The study is purely genomic/transcriptomic without any hint of metabolomic/MS analysis of the raw, edible part.
    *   The stated 'Species' is clearly used only as a growth medium or in a context entirely unrelated to its raw, edible form analysis (e.g., "Yeast grown on rice husk hydrolysate," where rice husk is not the target raw edible food).
    *   The description is so specific to a non-raw context that including raw material analysis would be illogical for the study's stated aims.

Based on this evaluation, respond STRICTLY in the following two-line format:

Overall Assessment: [ACCEPTED / MAYBE / REJECTED]
Reason: [Provide a concise justification. For ACCEPTED, highlight explicit confirmation. For MAYBE, state why raw data *might* exist despite the main focus (e.g., "Focuses on beer, but raw grains (rice) are listed as ingredients; raw controls *could* be present among many files."). For REJECTED, state why raw data is highly unlikely or explicitly ruled out.]
"""

def analyze_and_categorize_dataset(entry, retry_delay=10, max_retries=3):
    prompt_text = generate_prompt(entry)
    attempt = 0
    study_id_log = entry.get('study_id', 'UNKNOWN_ID')

    while attempt < max_retries:
        try:
            logging.debug(f"LLM Attempt {attempt + 1}/{max_retries} for Study ID: {study_id_log}.")
            response = client.chat.completions.create(
                model="gpt-4",
                messages=[{"role": "user", "content": prompt_text}],
                temperature=0.2
            )
            content = response.choices[0].message.content.strip()
            logging.debug(f"LLM Raw Response for {study_id_log}: {content}")

            lines = content.splitlines()
            assessment = "REJECTED"
            reason = "LLM response parsing failed or no reason provided."

            if lines:
                first_line_lower = lines[0].lower()
                if "overall assessment:" in first_line_lower:
                    val = first_line_lower.split("overall assessment:")[-1].strip().upper()
                    if val in ["ACCEPTED", "MAYBE", "REJECTED"]:
                        assessment = val
                    else:
                        logging.warning(f"Unexpected assessment value '{val}' from LLM for {study_id_log}. Defaulting to REJECTED.")
                        reason = f"LLM returned unexpected assessment value: {val}. Original response: {content}"
                else:
                    if "ACCEPTED" in lines[0].upper(): assessment = "ACCEPTED"
                    elif "MAYBE" in lines[0].upper(): assessment = "MAYBE"
                    elif "REJECTED" in lines[0].upper(): assessment = "REJECTED"
                    else:
                        logging.warning(f"Could not parse 'Overall Assessment:' keyword from first line for {study_id_log}. Defaulting to REJECTED.")
                        reason = f"Could not parse 'Overall Assessment:' keyword. Original response: {content}"
                
                if len(lines) > 1:
                    reason_line_lower = lines[1].lower()
                    if "reason:" in reason_line_lower:
                        reason = lines[1].split("reason:")[-1].strip()
                    else:
                        reason = lines[1].strip()
                        if not reason: reason = "No specific reason provided by LLM on second line."
                elif assessment != "REJECTED":
                    reason = "LLM provided assessment but no separate reason line."

            entry["llm_assessment"] = assessment
            entry["llm_reason"] = reason
            logging.info(f"Study ID: {study_id_log} - LLM Assessment: {assessment} - Reason: {reason[:100]}...")
            return entry
        except Exception as e:
            attempt += 1
            logging.error(f"OpenAI API error for study {study_id_log}: {e}. Attempt {attempt}/{max_retries}.")
            if attempt < max_retries:
                logging.info(f"Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
                retry_delay += 5
            else:
                logging.error(f"Max retries reached for study {study_id_log}. Marking as REJECTED due to API error.")
                entry["llm_assessment"] = "REJECTED"
                entry["llm_reason"] = f"Failed after {max_retries} API attempts. Last error: {str(e)}"
                return entry
    entry["llm_assessment"] = "REJECTED"
    entry["llm_reason"] = "Max retries reached or unexpected error loop exit."
    return entry

def run_llm_assessment_on_file(input_filepath, output_filepath):
    """
    Loads entries from an input JSON file, gets LLM decisions, and saves to a new output file.
    """
    if not os.path.exists(input_filepath):
        logging.error(f"Input JSON file not found: {input_filepath}")
        return

    logging.info(f"Processing JSON file for LLM assessment: {input_filepath}")

    try:
        with open(input_filepath, "r") as f:
            entries = json.load(f)
    except Exception as e:
        logging.error(f"Failed to read or decode JSON from {input_filepath}: {e}")
        return

    if not isinstance(entries, list):
        logging.error(f"Expected a list of entries in {input_filepath}, found {type(entries)}. Skipping.")
        return

    updated_entries = []
    total_entries = len(entries)
    for i, entry in enumerate(entries):
        if not isinstance(entry, dict):
            logging.warning(f"Skipping non-dictionary item at index {i} in {input_filepath}")
            updated_entries.append(entry)
            continue
        
        # Optional: Skip if already processed in the input file (if re-running on already processed files)
        # This check is less relevant if we always write to a new output file and start from original inputs.
        # if "llm_assessment" in entry and output_filepath == input_filepath: # only if updating in place
        #     logging.info(f"Skipping entry {i+1}/{total_entries} ({entry.get('study_id', 'N/A')}): Already has llm_assessment.")
        #     updated_entries.append(entry)
        #     continue

        logging.info(f"Processing entry {i+1}/{total_entries}: {entry.get('study_id', 'UNKNOWN_ID')}")
        updated_entry = analyze_and_categorize_dataset(entry)
        updated_entries.append(updated_entry)

        # Optional: Sleep to manage API rate limits, especially for large files
        if (i + 1) % 20 == 0 and i + 1 < total_entries:
            logging.info(f"Processed {i+1} entries from {os.path.basename(input_filepath)}. Pausing briefly...")
            time.sleep(5)

    try:
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_filepath), exist_ok=True)
        with open(output_filepath, "w") as f:
            json.dump(updated_entries, f, indent=2)
        logging.info(f"Successfully saved LLM assessments for {os.path.basename(input_filepath)} to {output_filepath} ({len(updated_entries)} entries).")
    except Exception as e:
        logging.error(f"Failed to write LLM assessments to {output_filepath}: {e}")

if __name__ == "__main__":
    if not api_key: # Check again in main guard
        logging.critical("CRITICAL: OPENAI_API_KEY environment variable not set. Please set it in a .env file or system environment. Exiting.")
        exit(1)

    DATA_DIR = "data"
    # Pattern to find input files, excluding 'example' and 'decided_' or 'filtered_' prefixed files
    # This pattern assumes food item names don't start with 'decided_' or 'filtered_'
    input_file_pattern = os.path.join(DATA_DIR, "*_gnps_datasets.json")
    
    all_input_files = glob.glob(input_file_pattern)
    
    # Filter out files that are already outputs of this script or later stages, or examples
    files_to_process = [
        f for f in all_input_files 
        if not os.path.basename(f).startswith("decided_") and \
           not os.path.basename(f).startswith("filtered_") and \
           not os.path.basename(f).startswith("example_") # Or handle example differently
    ]
    
    if not files_to_process:
        logging.warning(f"No input files found matching pattern '{input_file_pattern}' in '{DATA_DIR}' or all were filtered out. (Excluding 'decided_*', 'filtered_*', 'example_*')")
        # You might want to process 'example_gnps_datasets.json' if it exists and no other files are found
        example_file = os.path.join(DATA_DIR, "example_gnps_datasets.json")
        if os.path.exists(example_file):
            logging.info(f"Found example file: {example_file}. Processing it as a fallback.")
            files_to_process.append(example_file)
        else:
            exit(0)


    logging.info(f"Found {len(files_to_process)} dataset JSON files to process in '{DATA_DIR}':")
    for f_path in files_to_process:
        logging.info(f"  - {os.path.basename(f_path)}")

    logging.info("Starting LLM decision process for all found files...")
    script_start_time = time.time()

    for input_filepath in files_to_process:
        base_name = os.path.basename(input_filepath)
        # Construct output filename: data/decided_{original_filename_part}_gnps_datasets.json
        if base_name == "example_gnps_datasets.json": # Special handling for example output name
            output_filename = "decided_example_gnps_datasets.json"
        else:
            # Assumes format {food_item}_gnps_datasets.json
            output_filename = f"decided_{base_name}"

        output_filepath = os.path.join(DATA_DIR, output_filename)
        
        logging.info(f"\n--- Processing: {input_filepath} -> {output_filepath} ---")
        run_llm_assessment_on_file(input_filepath, output_filepath)

    script_end_time = time.time()
    logging.info(f"\nLLM decision script finished for all files in {script_end_time - script_start_time:.2f} seconds.")