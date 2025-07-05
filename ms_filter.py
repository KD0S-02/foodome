#!/usr/bin/env python3
import json
import os
import subprocess
import time
from urllib.parse import urlparse, unquote
import tempfile
from concurrent.futures import ProcessPoolExecutor, as_completed
import platform
import re
import logging
import glob

# ==============================================================================
# --- Configuration & Constants ---
# ==============================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(process)d - %(levelname)s - %(message)s')

DATA_DIR = "data"
MAX_FILES_FOR_MS2_COUNTING = 10
ENABLE_MSCONVERT_PROCESSING = False # <<< KEY FLAG FOR THIS CHANGE >>>
MSCONVERT_DOCKER_IMAGE = "chambm/pwiz-skyline-i-agree-to-the-vendor-licenses"
SUPPORTED_EXTENSIONS_LOWER = ['.mzxml', '.mgf', '.mzml', '.cdf']
MSCONVERT_SUPPORTED_RAW_EXTENSIONS = ['.raw', '.wiff', '.d'] # Files requiring msconvert
TEXT_BASED_EXTENSIONS = ['.mzxml', '.mgf', '.mzml', '.cdf'] # .cdf can sometimes be text or binary, treating as text-parseable here for simplicity if grep works
DOCKER_AVAILABLE = None

# ==============================================================================
# --- Generic Keyword Definitions for Filename Filtering ---
# ==============================================================================
RAW_TERMS = [r"\braw\b", r"unprocessed", r"fresh", r"uncooked", r"natural", r"crude", r"whole"]
CONTROL_TERMS = [
    r"control", r"ctrl", r"cntrl", r"ctl", r"uninfected", r"untreated", r"baseline",
    r"wild_type", r"wt", r"wildtype", r"parental", r"initial", r"input",
    r"t0\b", r"time0", r"day0", r"0h\b", r"0hr\b", r"zerohr", r"0_hr",
    r"pre_treatment", r"before_processing", r"pre[-]processing", r"before[-]processing",
    r"mock"
]
GENERIC_EDIBLE_PART_TERMS = [
    r"grain", r"kernel", r"seed", r"berry", r"endosperm", r"embryo", r"germ", r"cereal",
    r"fruit", r"pulp", r"flesh", r"peel", r"skin",
    r"vegetable", r"leafy_green", r"root_veg", r"tuber_veg",
    r"edible_part", r"sample"
]
PROCESSED_TERMS = [
    r"beer", r"ale", r"lager", r"brew", r"wort", r"malt", r"ferment", r"sourdough",
    r"bread", r"pasta", r"noodle", r"cake", r"biscuit", r"cookie", r"pastry",
    r"syrup", r"starch", r"ethanol", r"biofuel", r"distillate", r"mash", r"slurry",
    r"cook", r"bake", "baked", r"fried", r"toast", r"roast", r"steam", r"boil", r"autoclave",
    r"extract", r"digest", r"hydroly[sz](?:ate|ed)", r"supernatant", r"pellet",
    r"flour", r"meal", r"grit", r"semolina", r"paste", r"puree", r"juice", r"smoothie",
    r"extrude", r"puff", r"flake", r"instant", "processed"
]
NON_EDIBLE_PLANT_PARTS = [
    r"leaf", r"leaves", r"foliage", r"stem", r"stalk", r"shoot", r"stover", r"culm",
    r"root", r"rhizome",
    r"seedling", r"sprout",
    r"husk", r"hull", r"bran", r"chaff", r"glume", r"lemma", r"palea",
    r"tassel", r"silk", r"anther", r"pollen", r"flower",
    r"callus", r"cell_culture", r"suspension_culture", r"plant_tissue"
]
TREATMENT_PATHOGEN_TERMS = [
    r"infect", r"pathogen", r"disease", r"lesion", r"symptom",
    r"fungi", r"bacteria", r"virus", r"oomycete", r"nematode",
    r"treat", r"treatment", r"stress", r"elicitor", r"induc",
    r"pesticide", r"herbicide", "fungicide", "insecticide", r"fertilizer",
    r"mutant", r"transgenic", r"gmo", r"knockout", r"overexpress"
]
NON_SAMPLE_QC_TERMS = [
    r"qc", r"quality_control", r"qa", r"system_suitability", r"sst",
    r"blank", r"solvent_blank", r"method_blank", r"instrument_blank", r"reagent_blank",
    r"wash", r"equilibration", r"conditioning", r"gradient_test",
    r"standard", r"std", r"ref_mat", r"reference_material", r"calib",
    r"tune", r"mass_cal", r"msms_check", r"tryptic_digest_std"
]

def compile_regex_list(terms):
    return [re.compile(term, re.IGNORECASE) for term in terms]

COMPILED_RAW = compile_regex_list(RAW_TERMS)
COMPILED_CONTROL = compile_regex_list(CONTROL_TERMS)
COMPILED_GENERIC_EDIBLE_PART = compile_regex_list(GENERIC_EDIBLE_PART_TERMS)
COMPILED_PROCESSED = compile_regex_list(PROCESSED_TERMS)
COMPILED_NON_EDIBLE = compile_regex_list(NON_EDIBLE_PLANT_PARTS)
COMPILED_TREATMENT = compile_regex_list(TREATMENT_PATHOGEN_TERMS)
COMPILED_QC = compile_regex_list(NON_SAMPLE_QC_TERMS)

# ==============================================================================
# --- Helper Functions (FTP, MS2 counting, Docker - as before) ---
# ==============================================================================
def parse_ftp_path(ftp_link):
    parsed = urlparse(ftp_link); return parsed.netloc, parsed.path

def list_mass_spec_files_recursively(server, base_ftp_path):
    found_files = []
    pid_str = f"[{os.getpid()}]"
    logging.info(f"{pid_str} Recursively searching for mass spec files in ftp://{server}{base_ftp_path}...")
    if not base_ftp_path.endswith('/'): base_ftp_path += '/'
    lftp_cmd = f"lftp -e 'set ftp:ssl-allow no; find {base_ftp_path}; bye' {server}"
    try:
        process = subprocess.run(lftp_cmd, shell=True, capture_output=True, text=True, timeout=600)
    except subprocess.TimeoutExpired:
        logging.error(f"{pid_str} lftp command timed out for ftp://{server}{base_ftp_path}")
        return []
    if process.returncode != 0:
        if "lftp: command not found" in process.stderr:
             logging.critical(f"{pid_str} 'lftp' command not found. Please install lftp.")
        else:
            logging.error(f"{pid_str} lftp 'find' failed for ftp://{server}{base_ftp_path}. Stderr: {process.stderr.strip()}")
        return []

    lines = process.stdout.strip().split('\n')
    for line in lines:
        full_ftp_path = line.strip()
        if not full_ftp_path or full_ftp_path.endswith('/'): continue
        filename = os.path.basename(full_ftp_path)
        _ , file_ext = os.path.splitext(filename)
        if file_ext.lower() in SUPPORTED_EXTENSIONS_LOWER:
            found_files.append({'ftp_full_path': full_ftp_path, 'filename': filename, 'file_ext_lower': file_ext.lower()})
    
    if not found_files:
        logging.warning(f"{pid_str} No supported mass spec files found recursively under ftp://{server}{base_ftp_path}")
    else:
        logging.info(f"{pid_str} Found {len(found_files)} supported mass spec files in ftp://{server}{base_ftp_path}.")
    return found_files

def count_ms2_spectra_in_text_file_via_ftp(server, full_ftp_file_path, file_ext_lower):
    display_file_name = os.path.basename(full_ftp_file_path)
    pid_str = f"[{os.getpid()}]"
    logging.info(f"{pid_str} Counting MS2 (FTP stream): {display_file_name} ({file_ext_lower})")
    count_cmd = None
    # .cdf can be tricky, assuming it's text-like for grep if it reaches here
    if file_ext_lower == '.mzxml': count_cmd = f"curl --ftp-pasv -s 'ftp://{server}{full_ftp_file_path}' | grep -c 'msLevel=\"2\"'"
    elif file_ext_lower == '.mgf': count_cmd = f"curl --ftp-pasv -s 'ftp://{server}{full_ftp_file_path}' | grep -c 'BEGIN IONS'"
    elif file_ext_lower == '.mzml': count_cmd = f"curl --ftp-pasv -s 'ftp://{server}{full_ftp_file_path}' | grep -c 'accession=\"MS:1000580\"'"
    elif file_ext_lower == '.cdf': # Attempt for NetCDF, might not always work if binary MS2 markers
        count_cmd = f"curl --ftp-pasv -s 'ftp://{server}{full_ftp_file_path}' | strings | grep -Ec 'scan_type=MSMS|msLevel=2'" # Example, may need refinement
    else: logging.warning(f"{pid_str} Text MS2 count logic not defined for {file_ext_lower}"); return 0
    
    start_time = time.time()
    try:
        process = subprocess.run(count_cmd, shell=True, capture_output=True, text=True, timeout=3600)
    except subprocess.TimeoutExpired: logging.error(f"{pid_str} curl|grep timed out for {display_file_name}"); return 0
    
    if process.returncode > 1: logging.error(f"{pid_str} curl|grep failed for {display_file_name}. Stderr: {process.stderr.strip()}"); return 0
    try:
        count = int(process.stdout.strip())
        logging.info(f"{pid_str} Found {count} MS2 spectra in {display_file_name} (streamed in {time.time() - start_time:.2f}s)")
        return count
    except ValueError: logging.error(f"{pid_str} Could not parse count for {display_file_name}. Output: '{process.stdout.strip()}'"); return 0

def count_ms2_spectra_in_converted_file(local_mzml_path):
    display_file_name = os.path.basename(local_mzml_path)
    pid_str = f"[{os.getpid()}]"
    logging.info(f"{pid_str} Counting MS2 in CONVERTED local: {display_file_name}")
    count_cmd = f"grep -c 'accession=\"MS:1000580\"' \"{local_mzml_path}\""
    start_time = time.time()
    try:
        process = subprocess.run(count_cmd, shell=True, capture_output=True, text=True, timeout=1800)
    except subprocess.TimeoutExpired: logging.error(f"{pid_str} grep timed out for {display_file_name}"); return 0
    if process.returncode > 1: logging.error(f"{pid_str} grep failed for {display_file_name}. Stderr: {process.stderr.strip()}"); return 0
    try:
        count = int(process.stdout.strip())
        logging.info(f"{pid_str} Found {count} MS2 spectra in {display_file_name} (local grep {time.time() - start_time:.2f}s)")
        return count
    except ValueError: logging.error(f"{pid_str} Could not parse count from converted {display_file_name}. Output: '{process.stdout.strip()}'"); return 0

def check_docker_availability():
    global DOCKER_AVAILABLE
    if DOCKER_AVAILABLE is not None: return DOCKER_AVAILABLE
    pid_str = f"[{os.getpid()}]"; logging.info(f"{pid_str} Checking Docker availability...")
    try:
        subprocess.run("docker --version", shell=True, check=True, capture_output=True, text=True, timeout=10)
        subprocess.run("docker ps -q", shell=True, check=True, capture_output=True, text=True, timeout=15)
        logging.info(f"{pid_str} Docker appears available and running.")
        DOCKER_AVAILABLE = True
    except Exception as e:
        logging.warning(f"{pid_str} Docker check failed ({type(e).__name__}). msconvert via Docker unavailable. Details: {e}")
        DOCKER_AVAILABLE = False
    return DOCKER_AVAILABLE

def process_binary_file_with_msconvert(server, full_ftp_file_path):
    pid_str = f"[{os.getpid()}]"
    display_file_name = os.path.basename(full_ftp_file_path)
    # This function is only called if ENABLE_MSCONVERT_PROCESSING is True (checked before calling)
    # and Docker is available (also checked before calling).
    logging.info(f"{pid_str} Processing BINARY via Docker: {display_file_name}")
    with tempfile.TemporaryDirectory(prefix="msconvert_") as tmpdir:
        local_raw_path = os.path.join(tmpdir, display_file_name)
        mzml_basename = os.path.splitext(display_file_name)[0] + ".mzML"
        local_mzml_path = os.path.join(tmpdir, mzml_basename)
        download_cmd = f"curl --ftp-pasv --create-dirs -o \"{local_raw_path}\" 'ftp://{server}{full_ftp_file_path}'"
        try:
            subprocess.run(download_cmd, shell=True, check=True, capture_output=True, text=True, timeout=7200)
        except Exception as e: logging.error(f"{pid_str} Download failed for {display_file_name}: {e}"); return 0
        
        msconvert_filters = '--filter "msLevel 2-"'
        msconvert_args = f"msconvert.exe \"/data/{display_file_name}\" --mzML --outfile \"/data/{mzml_basename}\" -o \"/data\" {msconvert_filters}"
        docker_cmd = f"docker run --rm -v \"{tmpdir}\":/data {MSCONVERT_DOCKER_IMAGE} wine {msconvert_args}"
        try:
            subprocess.run(docker_cmd, shell=True, check=True, capture_output=True, text=True, timeout=7200)
        except Exception as e: logging.error(f"{pid_str} Docker msconvert failed for {display_file_name}: {e}"); return 0
        
        if os.path.exists(local_mzml_path): return count_ms2_spectra_in_converted_file(local_mzml_path)
        else: logging.error(f"{pid_str} Converted file {mzml_basename} not found in {tmpdir}."); return 0

# ==============================================================================
# --- Filename Heuristic Filtering ---
# ==============================================================================
def check_filename_relevance(filename_lower, derived_food_name_regex_list, assessment_type):
    if any(rgx.search(filename_lower) for rgx in COMPILED_QC):
        return False, "qc_or_blank_file"

    if assessment_type == "ACCEPTED":
        return True, "accepted_study_non_qc_file"

    elif assessment_type == "MAYBE":
        food_match = any(rgx.search(filename_lower) for rgx in derived_food_name_regex_list)
        
        if any(rgx.search(filename_lower) for rgx in COMPILED_PROCESSED):
            if not (any(rgx.search(filename_lower) for rgx in COMPILED_CONTROL) or \
                    any(rgx.search(filename_lower) for rgx in COMPILED_RAW)):
                return False, "maybe_processed_product_no_raw_control_signal"
                
        if any(rgx.search(filename_lower) for rgx in COMPILED_NON_EDIBLE):
            if not (any(rgx.search(filename_lower) for rgx in COMPILED_GENERIC_EDIBLE_PART) or \
                    any(rgx.search(filename_lower) for rgx in COMPILED_CONTROL)):
                return False, "maybe_non_edible_part_no_edible_control_signal"

        has_treatment_term = any(rgx.search(filename_lower) for rgx in COMPILED_TREATMENT)
        has_control_term = any(rgx.search(filename_lower) for rgx in COMPILED_CONTROL)
        has_raw_term = any(rgx.search(filename_lower) for rgx in COMPILED_RAW)
        has_generic_edible_term = any(rgx.search(filename_lower) for rgx in COMPILED_GENERIC_EDIBLE_PART)

        if food_match:
            if has_raw_term or has_control_term or has_generic_edible_term:
                return True, "maybe_food_match_with_raw_control_or_edible_term"
        
        if has_raw_term or has_control_term:
            if has_treatment_term and not (has_raw_term or has_control_term):
                 return False, "maybe_treatment_file_no_food_match_lacks_explicit_control_raw"
            return True, "maybe_no_food_match_but_strong_raw_control_signal"

        if has_treatment_term and not (has_control_term or has_raw_term or has_generic_edible_term or food_match):
            return False, "maybe_treatment_file_lacks_any_positive_signal"
            
        return False, "maybe_lacks_sufficient_positive_signal"
    
    return False, "unknown_assessment_type_or_unmatched_condition_in_heuristic"

# ==============================================================================
# --- Main Dataset Processing Logic ---
# ==============================================================================
def get_derived_food_name_regex_list(food_item_key_str):
    if not food_item_key_str: return []
    parts = [re.escape(part) for part in food_item_key_str.split('_') if len(part) > 2]
    full_key_escaped = re.escape(food_item_key_str)
    regex_strings = [r"\b" + part + r"\b" for part in parts]
    if full_key_escaped not in regex_strings:
         regex_strings.append(r"\b" + full_key_escaped + r"\b")
    unique_regex_strings = sorted(list(set(regex_strings)))
    return [re.compile(rs, re.IGNORECASE) for rs in unique_regex_strings]

def process_single_dataset(dataset_info_item, food_item_key):
    study_id = dataset_info_item.get('study_id', 'UNKNOWN_ID')
    llm_assessment = dataset_info_item.get('llm_assessment', 'REJECTED')
    pid_str = f"[{os.getpid()}:{study_id}]"
    logging.info(f"{pid_str} Processing dataset. LLM Assessment: {llm_assessment}")

    dataset_info_item['selected_files_for_ms2_analysis'] = []
    dataset_info_item['total_ms2_spectra_from_selected_files'] = 0
    dataset_info_item['unsupported_files_skipped_count'] = 0 # New field
    
    if llm_assessment == "REJECTED":
        dataset_info_item['ms2_count_source_type'] = 'rejected_by_llm'
        logging.info(f"{pid_str} Study REJECTED by LLM. Skipping MS2 analysis.")
        return dataset_info_item

    ftp_link = dataset_info_item.get('ftp_link', '')
    if not ftp_link or not ftp_link.startswith('ftp://'):
        dataset_info_item['ms2_count_source_type'] = 'no_ftp_link'
        logging.warning(f"{pid_str} No valid FTP link. Skipping MS2 analysis.")
        return dataset_info_item

    derived_food_name_regex = get_derived_food_name_regex_list(food_item_key)
    logging.info(f"{pid_str} Using derived food name regex patterns: {[r.pattern for r in derived_food_name_regex]} for {llm_assessment} study.")

    server, base_ftp_path = parse_ftp_path(ftp_link)
    all_ftp_files_info = list_mass_spec_files_recursively(server, base_ftp_path)

    if not all_ftp_files_info:
        dataset_info_item['ms2_count_source_type'] = f'ftp_no_files_found_{llm_assessment.lower()}'
        logging.warning(f"{pid_str} No files found via FTP. Skipping MS2 analysis.")
        return dataset_info_item

    candidate_files_for_ms2 = []
    unsupported_skipped_this_study = 0

    for file_info in all_ftp_files_info:
        filename_lower = file_info['filename'].lower()
        is_relevant, reason = check_filename_relevance(filename_lower, derived_food_name_regex, llm_assessment)
        
        if is_relevant:
            # File passed heuristics, now check if it's processable without msconvert
            is_msconvert_required = file_info['file_ext_lower'] in MSCONVERT_SUPPORTED_RAW_EXTENSIONS
            
            if is_msconvert_required and not ENABLE_MSCONVERT_PROCESSING:
                unsupported_skipped_this_study += 1
                logging.info(f"{pid_str} File '{file_info['filename']}' ({file_info['file_ext_lower']}) is RELEVANT ({reason}) but requires msconvert (disabled). Skipping MS2 count.")
                # Optionally, still add it to candidate_files_for_ms2 with a note and 0 spectra,
                # if you want to see it in `selected_files_for_ms2_analysis`.
                # For now, we just count it as skipped and don't add to MS2 processing list.
            else:
                candidate_files_for_ms2.append({
                    'ftp_file_path': file_info['ftp_full_path'],
                    'filename': file_info['filename'],
                    'file_ext_lower': file_info['file_ext_lower'],
                    'heuristic_match_reason': reason,
                    'ms2_spectra_in_file': 0 # Will be updated
                })
                logging.info(f"{pid_str} File '{file_info['filename']}' PASSED heuristic ({reason}) and is processable for {llm_assessment} study.")
        else:
            logging.debug(f"{pid_str} File '{file_info['filename']}' FAILED heuristic ({reason}) for {llm_assessment} study.")
    
    dataset_info_item['unsupported_files_skipped_count'] = unsupported_skipped_this_study

    if not candidate_files_for_ms2: # No files that are both relevant AND processable
        dataset_info_item['ms2_count_source_type'] = f'no_processable_files_passed_heuristics_{llm_assessment.lower()}'
        logging.info(f"{pid_str} No processable files passed heuristic filter for {llm_assessment} study.")
        return dataset_info_item

    logging.info(f"{pid_str} Found {len(candidate_files_for_ms2)} processable candidate files after heuristic filtering for {llm_assessment} study.")
    
    files_to_process_for_ms2 = candidate_files_for_ms2
    if len(candidate_files_for_ms2) > MAX_FILES_FOR_MS2_COUNTING:
        logging.warning(f"{pid_str} Heuristics selected {len(candidate_files_for_ms2)} processable files. Limiting to {MAX_FILES_FOR_MS2_COUNTING} for MS2 counting.")
        files_to_process_for_ms2 = sorted(
            candidate_files_for_ms2, 
            key=lambda x: (0 if 'control' in x['heuristic_match_reason'].lower() or 'raw' in x['heuristic_match_reason'].lower() else 1)
        )[:MAX_FILES_FOR_MS2_COUNTING]

    total_ms2_spectra_for_study = 0
    processed_file_details = []

    for selected_file_info in files_to_process_for_ms2:
        ftp_path = selected_file_info['ftp_file_path']
        file_ext = selected_file_info['file_ext_lower']
        ms2_count_this_file = 0

        if file_ext in TEXT_BASED_EXTENSIONS: # mzxml, mgf, mzml, cdf (if cdf is text-like)
            ms2_count_this_file = count_ms2_spectra_in_text_file_via_ftp(server, ftp_path, file_ext)
        elif file_ext in MSCONVERT_SUPPORTED_RAW_EXTENSIONS:
            # This block should only be reached if ENABLE_MSCONVERT_PROCESSING is True,
            # because otherwise they are filtered out before reaching candidate_files_for_ms2.
            # However, double-checking here is safe.
            if ENABLE_MSCONVERT_PROCESSING and check_docker_availability():
                ms2_count_this_file = process_binary_file_with_msconvert(server, ftp_path)
            else:
                logging.warning(f"{pid_str} Attempting to process binary {selected_file_info['filename']} but msconvert disabled/Docker unavailable. Should have been caught earlier. Count will be 0.")
        else:
            logging.warning(f"{pid_str} File {selected_file_info['filename']} has unsupported extension '{file_ext}' for direct counting. Count will be 0.")

        selected_file_info['ms2_spectra_in_file'] = ms2_count_this_file
        total_ms2_spectra_for_study += ms2_count_this_file
        processed_file_details.append(selected_file_info)

    dataset_info_item['selected_files_for_ms2_analysis'] = processed_file_details
    dataset_info_item['total_ms2_spectra_from_selected_files'] = total_ms2_spectra_for_study
    dataset_info_item['ms2_count_source_type'] = f'ftp_analysis_{llm_assessment.lower()}_heuristic_filtered'
    
    logging.info(f"{pid_str} Finished MS2 analysis. Total MS2 from selected files: {total_ms2_spectra_for_study}. Skipped {unsupported_skipped_this_study} msconvert-only files.")
    return dataset_info_item

def process_dataset_file(input_filepath, output_filepath, food_item_key, max_workers=1):
    logging.info(f"Starting to process dataset file: {input_filepath} for food key: {food_item_key}")
    try:
        with open(input_filepath, 'r') as f:
            data_from_json = json.load(f)
    except Exception as e:
        logging.error(f"Failed to load or parse JSON from {input_filepath}: {e}")
        return

    if not isinstance(data_from_json, list):
        logging.error(f"Expected a list of datasets in {input_filepath}, found {type(data_from_json)}. Skipping.")
        return

    datasets_to_process_list = data_from_json
    final_processed_datasets = []
    actual_max_workers = min(max_workers, len(datasets_to_process_list)) if len(datasets_to_process_list) > 0 else 1

    with ProcessPoolExecutor(max_workers=actual_max_workers) as executor:
        futures = {
            executor.submit(process_single_dataset, dataset_item, food_item_key): dataset_item
            for dataset_item in datasets_to_process_list
        }
        for i, future in enumerate(as_completed(futures)):
            original_dataset = futures[future]
            study_id_log = original_dataset.get('study_id', 'UNKNOWN_ON_COMPLETION')
            try:
                processed_dataset = future.result()
                final_processed_datasets.append(processed_dataset)
                logging.info(f"({i+1}/{len(datasets_to_process_list)}) Completed processing for study: {study_id_log}")
            except Exception as e:
                logging.error(f"Error processing study {study_id_log} in worker: {e}", exc_info=True)
                original_dataset.update({
                    'selected_files_for_ms2_analysis': [],
                    'total_ms2_spectra_from_selected_files': 0,
                    'unsupported_files_skipped_count': original_dataset.get('unsupported_files_skipped_count', 'Error occurred before count'),
                    'ms2_count_source_type': 'processing_error',
                    'processing_error_message': str(e)
                })
                final_processed_datasets.append(original_dataset)
    
    final_processed_datasets.sort(key=lambda x: x.get('study_id', ''))
    output_data_content = final_processed_datasets

    try:
        os.makedirs(os.path.dirname(output_filepath), exist_ok=True)
        with open(output_filepath, 'w') as f:
            json.dump(output_data_content, f, indent=2)
        logging.info(f"Successfully saved final processed data to {output_filepath}")
    except Exception as e:
        logging.error(f"Failed to write final processed data to {output_filepath}: {e}")

# ==============================================================================
# --- Script Entry Point ---
# ==============================================================================
if __name__ == "__main__":
    script_start_time = time.time()
    num_cpu = os.cpu_count()
    num_workers = max(1, min(num_cpu // 2 if num_cpu else 1, 4)) 
    logging.info(f"Using {num_workers} worker processes for parallel dataset processing.")
    logging.info(f"msconvert processing for raw/wiff/d files is currently {'ENABLED' if ENABLE_MSCONVERT_PROCESSING else 'DISABLED'}.")
    if not ENABLE_MSCONVERT_PROCESSING:
         logging.info("  Files requiring msconvert (e.g., .raw, .wiff, .d) will be counted as 'unsupported_files_skipped_count' and not processed for MS2 spectra.")
    logging.info(f"Max files per dataset for MS2 counting (after heuristics/QC filter): {MAX_FILES_FOR_MS2_COUNTING}")
    logging.info("-" * 80)

    input_file_pattern = os.path.join(DATA_DIR, "decided_*_gnps_datasets.json")
    input_files = glob.glob(input_file_pattern)

    if not input_files:
        logging.warning(f"No 'decided_*.json' files found in '{DATA_DIR}'. Exiting.")
        exit(0)
    
    logging.info(f"Found {len(input_files)} 'decided_*.json' files to process:")
    for f_path in input_files: logging.info(f"  - {os.path.basename(f_path)}")

    for input_filepath in input_files:
        input_basename = os.path.basename(input_filepath)
        match = re.match(r"decided_([a-zA-Z0-9_.-]+)_gnps_datasets.json", input_basename)
        if match:
            food_item_key = match.group(1)
            output_filename = f"final_{food_item_key}_gnps_datasets.json"
            output_filepath = os.path.join(DATA_DIR, output_filename)
            logging.info(f"\n>>> Processing {input_basename} (Food Item Key: {food_item_key}) -> {output_filename} <<<")
            process_dataset_file(input_filepath, output_filepath, food_item_key, max_workers=num_workers)
        else:
            logging.warning(f"Could not parse food item key from filename: {input_basename}. Skipping.")

    script_end_time = time.time()
    logging.info(f"\nScript finished in {script_end_time - script_start_time:.2f} seconds.")