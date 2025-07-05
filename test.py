import json
import os
import glob
import logging

# --- Configuration ---
DATA_FOLDER = 'data'  # Folder containing the JSON files to process
KEYS_TO_REMOVE = ["decision", "reason"] # Keys to remove from dictionaries

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def remove_keys_from_dict(data_dict, keys_to_remove):
    """
    Removes specified keys from a single dictionary.
    """
    if not isinstance(data_dict, dict):
        return data_dict # Not a dictionary, do nothing

    keys_found_and_removed = []
    for key in keys_to_remove:
        if key in data_dict:
            del data_dict[key]
            keys_found_and_removed.append(key)
    
    # Recursively process values that are dictionaries or lists
    for key, value in data_dict.items():
        if isinstance(value, dict):
            data_dict[key] = remove_keys_from_dict(value, keys_to_remove)
        elif isinstance(value, list):
            data_dict[key] = remove_keys_from_list(value, keys_to_remove)
            
    return data_dict, keys_found_and_removed

def remove_keys_from_list(data_list, keys_to_remove):
    """
    Iterates through a list, applying key removal to dictionary elements
    and recursively processing nested lists.
    """
    if not isinstance(data_list, list):
        return data_list # Not a list, do nothing

    processed_list = []
    any_keys_removed_in_list = False
    for item in data_list:
        if isinstance(item, dict):
            processed_item, keys_removed_from_item = remove_keys_from_dict(item, keys_to_remove)
            if keys_removed_from_item:
                any_keys_removed_in_list = True
            processed_list.append(processed_item)
        elif isinstance(item, list):
            # Recursively process nested lists
            processed_sublist, _ = remove_keys_from_list(item, keys_to_remove) # Ignoring keys_removed flag from sublist for simplicity here
            processed_list.append(processed_sublist)
        else:
            processed_list.append(item) # Non-dict, non-list item, keep as is
    return processed_list, any_keys_removed_in_list


def process_json_file(filepath, keys_to_remove):
    """
    Loads a JSON file, removes specified keys, and saves it back.
    """
    logging.info(f"Processing file: {filepath}")
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            # Handle potentially empty files
            if not content.strip():
                logging.warning(f"File {filepath} is empty or contains only whitespace. Skipping.")
                return
            data = json.loads(content)
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON from {filepath}: {e}. Skipping this file.")
        return
    except Exception as e:
        logging.error(f"Error reading file {filepath}: {e}. Skipping this file.")
        return

    original_data_type = type(data)
    keys_removed_overall = False

    if isinstance(data, list):
        modified_data, keys_removed_in_list = remove_keys_from_list(data, keys_to_remove)
        if keys_removed_in_list:
            keys_removed_overall = True
    elif isinstance(data, dict):
        modified_data, keys_removed_from_dict = remove_keys_from_dict(data, keys_to_remove)
        if keys_removed_from_dict:
            keys_removed_overall = True
    else:
        logging.warning(f"File {filepath} does not contain a list or dict at the root. Skipping key removal for this file.")
        return # No changes to save

    if keys_removed_overall:
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(modified_data, f, indent=2) # Save with indent for readability
            logging.info(f"Successfully removed keys {keys_to_remove} from {filepath} and saved changes.")
        except Exception as e:
            logging.error(f"Error writing modified data to {filepath}: {e}")
    else:
        logging.info(f"No keys {keys_to_remove} found to remove in {filepath}. File unchanged.")


# --- Main Script Execution ---
if __name__ == "__main__":
    logging.info(f"Starting key removal process in folder: {DATA_FOLDER}")
    logging.info(f"Keys to remove: {KEYS_TO_REMOVE}")
    
    if not os.path.isdir(DATA_FOLDER):
        logging.error(f"Data folder '{DATA_FOLDER}' not found. Exiting.")
        exit(1)

    # Find all .json files recursively in the DATA_FOLDER
    # For your current structure, this will just find files in 'data/'
    # If you had 'data/subdir/file.json', it would also find those.
    json_files_to_process = glob.glob(os.path.join(DATA_FOLDER, '**', '*.json'), recursive=True)

    if not json_files_to_process:
        logging.info(f"No .json files found in '{DATA_FOLDER}'. Nothing to do.")
        exit(0)

    logging.info(f"Found {len(json_files_to_process)} JSON files to check:")
    for f_path in json_files_to_process:
        logging.info(f"  - {f_path}")
    
    # Confirmation step
    # proceed = input(f"This will modify {len(json_files_to_process)} files in place. "
    #                 "Ensure you have a backup. Proceed? (yes/no): ")
    # if proceed.lower() != 'yes':
    #     logging.info("Operation cancelled by user.")
    #     exit(0)

    for filepath in json_files_to_process:
        process_json_file(filepath, KEYS_TO_REMOVE)

    logging.info("Key removal process finished.")