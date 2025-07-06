import logging
import json
import argparse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# attributes to extract from the study pages
attrs = set()
attrs.add("study_id")
attrs.add("study_title")
attrs.add("study_summary")
attrs.add("raw_data_available")
attrs.add("raw_data_file_type(s)")
attrs.add("analysis_type_detail")
attrs.add("project_title")
attrs.add("project_summary")
attrs.add("subject_type")
attrs.add("subject_species")
attrs.add("species_group")
attrs.add("collection_summary")
attrs.add("sample_type")

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

def init_driver(headless=True):
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(options=options)

# Function to get study links based on a scientific term
def get_study_links(scientific_term):
    logging.info(f"Launching browser to apply species filter: '{scientific_term}'")
    driver = init_driver()
    url = f"https://www.metabolomicsworkbench.org/data/metadata_search2.php?Q={scientific_term}"
    logging.info(f"Navigating to URL: {url}")
    driver.get(url)
    
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "content"))
        )
        logging.info("Search results loaded.")

        soup = BeautifulSoup(driver.page_source, "html.parser")
        rows = soup.find_all("tr")

        entries = []
        for row in rows:
            link_tag = row.find("a", href=True)
            if not link_tag:
                continue
            baseUrl = "https://www.metabolomicsworkbench.org/data/"
            study_link = baseUrl + link_tag["href"]
            entries.append(study_link)

        logging.info(f"Found {len(entries)} study links.")
        return entries
    
    except Exception as e:
        logging.error(f"Error while processing the page: {e}")
        driver.quit()
        return []


def process_studies(entries):

    process_studies = []

    for entry in entries:
        logging.info(f"Processing study link: {entry}")
        driver = init_driver()
        driver.get(entry)
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.ID, "content"))
            )
            soup = BeautifulSoup(driver.page_source, "html.parser")

            download_link = soup.find("a", href=True, string=lambda x: x and 
                        ("download data files" in x.lower() or "contains raw data" in x.lower()))
            
            is_source_available = False

            if download_link:
                is_source_available = True
                logging.info(f"Found download link: {download_link['href']}")

            if not is_source_available:
                logging.warning(f"No download link found for study: {entry}")
                continue

            rows = soup.find_all("tr")
            print(f"Found {len(rows)} rows in the study page.")
            
            study = {}
            study["download_link"] = download_link["href"]

            for row in rows:
                cols = row.find_all("td")
                
                if len(cols) < 2:
                    continue
                

                key = cols[0].get_text(separator=" ", strip=True).lower()
                value = cols[1].get_text(separator=" ", strip=True)
                key = key.replace(" ", "_").replace(":", "")

                if key in attrs:
                    study[key] = value
            
            logging.info(f"Processed study data: {study}")
            process_studies.append(study)        

        except Exception as e:
            logging.error(f"Error processing study {entry}: {e}")


    logging.info("All studies processed.")
    driver.quit()    
    return process_studies


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape metabolomics studies based on a scientific term.")
    parser.add_argument("scientific_term", type=str, help="The scientific term to search for metabolomics studies.")
    args = parser.parse_args()
    if not args.scientific_term:
        logging.error("No scientific term provided. Please use the --term argument to specify a term.")
        exit(1)
    scientific_term = args.scientific_term.strip()
    study_links = get_study_links(scientific_term)
    if study_links:
        processed_studies = process_studies(study_links)
        for study in processed_studies:
            logging.info(f"Study ID: {study['study_id']}, Title: {study['study_title']}")
    else:
        logging.info("No studies found for the given scientific term.")
    filepath = f"{scientific_term}_metabolomics_workbench.json"
    with open(filepath, "w") as f:
        json.dump(processed_studies, f, indent=4)

