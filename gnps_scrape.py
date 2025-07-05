import time
import json
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

def init_driver(headless=True):
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(options=options)

def get_study_links(scientific_term):
    logging.info(f"Launching browser to apply species filter: '{scientific_term}'")
    driver = init_driver()
    driver.get("https://massive.ucsd.edu/ProteoSAFe/massive-quant-datasets.jsp")

    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "main_quant_analysis_input"))
        )
        driver.find_element(By.ID, "main_quant_analysis_input").clear()
        logging.info("Cleared quant_analysis_input field.")

        sb = driver.find_element(By.ID, "main_species_resolved_input")
        sb.clear()
        sb.send_keys(scientific_term)
        logging.info(f"Typed species: {scientific_term}")

        driver.find_element(By.ID, "main.filter").click()
        logging.info("Clicked Apply Filters.")
        time.sleep(10)

        soup = BeautifulSoup(driver.page_source, "html.parser")
        rows = soup.find_all("tr", id=lambda x: x and x.startswith("table[main]_row["))
        logging.info(f"Found {len(rows)} table rows.")

        entries = []
        for row in rows:
            col0 = row.find("td", id=lambda x: x and x.endswith("_column[0]"))
            if not col0:
                continue

            link_tag = col0.find("a", href=True)
            if not link_tag or "dataset.jsp?task=" not in link_tag["href"]:
                continue

            study_id = link_tag.text.strip()
            # ← use the current page URL as base so "./dataset.jsp" resolves into "/ProteoSAFe/dataset.jsp"
            task_url = urljoin(driver.current_url, link_tag["href"])
            task_id  = link_tag["href"].split("task=")[-1]
            title_txt = col0.get_text(strip=True).split(study_id)[0].strip()

            entries.append({
                "species": scientific_term,
                "url":      task_url,
                "dataset_id": task_id,
                "study_id":   study_id,
                "title":      title_txt,
                "description": "",
                "principal_investigators": "",
                "size":       "",
                "num_files":  "",
                "spectra":    "",
                "ftp_link":   "",
                "decision":   "",
                "reason":     ""
            })

        logging.info(f"Found {len(entries)} datasets for species '{scientific_term}'.")
        driver.quit()
        return entries

    except Exception as e:
        logging.error(f"Error during filter and scrape: {e}")
        with open("failed_page_dump.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        driver.quit()
        return []

def scrape_dataset_page(driver, entry, species_term=None):
    logging.info(f"Processing entry {entry['study_id']}")
    driver.get(entry["url"])

    # wait for any dataset-block to appear
    WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CLASS_NAME, "dataset-block"))
    )
    time.sleep(5)  # let JS finish

    soup = BeautifulSoup(driver.page_source, "html.parser")

    def debug_element(desc, el):
        if el:
            logging.info(f"DEBUG: {desc}: <{el.name}> '{el.get_text(strip=True)[:30]}…'")
        else:
            logging.info(f"DEBUG: {desc} not found")

    data = {}  # temp container for the block logic

    # ------------------
    # Description Logic
    # ------------------
    try:
        description_div = soup.find('div', class_='dataset-block')
        debug_element("Dataset block div", description_div)

        if description_div:
            description_h2 = description_div.find(
                'h2',
                string=lambda s: 'Description' in s if s else False
            )
            debug_element("Description h2", description_h2)

            if description_h2:
                # Use <p> inside the same block as the description
                p = description_div.find('p')
                debug_element("Description paragraph", p)

                if p:
                    data["description"] = p.get_text(strip=True)
                else:
                    data["description"] = "<p> not found in description block"
            else:
                data["description"] = "<h2>Description</h2> not found"
        else:
            data["description"] = "Description section not found"

    except Exception as e:
        logging.warning(f"Error getting description: {e}")
        data["description"] = f"Error retrieving description: {e}"

    entry["description"] = data["description"]

    # --------------------------------------
    # Principal Investigators Logic
    # --------------------------------------
    try:
        principal_text = soup.find(
            string=lambda text: text and "Principal Investigators:" in text
        )
        debug_element("PI text", principal_text)

        if principal_text:
            parent_td = principal_text
            while parent_td and parent_td.name != 'td':
                parent_td = parent_td.parent
            debug_element("PI parent td", parent_td)

            if parent_td:
                value_td = parent_td.find_next_sibling('td', class_='value')
                debug_element("PI value td", value_td)
                data["principal_investigators"] = value_td.get_text(strip=True) if value_td else "PI value cell not found"
            else:
                data["principal_investigators"] = "PI parent <td> not found"
        else:
            # fallback
            tr_with_pi = soup.find('tr', id='0')
            debug_element("PI fallback tr#0", tr_with_pi)
            if tr_with_pi:
                value_td = tr_with_pi.find('td', class_='value')
                debug_element("PI fallback value td", value_td)
                data["principal_investigators"] = value_td.get_text(strip=True) if value_td else "PI cell not in tr#0"
            else:
                data["principal_investigators"] = "PI row not found"

    except Exception as e:
        logging.warning(f"Error getting principal investigators: {e}")
        data["principal_investigators"] = f"Error retrieving PI: {e}"

    entry["principal_investigators"] = data["principal_investigators"]

    # -------------------------
    # Summary fields (table)
    # -------------------------
    fc = soup.find("td", {"id": "filecount"})
    entry["num_files"] = fc.text.strip() if fc else ""
    sz = soup.find("td", {"id": "filesize"})
    entry["size"] = sz.text.strip() if sz else ""
    sp = soup.find("td", {"id": "spectra"})
    entry["spectra"] = sp.text.strip() if sp else ""
    ftp = soup.find("input", {"id": "ftpLink"})
    entry["ftp_link"] = ftp.get("value", "").strip() if ftp else ""

    # leave these blank
    entry["decision"] = ""
    entry["reason"] = ""

    return entry

def run_scraper(scientific_term):
    output_file = f"data/{scientific_term.lower()}_gnps_datasets.json"

    entries = get_study_links(scientific_term)
    if not entries:
        logging.warning("No datasets found. Exiting.")
        return

    driver = init_driver()
    all_data = []
    for ent in entries:
        try:
            all_data.append(scrape_dataset_page(driver, ent))
        except Exception as e:
            logging.error(f"Failed on {ent['url']}: {e}")
    driver.quit()

    with open(output_file, "w") as f:
        json.dump(all_data, f, indent=2)
    logging.info(f"Saved {len(all_data)} entries to {output_file}")

if __name__ == "__main__":
    run_scraper("gallus")