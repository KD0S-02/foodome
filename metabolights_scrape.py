import requests
import json

def fetch_metabolomics_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad responses
        return response.text
    except requests.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return None
    
if __name__ == "__main__":
    studies_metadata = {}
    url = "https://www.ebi.ac.uk:443/metabolights/ws/studies"
    data = fetch_metabolomics_data(url)

    decoded_data = json.decoder.JSONDecoder().raw_decode(data)[0]

    study_ids = decoded_data["content"]

    processed_studies = []

    for id in study_ids:
        study_link = f"https://www.ebi.ac.uk:443/metabolights/ws/v1/study/{id}"
        res = fetch_metabolomics_data(study_link)
        decoded_response = json.decoder.JSONDecoder().raw_decode(res)[0]
        study_details = decoded_response["content"]
        processed_studies.append(study_details)

    with open("./data/metabolights_all_studies.json", "w") as f:
        json.dump(processed_studies, f, indent=4)