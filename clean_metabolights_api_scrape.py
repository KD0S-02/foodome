import json

studies = []

MS2_RELEVANT_KEYS = {
    "title",
    "studyDescription",      
    "description",           
    "descriptors",
    "assays",
    "protocols",
    "sampleTable",
    "organism",
}

with open("./data/metabolights_all_studies.json", "r") as f:
    studies = json.load(f)

processed_studies = []

print(f"Total Studies : {len(studies)}")

for study in studies:
    processed_study = {}
    for key, value in study.items():
        if key in MS2_RELEVANT_KEYS:
            processed_study[key] = value
    processed_studies.append(processed_study)

with open("./data/clean_metabolights_all_studies.json", "w") as file:
    json.dump(processed_studies, file, indent=4)