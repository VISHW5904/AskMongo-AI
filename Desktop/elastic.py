from elasticsearch import Elasticsearch # type: ignore
from elasticsearch.exceptions import NotFoundError
import warnings
import requests
import json # For pretty-printing JSON

# Suppress InsecureRequestWarning for local development with self-signed certs
requests.urllib3.disable_warnings(requests.urllib3.exceptions.InsecureRequestWarning)

# Connect to Elasticsearch
es = Elasticsearch(
    "https://localhost:9208",
    basic_auth=("elastic", "E!plSaahaj@1310"),
    verify_certs=False  # For localhost with self-signed cert; use ca_certs in production
)

if not es.ping():
    raise ValueError("Connection to Elasticsearch failed!")
print("Successfully connected to Elasticsearch.")

# 1. Get all indices
try:
    all_indices_info = es.indices.get_alias(index="*")
except Exception as e:
    print(f"Error getting indices: {e}")
    exit()

saahaj_indices = [index_name for index_name in all_indices_info if index_name.startswith("saahaj_")]

if not saahaj_indices:
    print("No indices found starting with 'saahaj_'. Exiting.")
    exit()

output_filename = "vk.txt"
definitions_for_file = [] # Store all definitions before writing

print(f"Will write index definitions to: {output_filename}")

for index_name in saahaj_indices:
    print(f"Processing index: {index_name}")

    try:
        # 2. Get mapping and settings
        mapping_data = es.indices.get_mapping(index=index_name)
        raw_mappings_from_api = mapping_data[index_name]['mappings']

        # Adjust mapping: remove "_doc" type wrapper if present
        # This logic is from our previous successful iteration
        if "_doc" in raw_mappings_from_api:
            mapping_for_definition = raw_mappings_from_api["_doc"]
            print(f"  Adjusted mapping: Using content of '_doc' type.")
        else:
            mapping_for_definition = raw_mappings_from_api
            print(f"  Mapping: Using as-is (presumed typeless or empty).")

        settings_data = es.indices.get_settings(index=index_name)
        original_settings = settings_data[index_name]['settings']['index']

        # 3. Clean up settings (only include what's relevant for creation)
        clean_settings = {
            "number_of_shards": int(original_settings.get('number_of_shards', 1)),
            "number_of_replicas": int(original_settings.get('number_of_replicas', 0))
        }
        if 'analysis' in original_settings:
            clean_settings['analysis'] = original_settings['analysis']
        if 'refresh_interval' in original_settings:
            clean_settings['refresh_interval'] = original_settings['refresh_interval']
        # Add any other settings you want to carry over

        # 4. Prepare the definition for the file
        new_index_name = index_name.replace("saahaj_", "vishw_", 1)

        index_body_for_file = {
            "settings": clean_settings,
            "mappings": mapping_for_definition
        }

        # Format the output for the file
        definition_string = f"PUT {new_index_name}\n"
        definition_string += json.dumps(index_body_for_file, indent=2)
        definition_string += "\n\n# ----------------------------------------\n\n" # Separator

        definitions_for_file.append(definition_string)
        print(f"  Prepared definition for {new_index_name} to be written to file.")

    except NotFoundError:
        print(f"  Error: Index {index_name} not found during processing.")
    except Exception as e:
        print(f"  An error occurred while processing index {index_name}: {e}")

# Write all collected definitions to the file
if definitions_for_file:
    try:
        with open(output_filename, 'w', encoding='utf-8') as f: # 'w' to overwrite each time
            for definition in definitions_for_file:
                f.write(definition)
        print(f"Successfully wrote all index definitions to {output_filename}")
    except IOError as e:
        print(f"Error writing to file {output_filename}: {e}")
else:
    print("No definitions were generated to write to the file.")


print("Processing complete.")