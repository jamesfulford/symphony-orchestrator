import typing

import requests
import edn_format

from . import edn_syntax

COMPOSER_CONFIG = {
    "projectId": "leverheads-278521",
    "databaseName": "(default)"
}


def get_symphony(symphony_id: str) -> dict:

    print(f"Fetching symphony {symphony_id} from Composer")
    response = requests.get(
        f'https://firestore.googleapis.com/v1/projects/{COMPOSER_CONFIG["projectId"]}/databases/{COMPOSER_CONFIG["databaseName"]}/documents/symphony/{symphony_id}')
    response.raise_for_status()

    response_json = response.json()
    return response_json


def get_public_symphonies() -> list[dict]:
    response = requests.get(
        f"https://firestore.googleapis.com/v1/projects/{COMPOSER_CONFIG['projectId']}/databases/{COMPOSER_CONFIG['databaseName']}/documents/public_symphony/")
    response.raise_for_status()

    response_json = response.json()
    print(f"fetched {len(response_json['documents'])} public symphonies")
    return response_json['documents']

def extract_root_node_from_symphony_response(response: dict) -> dict:
    return typing.cast(dict, edn_syntax.convert_edn_to_pythonic(
        edn_format.loads(response['fields']['latest_version_edn']['stringValue'])))
