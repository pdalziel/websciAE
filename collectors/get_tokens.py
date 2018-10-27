import json
from os import path


def load_access_token():
    file_path = path.join(path.dirname(__file__), 'secret.json')
    with open(file_path, 'r') as f:
        secrets_dict = json.load(f)
        return secrets_dict.get("secrets")

