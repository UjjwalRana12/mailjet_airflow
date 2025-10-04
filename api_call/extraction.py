import json
import logging
import os
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/extraction.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)
def extract_auth_token_object(json_file_path):
    """
    Read JSON file and extract authTokenObject
    """
    try:
        # Read the JSON file
        with open(json_file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        # Get the authTokenObject (it's a JSON string)
        auth_token_string = data.get('authTokenObject')
        logging.info(f"auth token is {auth_token_string}")
        
        if auth_token_string:
            # Parse the JSON string to get the actual object
            auth_token_object = json.loads(auth_token_string)
            return auth_token_object
        else:
            print("authTokenObject not found in the JSON file")
            return None
            
    except FileNotFoundError:
        print(f"File {json_file_path} not found")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None

def extract_membership_token(json_file_path):
    """
    Read JSON file and extract authTokenObject
    """
    try:
        # Read the JSON file
        with open(json_file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        # Get the authTokenObject (it's a JSON string)
        living_token_string = data.get('livingObject')
        logging.info(f"memebrship token is {living_token_string}")
        
        if living_token_string:
            # Parse the JSON string to get the actual object
            living_token_object = json.loads(living_token_string)
            return living_token_object
        else:
            print("authTokenObject not found in the JSON file")
            return None
            
    except FileNotFoundError:
        print(f"File {json_file_path} not found")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None
    

def extract_party_access_token(json_file_path):
    """
    Extract only the partyAccessToken from JSON file
    """
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    return data.get('partyAccessToken')


# Usage
json_file_path = r'auth_data\session_storage.json' 
auth_token_obj = extract_auth_token_object(json_file_path)
living_token_object = extract_membership_token(json_file_path)
party_token_obj = extract_party_access_token(json_file_path)

# Extract tokens
access_token = None
domain = None
membership_token = None


if auth_token_obj:
    access_token = auth_token_obj.get('access_token')
    domain = auth_token_obj.get('domain')

if living_token_object:
    membership_token = living_token_object.get('memberProfileToken')
    if not membership_token:
        membership_token = living_token_object.get('login-memberProfileToken')

if party_token_obj:
    print(f"party token object {party_token_obj}")

# Prepare token data
tokens = {
    "access_token": access_token,
    "domain": domain,
    "membership_profile_token": membership_token,
    "party_token": party_token_obj
}

# Save to token.json in assests folder
os.makedirs(r'assests', exist_ok=True)
with open(r'assests\token.json', 'w', encoding='utf-8') as f:
    json.dump(tokens, f, indent=2)

print("Tokens extracted and saved to token.json")

