import jwt
import json

def generate_transaction_id():
    json_path = r'assests\token.json'
    with open(json_path, 'r', encoding='utf-8') as f:
        tokens = json.load(f)

    jwt_token = tokens.get("party_token")
    if not jwt_token:
        return None

    try:
        decoded = jwt.decode(jwt_token, options={"verify_signature": False})
        correlation_id = decoded.get('correlation_id')
        if correlation_id:
            tokens['transaction_id'] = correlation_id
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(tokens, f, indent=2)
        return correlation_id
    except Exception as e:
        print(f"Error decoding JWT: {e}")
        return None

transaction_id = generate_transaction_id()
print(transaction_id)

