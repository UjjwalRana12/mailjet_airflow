import requests
import json
import pandas as pd
from datetime import datetime
import os
import logging
import time

# Setup logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/api_fetch.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load tokens
with open(r'assests\token.json', 'r', encoding='utf-8') as f:
    tokens = json.load(f)

access_token = tokens.get("access_token")
x_jwt_token = tokens.get("party_token")
x_membership_profile_token = tokens.get("membership_profile_token")
transaction_id = tokens.get("transaction_id")

class WyndhamAPIClient:
    def __init__(self):
        self.base_url = "https://api.wvc.wyndhamdestinations.com/resort-operations/v3/resorts/availability"
        
        self.headers = {
            "accept": "application/json, text/plain, */*",
            "authorization": f"Bearer {access_token}",
            "content-type": "application/json;charset=UTF-8",
            "x-brandid": "000",
            "x-channel": "WEB",
            "x-jwt-token": f"{x_jwt_token}",
            "x-membership-profile-token": f"{x_membership_profile_token}",
            "x-originator-applicationid": "CUI",
            "x-transactionid": f"{transaction_id}",
            "x-userid": "KITTY2112$"
        }

    def create_product_id(self, wyndham_resort_id):
        """Create product ID format"""
        resort_number = int(float(wyndham_resort_id))
        padded_number = f"{resort_number:012d}"
        return f"PI|R{padded_number}"

    def fetch_availability(self, product_id, check_in_date, check_out_date):
        """Fetch availability for a resort"""
        # Update timestamp
        self.headers["x-request-timestamp"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z"
        
        payload = {
            "oktaId": "00u6tk4y3hvxbX9OK5d7",
            "productId": product_id,
            "checkInDate": check_in_date,
            "checkOutDate": check_out_date,
            "filters": [
                {"filterType": "include-accessable-units", "filterValues": False},
                {"filterType": "include-clubpass-resorts", "filterValues": "true"}
            ],
            "purchaseType": False
        }
        
        try:
            logger.info(f"Fetching {product_id} from {check_in_date} to {check_out_date}")
            
            response = requests.post(self.base_url, headers=self.headers, json=payload, timeout=30)
            
            if response.status_code == 200:
                logger.info("✅ Success")
                return response.json()
            else:
                logger.warning(f"❌ Failed: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error: {e}")
            return None

    def fetch_multiple_resorts(self):
        """Fetch data for multiple resorts"""
        
        # Simple mapping: ResortId -> Wyndham ID
        resort_mapping = {
            69: 67.0,    # Mountain Vista Resort
            70: 5.0,     # Ocean Walk Resort  
            71: 173.0,   # Resort 71
            72: 19.0,    # Resort 72
            73: 7.0      # Resort 73
        }
        
        # Simple date ranges to test
        test_requests = [
            {"resort_id": 69, "wyndham_id": 67.0, "check_in": "2025-11-04", "check_out": "2025-11-08"},
            {"resort_id": 70, "wyndham_id": 5.0, "check_in": "2025-11-10", "check_out": "2025-11-14"},
            {"resort_id": 71, "wyndham_id": 173.0, "check_in": "2025-11-15", "check_out": "2025-11-19"},
            {"resort_id": 72, "wyndham_id": 19.0, "check_in": "2025-11-20", "check_out": "2025-11-24"},
            {"resort_id": 73, "wyndham_id": 7.0, "check_in": "2025-11-25", "check_out": "2025-11-29"}
        ]
        
        results = []
        
        for i, request in enumerate(test_requests, 1):
            logger.info(f"[{i}/{len(test_requests)}] Processing Resort {request['resort_id']}")
            
            product_id = self.create_product_id(request['wyndham_id'])
            
            data = self.fetch_availability(
                product_id, 
                request['check_in'], 
                request['check_out']
            )
            
            results.append({
                'resort_id': request['resort_id'],
                'wyndham_id': request['wyndham_id'],
                'product_id': product_id,
                'check_in': request['check_in'],
                'check_out': request['check_out'],
                'api_data': data,
                'success': data is not None,
                'timestamp': datetime.now().isoformat()
            })
            
            # Delay between requests
            time.sleep(1)
        
        return results

    def save_results(self, results):
        """Save results to JSON"""
        os.makedirs("results", exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"simple_results_{timestamp}.json"
        filepath = os.path.join("results", filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Results saved to {filepath}")
        return filepath

def main():
    print("🚀 Starting Simple API Fetch...\n")
    
    client = WyndhamAPIClient()
    
    # Fetch data for multiple resorts
    results = client.fetch_multiple_resorts()
    
    # Save results
    filepath = client.save_results(results)
    
    # Print summary
    successful = [r for r in results if r['success']]
    failed = [r for r in results if not r['success']]
    
    print(f"\n📊 Results:")
    print(f"   ✅ Successful: {len(successful)}")
    print(f"   ❌ Failed: {len(failed)}")
    print(f"   📁 Saved to: {filepath}")
    
    for result in results:
        status = "✅" if result['success'] else "❌"
        print(f"   {status} Resort {result['resort_id']}: {result['check_in']} to {result['check_out']}")

if __name__ == "__main__":
    main()