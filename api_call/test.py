import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import logging

load_dotenv()
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
with open(r'assests\token.json', 'r', encoding='utf-8') as f:  # Fixed: assests not assets
    tokens = json.load(f)

access_token = tokens.get("access_token")
x_jwt_token = tokens.get("party_token")
x_membership_profile_token = tokens.get("membership_profile_token")
transaction_id = tokens.get("transaction_id")

class WyndhamAPIClient:
    def __init__(self):
        """Initialize the Wyndham API client with headers and tokens"""
        self.base_url = "https://api.wvc.wyndhamdestinations.com/resort-operations/v3/resorts/availability"
        
        # Headers from the fetch request
        self.headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "en-US,en;q=0.9,en-IN;q=0.8,hi;q=0.7,pt-PT;q=0.6,pt;q=0.5",
            "authorization": f"Bearer {access_token}",
            "content-type": "application/json;charset=UTF-8",
            "priority": "u=1, i",
            "sec-ch-ua": "\"Microsoft Edge\";v=\"141\", \"Not?A_Brand\";v=\"8\", \"Chromium\";v=\"141\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "x-brandid": "000",
            "x-channel": "WEB",
            "x-jwt-token": f"{x_jwt_token}",
            "x-membership-profile-token": f"{x_membership_profile_token}",
            "x-originator-applicationid": "CUI",
            "x-originator-hostname": "2405:201:681b:2182:45d8:610:456b:ad32",
            "x-request-timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z",
            "x-sessionid": "",
            "x-transactionid": f"{transaction_id}",    
            "x-userid": "KITTY2112$",
            "x-uuid": ""
        }

    def test_single_request(self, product_id="PI|R000000000169", check_in="2025-11-04", check_out="2025-11-08"):
        """Test a single API request"""
        # Update timestamp for each request
        self.headers["x-request-timestamp"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z"
        
        # Request payload
        payload = {
            "oktaId": "00u6tk4y3hvxbX9OK5d7",
            "productId": product_id,
            "checkInDate": check_in,
            "checkOutDate": check_out,
            "filters": [
                {
                    "filterType": "include-accessable-units",
                    "filterValues": False
                },
                {
                    "filterType": "include-clubpass-resorts",
                    "filterValues": "true"
                }
            ],
            "purchaseType": False
        }
        
        try:
            logger.info(f"Testing API call for {product_id} from {check_in} to {check_out}")
            response = requests.post(
                self.base_url,
                headers=self.headers,
                json=payload,
                timeout=30
            )
            
            logger.info(f"Response Status Code: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                logger.info("✅ API call successful!")
                return data
            else:
                logger.error(f"❌ API call failed with status code: {response.status_code}")
                logger.error(f"Response: {response.text}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Error making API request: {e}")
            return None
    
    def save_test_result(self, data, filename="test_result.json"):
        """Save test result to file"""
        try:
            os.makedirs("test_results", exist_ok=True)
            filepath = os.path.join("test_results", filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"📁 Test result saved to {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Error saving test result: {e}")
            return None

def test_api_connection():
    """Test API connection and response"""
    try:
        print("🔍 Testing Wyndham API connection...")
        
        client = WyndhamAPIClient()
        
        # Test with the exact same parameters that work on the website
        test_data = client.test_single_request(
            product_id="PI|R000000000169",  # Using the exact format from website
            check_in="2025-11-04",
            check_out="2025-11-08"
        )
        
        if test_data:
            print("✅ API test successful!")
            
            # Save result
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"api_test_{timestamp}.json"
            filepath = client.save_test_result(test_data, filename)
            
            # Print the full response
            print("\n" + "="*80)
            print("🎯 API RESPONSE:")
            print("="*80)
            print(json.dumps(test_data, indent=2, ensure_ascii=False))
            print("="*80)
            
            # Print some basic info about the response
            if 'data' in test_data:
                data = test_data['data']
                print(f"\n📍 Resort: {data.get('resort', {}).get('name', 'N/A')}")
                print(f"📅 Check-in: {data.get('checkInDate', 'N/A')}")
                print(f"📅 Check-out: {data.get('checkOutDate', 'N/A')}")
                
                if 'availabilities' in data:
                    availabilities = data['availabilities']
                    print(f"🏨 Available options: {len(availabilities)}")
                    
                    if availabilities:
                        print("\n📋 Sample availability:")
                        first_availability = availabilities[0]
                        print(f"   Unit Type: {first_availability.get('unitType', 'N/A')}")
                        print(f"   Unit Size: {first_availability.get('unitSize', 'N/A')}")
                        print(f"   Available Units: {first_availability.get('availableUnits', 'N/A')}")
                        print(f"   Is Available: {first_availability.get('isAvailable', 'N/A')}")
                        print(f"   Price: {first_availability.get('price', 'N/A')}")
                        print(f"   Points Required: {first_availability.get('pointsRequired', 'N/A')}")
                else:
                    print("❌ No availability data found")
            
            return True
        else:
            print("❌ API test failed!")
            return False
            
    except Exception as e:
        logger.error(f"Error in API test: {e}")
        print(f"❌ Test error: {e}")
        return False

def test_multiple_scenarios():
    """Test multiple scenarios"""
    print("🔍 Testing multiple scenarios...")
    
    client = WyndhamAPIClient()
    
    # Test scenarios using the correct product ID format
    test_scenarios = [
        {
            "name": "Website Example (PI|R000000000169)",
            "product_id": "PI|R000000000169",
            "check_in": "2025-11-04",
            "check_out": "2025-11-08"
        },
        {
            "name": "Mountain Vista Resort (9-digit format)",
            "product_id": "PI|R000000067",  # Convert 67 to 9-digit format
            "check_in": "2025-11-11",
            "check_out": "2025-11-13"
        },
        {
            "name": "Ocean Walk Resort (9-digit format)",
            "product_id": "PI|R000000005",  # Convert 5 to 9-digit format
            "check_in": "2025-11-11",
            "check_out": "2025-11-13"
        }
    ]
    
    successful_tests = 0
    
    for i, scenario in enumerate(test_scenarios, 1):
        print(f"\n{'='*60}")
        print(f"🧪 Test {i}: {scenario['name']}")
        print(f"{'='*60}")
        
        result = client.test_single_request(
            product_id=scenario['product_id'],
            check_in=scenario['check_in'],
            check_out=scenario['check_out']
        )
        
        if result:
            successful_tests += 1
            print(f"✅ {scenario['name']} - SUCCESS!")
            
            # Save successful result
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"success_test_{i}_{timestamp}.json"
            client.save_test_result(result, filename)
            
            # Show brief summary
            if 'data' in result and 'availabilities' in result['data']:
                avail_count = len(result['data']['availabilities'])
                print(f"   📊 Found {avail_count} availability options")
        else:
            print(f"❌ {scenario['name']} - FAILED!")
        
        # Small delay between tests
        import time
        time.sleep(1)
    
    print(f"\n📊 Test Results: {successful_tests}/{len(test_scenarios)} scenarios successful")
    return successful_tests > 0

def test_token_validity():
    """Test if tokens are loaded correctly"""
    print("🔑 Testing token validity...")
    
    try:
        with open(r'assests\token.json', 'r', encoding='utf-8') as f:  
            tokens = json.load(f)
        
        required_tokens = ["access_token", "party_token", "membership_profile_token", "transaction_id"]
        
        for token_name in required_tokens:
            token_value = tokens.get(token_name)
            if token_value:
                print(f"✅ {token_name}: {'*' * 20}{token_value[-10:]}")
            else:
                print(f"❌ {token_name}: Missing!")
                return False
        
        return True
        
    except FileNotFoundError:
        print("❌ Token file not found: assests/token.json")
        return False
    except Exception as e:
        print(f"❌ Error loading tokens: {e}")
        return False

def main():
    """Main test function"""
    print("🚀 Starting Wyndham API Tests...\n")
    
    # Test 1: Token validity
    print("=" * 50)
    token_test = test_token_validity()
    
    if not token_test:
        print("❌ Token test failed. Please check your token.json file.")
        return
    
    print("\n" + "=" * 50)
    
    # Test 2: Single API test with website example
    api_test = test_api_connection()
    
    if not api_test:
        print("\n" + "=" * 50)
        print("🔄 Trying multiple scenarios...")
        api_test = test_multiple_scenarios()
    
    print("\n" + "=" * 50)
    print("📊 Test Summary:")
    print(f"🔑 Token Test: {'✅ PASS' if token_test else '❌ FAIL'}")
    print(f"🌐 API Test: {'✅ PASS' if api_test else '❌ FAIL'}")
    
    if token_test and api_test:
        print("\n🎉 Tests passed! Your API setup is working correctly.")
    else:
        print("\n⚠️ API tests failed. Check if:")
        print("   1. Tokens are still valid (they might have expired)")
        print("   2. Product IDs are correct")
        print("   3. Account has access to these resorts")

if __name__ == "__main__":
    main()