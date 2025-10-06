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

def get_okta_id():
    """Extract Okta ID from token file"""
    try:
        with open(r'assests\token.json', 'r', encoding='utf-8') as f:
            tokens = json.load(f)
        
        transaction_id = tokens.get('transaction_id', '')
        
        if 'AT' in transaction_id:
            okta_id = transaction_id.split('AT')[0].lstrip('0')
            return okta_id
        
        return None
        
    except Exception as e:
        logger.error(f"Error extracting Okta ID: {e}")
        return None

# Load tokens
with open(r'assests\token.json', 'r', encoding='utf-8') as f:
    tokens = json.load(f)

access_token = tokens.get("access_token")
x_jwt_token = tokens.get("party_token")
x_membersship_profile_token = tokens.get("membership_profile_token")
transaction_id = tokens.get("transaction_id")

# Get Okta ID
okta_id = get_okta_id()
if not okta_id:
    logger.error("Could not extract Okta ID from token file")
    exit(1)

logger.info(f"Using Okta ID: {okta_id}")

class WyndhamAPIClient:
    def __init__(self):
        self.base_url = "https://api.wvc.wyndhamdestinations.com/resort-operations/v3/resorts/availability"
        self.okta_id = okta_id
        
        self.headers = {
            "accept": "application/json, text/plain, */*",
            "authorization": f"Bearer {access_token}",
            "content-type": "application/json;charset=UTF-8",
            "x-brandid": "000",
            "x-channel": "WEB",
            "x-jwt-token": f"{x_jwt_token}",
            "x-membership-profile-token": f"{x_membersship_profile_token}",
            "x-originator-applicationid": "CUI",
            "x-transactionid": f"{transaction_id}",
            "x-userid": "KITTY2112$"
        }

    def load_resort_mapping(self):
        """Load resort mapping from CSV file"""
        try:
            df = pd.read_csv('assests/final_MASTER_merged_ssms_with_tzort_mapping.csv')
            
            # Create mapping from ResortId to Wyndham resort id
            mapping = df.drop_duplicates('ResortId')[['ResortId', 'resort id in wyndham onwner site']].copy()
            mapping = mapping.dropna(subset=['resort id in wyndham onwner site'])
            
            mapping_dict = {}
            for _, row in mapping.iterrows():
                resort_id = int(row['ResortId'])
                wyndham_id = float(row['resort id in wyndham onwner site'])
                mapping_dict[resort_id] = wyndham_id
            
            logger.info(f"Loaded {len(mapping_dict)} resort mappings")
            return mapping_dict
            
        except Exception as e:
            logger.error(f"Error loading resort mapping: {e}")
            return {}

    def load_orders_data(self):
        """Load orders data and filter for MinUnits > 1"""
        try:
            df = pd.read_csv('csvs/all_resorts_detailed_orders.csv')
            
            # Filter for MinUnits > 1
            filtered_df = df[df['MinUnits'] > 1].copy()
            logger.info(f"Found {len(filtered_df)} orders with MinUnits > 1")
            
            # Get unique combinations of ResortId, Arrival, Departure
            unique_requests = filtered_df[['ResortId', 'Resort', 'Arrival', 'Departure']].drop_duplicates()
            logger.info(f"Found {len(unique_requests)} unique resort/date combinations")
            
            return unique_requests
            
        except Exception as e:
            logger.error(f"Error loading orders data: {e}")
            return pd.DataFrame()

    def create_product_id(self, wyndham_resort_id):
        """Create product ID format"""
        resort_number = int(float(wyndham_resort_id))
        padded_number = f"{resort_number:012d}"
        return f"PI|R{padded_number}"

    def fetch_availability(self, product_id, check_in_date, check_out_date, resort_name):
        """Fetch availability for a resort"""
        # Update timestamp
        self.headers["x-request-timestamp"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z"
        
        payload = {
            "oktaId": self.okta_id,
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
            logger.info(f"Fetching {resort_name} ({product_id}) from {check_in_date} to {check_out_date}")
            
            response = requests.post(self.base_url, headers=self.headers, json=payload, timeout=30)
            
            if response.status_code == 200:
                logger.info("✅ Success")
                return response.json()
            else:
                logger.warning(f"❌ Failed: {response.status_code} - {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error: {e}")
            return None

    def save_individual_result(self, resort_id, resort_name, product_id, check_in, check_out, api_data):
        """Save individual API result to separate file"""
        try:
            # Create results directory
            os.makedirs("api_results", exist_ok=True)
            
            # Create filename with resort info and dates
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_resort_name = "".join(c for c in resort_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
            safe_resort_name = safe_resort_name.replace(' ', '_')
            
            filename = f"resort_{resort_id}_{safe_resort_name}_{check_in}_to_{check_out}_{timestamp}.json"
            filepath = os.path.join("api_results", filename)
            
            # Prepare data to save
            result_data = {
                "request_info": {
                    "resort_id": resort_id,
                    "resort_name": resort_name,
                    "product_id": product_id,
                    "check_in": check_in,
                    "check_out": check_out,
                    "fetch_timestamp": datetime.now().isoformat(),
                    "success": api_data is not None
                },
                "api_response": api_data
            }
            
            # Save to file
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(result_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"💾 Saved result to: {filename}")
            return filepath
            
        except Exception as e:
            logger.error(f"Error saving individual result: {e}")
            return None

    def process_csv_data(self):
        """Main method to process CSV data and make API calls"""
        # Load mapping and orders data
        resort_mapping = self.load_resort_mapping()
        orders_df = self.load_orders_data()
        
        if orders_df.empty:
            logger.warning("No orders data found")
            return []
        
        results = []
        processed_count = 0
        success_count = 0
        error_count = 0
        
        total_requests = len(orders_df)
        logger.info(f"Starting to process {total_requests} API requests...")
        
        for index, row in orders_df.iterrows():
            try:
                resort_id = int(row['ResortId'])
                resort_name = row['Resort']
                arrival = row['Arrival']
                departure = row['Departure']
                
                # Check if we have mapping for this resort
                if resort_id not in resort_mapping:
                    logger.warning(f"No Wyndham mapping found for ResortId {resort_id} ({resort_name})")
                    error_count += 1
                    continue
                
                wyndham_resort_id = resort_mapping[resort_id]
                product_id = self.create_product_id(wyndham_resort_id)
                
                # Convert dates to YYYY-MM-DD format
                try:
                    arrival_date = pd.to_datetime(arrival).strftime('%Y-%m-%d')
                    departure_date = pd.to_datetime(departure).strftime('%Y-%m-%d')
                except Exception as date_error:
                    logger.error(f"Invalid date format for {resort_name}: {arrival} to {departure} - {date_error}")
                    error_count += 1
                    continue
                
                logger.info(f"[{processed_count + 1}/{total_requests}] Processing: {resort_name} (ResortId: {resort_id} -> Product: {product_id})")
                
                # Fetch availability data
                api_data = self.fetch_availability(product_id, arrival_date, departure_date, resort_name)
                
                processed_count += 1
                
                # Save individual result to file
                filepath = self.save_individual_result(
                    resort_id, resort_name, product_id, 
                    arrival_date, departure_date, api_data
                )
                
                if api_data:
                    success_count += 1
                    status = "SUCCESS"
                else:
                    error_count += 1
                    status = "FAILED"
                
                # Track result
                results.append({
                    'resort_id': resort_id,
                    'resort_name': resort_name,
                    'wyndham_resort_id': wyndham_resort_id,
                    'product_id': product_id,
                    'check_in': arrival_date,
                    'check_out': departure_date,
                    'status': status,
                    'filepath': filepath,
                    'fetch_timestamp': datetime.now().isoformat()
                })
                
                # Progress update every 5 requests
                if processed_count % 5 == 0:
                    logger.info(f"📊 Progress: {processed_count}/{total_requests} processed, {success_count} successful, {error_count} failed")
                
                # Delay between requests to avoid rate limiting
                time.sleep(1)
                
            except Exception as row_error:
                logger.error(f"Error processing row {index}: {row_error}")
                error_count += 1
                continue
        
        logger.info(f"🏁 Completed: {processed_count} total requests, {success_count} successful, {error_count} failed")
        return results

    def save_summary_report(self, results):
        """Save summary report of all API calls"""
        try:
            os.makedirs("reports", exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Save summary CSV
            summary_df = pd.DataFrame(results)
            csv_path = f"reports/api_calls_summary_{timestamp}.csv"
            summary_df.to_csv(csv_path, index=False)
            
            # Save summary JSON
            json_path = f"reports/api_calls_summary_{timestamp}.json"
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump({
                    "summary": {
                        "total_requests": len(results),
                        "successful": len([r for r in results if r['status'] == 'SUCCESS']),
                        "failed": len([r for r in results if r['status'] == 'FAILED']),
                        "generation_time": datetime.now().isoformat()
                    },
                    "results": results
                }, f, indent=2, ensure_ascii=False)
            
            logger.info(f"📋 Summary report saved to: {csv_path} and {json_path}")
            return csv_path, json_path
            
        except Exception as e:
            logger.error(f"Error saving summary report: {e}")
            return None, None

def main():
    print("🚀 Starting CSV-based API Data Fetching...\n")
    
    client = WyndhamAPIClient()
    
    # Process CSV data and make API calls
    results = client.process_csv_data()
    
    if results:
        # Save summary report
        csv_path, json_path = client.save_summary_report(results)
        
        # Print final summary
        successful = [r for r in results if r['status'] == 'SUCCESS']
        failed = [r for r in results if r['status'] == 'FAILED']
        
        print(f"\n🎉 Processing Complete!")
        print(f"   ✅ Successful API calls: {len(successful)}")
        print(f"   ❌ Failed API calls: {len(failed)}")
        print(f"   📁 Individual files saved in: api_results/")
        print(f"   📋 Summary report: {csv_path}")
        
        # Show sample successful calls
        if successful:
            print(f"\n🔥 Sample successful calls:")
            for result in successful[:3]:
                print(f"   - {result['resort_name']}: {result['check_in']} to {result['check_out']}")
    else:
        print("❌ No data was processed")

if __name__ == "__main__":
    main()