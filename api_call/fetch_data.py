import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import logging
import time

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

def get_okta_id():
    """Simple function to extract Okta ID from token file"""
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
x_membership_profile_token = tokens.get("membership_profile_token")
transaction_id = tokens.get("transaction_id")

# Get Okta ID - Use the hardcoded one that works in test.py
okta_id = "00u6tk4y3hvxbX9OK5d7"  # Use the same as test.py
logger.info(f"Using Okta ID: {okta_id}")

class WyndhamAPIClient:
    def __init__(self):
        """Initialize the Wyndham API client with headers and tokens"""
        self.base_url = "https://api.wvc.wyndhamdestinations.com/resort-operations/v3/resorts/availability"
        self.okta_id = okta_id
        
        # Headers - copy exactly from test.py
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

    def load_resort_mapping(self, mapping_file='assests/final_MASTER_merged_ssms_with_tzort_mapping.csv'):
        """Load resort mapping from CSV file"""
        try:
            df = pd.read_csv(mapping_file)
            
            # Create mapping from ResortId to Wyndham resort id
            # Remove duplicates and handle NaN values
            mapping = df.drop_duplicates('ResortId')[['ResortId', 'resort id in wyndham onwner site']].copy()
            mapping = mapping.dropna(subset=['resort id in wyndham onwner site'])
            
            # Convert both to proper types and create dictionary
            mapping_dict = {}
            for _, row in mapping.iterrows():
                resort_id = int(row['ResortId'])  # Convert to int (69, 70, etc.)
                wyndham_id = float(row['resort id in wyndham onwner site'])  # Keep as float to handle 67.0
                mapping_dict[resort_id] = wyndham_id
            
            logger.info(f"Loaded {len(mapping_dict)} resort mappings")
            
            # Debug: Print first few mappings
            for i, (resort_id, wyndham_id) in enumerate(list(mapping_dict.items())[:5]):
                logger.info(f"Sample mapping: ResortId {resort_id} -> Wyndham ID {wyndham_id}")
            
            return mapping_dict
            
        except Exception as e:
            logger.error(f"Error loading resort mapping: {e}")
            return {}
    
    def load_orders_data(self, orders_file='csvs/all_resorts_detailed_orders.csv'):
        """Load orders data and filter for MinUnits > 1"""
        try:
            df = pd.read_csv(orders_file)
            # Filter for MinUnits > 1
            filtered_df = df[df['MinUnits'] > 1].copy()
            logger.info(f"Found {len(filtered_df)} orders with MinUnits > 1")
            return filtered_df
        except Exception as e:
            logger.error(f"Error loading orders data: {e}")
            return pd.DataFrame()
    
    def create_product_id(self, wyndham_resort_id):
        """Create product ID format for API call with proper zero padding"""
        try:
            # Convert to integer first to remove any leading zeros
            resort_number = int(float(wyndham_resort_id))  # 67.0 -> 67
            
            # Pad to 12 digits total (9 zeros + 3 digits), not 9 digits total
            padded_number = f"{resort_number:012d}"  # 67 -> 000000000067
            product_id = f"PI|R{padded_number}"      # PI|R000000000067
            
            logger.debug(f"Created product ID: {wyndham_resort_id} -> {product_id}")
            return product_id
        except (ValueError, TypeError):
            logger.error(f"Invalid wyndham_resort_id: {wyndham_resort_id}")
            return None
    
    def fetch_resort_availability(self, product_id, check_in_date, check_out_date, resort_name="", bed_type=None):
        """
        Fetch availability for a specific resort and date range
        
        Args:
            product_id (str): Resort product ID (e.g., "PI|R000000000169")
            check_in_date (str): Check-in date in YYYY-MM-DD format
            check_out_date (str): Check-out date in YYYY-MM-DD format
            resort_name (str): Resort name for logging
            bed_type (str): Optional bed type filter
        
        Returns:
            dict: API response data
        """
        # Update timestamp for each request
        self.headers["x-request-timestamp"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + "Z"
        
        # Base filters - copy exactly from test.py
        filters = [
            {
                "filterType": "include-accessable-units",
                "filterValues": False
            },
            {
                "filterType": "include-clubpass-resorts",
                "filterValues": "true"
            }
        ]
        
        # Remove bed type filter for now - test.py doesn't use it
        # if bed_type:
        #     bed_type_filter = {
        #         "filterType": "unit-size",
        #         "filterValues": [bed_type]
        #     }
        #     filters.append(bed_type_filter)
        
        # Request payload - copy exactly from test.py
        payload = {
            "oktaId": "00u6tk4y3hvxbX9OK5d7",  # Use hardcoded value that works
            "productId": product_id,
            "checkInDate": check_in_date,
            "checkOutDate": check_out_date,
            "filters": filters,
            "purchaseType": False
        }
        
        try:
            bed_info = f" (Bed Type: {bed_type})" if bed_type else ""
            logger.info(f"Fetching data for {resort_name} ({product_id}) from {check_in_date} to {check_out_date}{bed_info}")
            
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
            logger.error(f"❌ Error fetching data for {product_id} ({resort_name}): {e}")
            return None

    def process_orders_and_fetch_data(self):
        """Main method to process orders and fetch availability data"""
        # Load mapping and orders data
        resort_mapping = self.load_resort_mapping()
        orders_df = self.load_orders_data()
        
        if orders_df.empty:
            logger.warning("No orders data found")
            return []
        
        # Process each unique combination - convert resort_id to int for mapping lookup
        unique_orders = orders_df[['ResortId', 'Resort', 'Arrival', 'Departure', 'MinUnits', 'Studio', 'Bed1', 'Bed2', 'Bed3', 'Bed4']].drop_duplicates()
        
        results = []
        processed_count = 0
        success_count = 0
        error_count = 0
        
        total_orders = len(unique_orders)
        logger.info(f"Processing {total_orders} unique orders...")
        
        for _, row in unique_orders.iterrows():
            try:
                resort_id = int(row['ResortId'])  # Convert to int for mapping lookup
                resort_name = row['Resort']
                arrival = row['Arrival']
                departure = row['Departure']
                min_units = row['MinUnits']
                
                # Determine bed type from boolean columns
                bed_type = None
                if row['Studio']:
                    bed_type = "Studio"
                elif row['Bed1']:
                    bed_type = "1 Bedroom"
                elif row['Bed2']:
                    bed_type = "2 Bedroom"
                elif row['Bed3']:
                    bed_type = "3 Bedroom"
                elif row['Bed4']:
                    bed_type = "4 Bedroom"
                
                # Check if we have mapping for this resort
                if resort_id not in resort_mapping:
                    logger.warning(f"No Wyndham mapping found for ResortId {resort_id} ({resort_name})")
                    error_count += 1
                    continue
                
                wyndham_resort_id = resort_mapping[resort_id]
                product_id = self.create_product_id(wyndham_resort_id)
                
                # Check if product_id creation was successful
                if not product_id:
                    logger.error(f"Failed to create product_id for {resort_name} (Wyndham ID: {wyndham_resort_id})")
                    error_count += 1
                    continue
                
                # Convert dates to YYYY-MM-DD format
                try:
                    arrival_date = pd.to_datetime(arrival).strftime('%Y-%m-%d')
                    departure_date = pd.to_datetime(departure).strftime('%Y-%m-%d')
                except Exception as date_error:
                    logger.error(f"Invalid date format for {resort_name}: {arrival} to {departure} - {date_error}")
                    error_count += 1
                    continue
                
                logger.info(f"Mapping: ResortId {resort_id} ({resort_name}) -> Wyndham ID {wyndham_resort_id} -> Product ID {product_id}")
                
                # Fetch availability data
                availability_data = self.fetch_resort_availability(
                    product_id, arrival_date, departure_date, resort_name, bed_type
                )
                
                processed_count += 1
                
                if availability_data:
                    success_count += 1
                    results.append({
                        'resort_id': resort_id,
                        'resort_name': resort_name,
                        'wyndham_resort_id': wyndham_resort_id,
                        'product_id': product_id,
                        'check_in': arrival_date,
                        'check_out': departure_date,
                        'min_units': min_units,
                        'bed_type': bed_type,
                        'api_data': availability_data,
                        'fetch_timestamp': datetime.now().isoformat(),
                        'status': 'success'
                    })
                    logger.info(f"✅ Successfully fetched data for {resort_name}")
                else:
                    error_count += 1
                    # Still save the failed attempt for tracking
                    results.append({
                        'resort_id': resort_id,
                        'resort_name': resort_name,
                        'wyndham_resort_id': wyndham_resort_id,
                        'product_id': product_id,
                        'check_in': arrival_date,
                        'check_out': departure_date,
                        'min_units': min_units,
                        'bed_type': bed_type,
                        'api_data': None,
                        'fetch_timestamp': datetime.now().isoformat(),
                        'status': 'failed',
                        'error_reason': 'No search results found or API error'
                    })
                    logger.warning(f"❌ Failed to fetch data for {resort_name}")
                
                # Add small delay between requests to avoid rate limiting
                time.sleep(0.5)
                
                # Log progress every 5 requests
                if processed_count % 5 == 0:
                    logger.info(f"Progress: {processed_count}/{total_orders} processed, {success_count} successful, {error_count} failed")
            
            except Exception as row_error:
                logger.error(f"Error processing row: {row_error}")
                error_count += 1
                continue
        
        logger.info(f"Completed: {processed_count} total requests, {success_count} successful, {error_count} failed")
        return results
    
    def save_results(self, results, filename=None):
        """Save API results to JSON and CSV files"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"resort_availability_results_{timestamp}.json"
        
        try:
            # Create results directory if it doesn't exist
            os.makedirs("results", exist_ok=True)
            filepath = os.path.join("results", filename)
            
            # Save detailed JSON results
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Results saved to {filepath}")
            
            # Save summary CSV
            summary_data = []
            for result in results:
                summary_data.append({
                    'resort_id': result['resort_id'],
                    'resort_name': result['resort_name'],
                    'wyndham_resort_id': result['wyndham_resort_id'],
                    'product_id': result['product_id'],
                    'check_in': result['check_in'],
                    'check_out': result['check_out'],
                    'min_units': result['min_units'],
                    'bed_type': result.get('bed_type', ''),
                    'api_success': result['api_data'] is not None,
                    'fetch_timestamp': result['fetch_timestamp']
                })
            
            summary_df = pd.DataFrame(summary_data)
            summary_filepath = os.path.join("results", f"summary_{filename.replace('.json', '.csv')}")
            summary_df.to_csv(summary_filepath, index=False)
            logger.info(f"Summary saved to {summary_filepath}")
            
            return filepath
            
        except Exception as e:
            logger.error(f"Error saving results: {e}")
            return None

    def save_test_result(self, data, filename="test_result.json"):
        """Save test result to file (from test.py)"""
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

def main():
    """Main function to fetch resort availability data"""
    try:
        print("🚀 Starting Wyndham API Data Fetching...\n")
        
        client = WyndhamAPIClient()
        
        # Process orders and fetch availability data
        results = client.process_orders_and_fetch_data()
        
        if results:
            # Save results
            filepath = client.save_results(results)
            print(f"✅ Successfully processed {len(results)} resort availability requests")
            print(f"📁 Results saved to: {filepath}")
            
            # Show summary
            print(f"\n📊 Summary:")
            for result in results:
                status = "SUCCESS" if result['api_data'] else "FAILED"
                bed_info = f" ({result.get('bed_type', 'Any')})" if result.get('bed_type') else ""
                print(f"   {result['resort_name']}{bed_info}: {result['check_in']} to {result['check_out']} - {status}")
        else:
            print("❌ No data was fetched successfully")
            
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()