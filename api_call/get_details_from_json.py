import json
import pandas as pd
from datetime import datetime
import os
import glob

def extract_detailed_availability_from_json(json_file_path):
    """
    Extract detailed availability data from JSON response file using actual counts
    """
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Get request info from the file structure
        request_info = data.get('request_info', {})
        api_response = data.get('api_response', {})
        
        # If no API response (failed call), return empty DataFrame
        if not api_response:
            return pd.DataFrame()
        
        detailed_data = []
        resorts = api_response.get('resorts', [])
        
        if resorts:
            for resort in resorts:
                resort_name = resort.get('name', 'Unknown Resort')
                product_id = resort.get('productId', '')
                
                resort_offerings = resort.get('resortOfferings', [])
                
                for offering in resort_offerings:
                    offering_name = offering.get('offeringName', '')
                    accom_classes = offering.get('accomdationClasses', [])
                    
                    for accom in accom_classes:
                        unit_type = accom.get('unitType', '')
                        unit_name = accom.get('unitName', '')
                        total_points_before = int(accom.get('totalPointsBeforeDiscount', 0))
                        total_points_after = int(accom.get('totalPointsAfterDiscount', 0))
                        
                        # Calculate minimum available count across all days
                        min_available = float('inf')
                        calendar_days = accom.get('calendarDays', [])
                        
                        for day in calendar_days:
                            date = day.get('date', '')
                            inventory_offerings = day.get('inventoryOfferings', [])
                            
                            # For each day, take the MAXIMUM available count across consumer types
                            day_max_available = 0
                            for inv in inventory_offerings:
                                available_count = int(inv.get('availableCount', 0))
                                day_max_available = max(day_max_available, available_count)
                            
                            # Take the MINIMUM across all days (bottleneck for the entire stay)
                            min_available = min(min_available, day_max_available)
                        
                        # Handle case where no days found
                        if min_available == float('inf'):
                            min_available = 0
                        
                        detailed_data.append({
                            'resort_id': request_info.get('resort_id', ''),
                            'check_in': request_info.get('check_in', ''),
                            'check_out': request_info.get('check_out', ''),
                            'resort_name': resort_name,
                            'offering_name': offering_name,
                            'unit_type': unit_type,
                            'unit_name': unit_name,
                            'product_id': product_id,
                            'original_points': total_points_before,
                            'current_points': total_points_after,
                            'available_count': min_available,
                            'savings': total_points_before - total_points_after,
                            'discount_percentage': round(((total_points_before - total_points_after) / total_points_before * 100), 1) if total_points_before > 0 else 0,
                            'extraction_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'fetch_timestamp': request_info.get('fetch_timestamp', '')
                        })
        
        return pd.DataFrame(detailed_data)
        
    except Exception as e:
        print(f"Error extracting detailed data from {json_file_path}: {e}")
        return pd.DataFrame()

def extract_daily_breakdown_from_json(json_file_path):
    """Extract day-by-day breakdown from JSON file"""
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        request_info = data.get('request_info', {})
        api_response = data.get('api_response', {})
        
        if not api_response:
            return pd.DataFrame()
        
        daily_data = []
        resorts = api_response.get('resorts', [])
        
        if resorts:
            for resort in resorts:
                resort_name = resort.get('name', 'Unknown Resort')
                resort_offerings = resort.get('resortOfferings', [])
                
                for offering in resort_offerings:
                    accom_classes = offering.get('accomdationClasses', [])
                    
                    for accom in accom_classes:
                        unit_name = accom.get('unitName', '')
                        calendar_days = accom.get('calendarDays', [])
                        
                        for day in calendar_days:
                            date = day.get('date', '')
                            inventory_offerings = day.get('inventoryOfferings', [])
                            
                            for inv in inventory_offerings:
                                consumer_type = inv.get('consumerType', '')
                                available_count = int(inv.get('availableCount', 0))
                                
                                daily_data.append({
                                    'resort_id': request_info.get('resort_id', ''),
                                    'check_in': request_info.get('check_in', ''),
                                    'check_out': request_info.get('check_out', ''),
                                    'resort_name': resort_name,
                                    'date': date,
                                    'unit_name': unit_name,
                                    'consumer_type': consumer_type,
                                    'available_count': available_count
                                })
        
        return pd.DataFrame(daily_data)
        
    except Exception as e:
        print(f"Error extracting daily breakdown from {json_file_path}: {e}")
        return pd.DataFrame()

def process_all_api_results():
    """Process all JSON files in api_results folder"""
    
    # Get all JSON files from api_results folder
    json_files = glob.glob("api_results/*.json")
    
    if not json_files:
        print("❌ No JSON files found in api_results folder")
        return
    
    print(f"🔍 Found {len(json_files)} JSON files to process...")
    
    all_detailed_data = []
    all_daily_data = []
    processed_count = 0
    success_count = 0
    
    for json_file in json_files:
        try:
            print(f"📁 Processing: {os.path.basename(json_file)}")
            
            # Extract detailed availability
            detailed_df = extract_detailed_availability_from_json(json_file)
            if not detailed_df.empty:
                all_detailed_data.append(detailed_df)
                success_count += 1
                print(f"   ✅ Found {len(detailed_df)} accommodation options")
            else:
                print(f"   ⚠️ No availability data found")
            
            # Extract daily breakdown
            daily_df = extract_daily_breakdown_from_json(json_file)
            if not daily_df.empty:
                all_daily_data.append(daily_df)
            
            processed_count += 1
            
        except Exception as e:
            print(f"   ❌ Error processing {json_file}: {e}")
            continue
    
    # Combine all data
    if all_detailed_data:
        combined_detailed = pd.concat(all_detailed_data, ignore_index=True)
        combined_daily = pd.concat(all_daily_data, ignore_index=True) if all_daily_data else pd.DataFrame()
        
        # Create output directory
        os.makedirs("extracted_results", exist_ok=True)
        
        # Save combined detailed availability
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        detailed_csv_path = f"extracted_results/all_resort_availability_{timestamp}.csv"
        combined_detailed.to_csv(detailed_csv_path, index=False)
        
        # Save combined daily breakdown
        daily_csv_path = f"extracted_results/all_daily_breakdown_{timestamp}.csv"
        if not combined_daily.empty:
            combined_daily.to_csv(daily_csv_path, index=False)
        
        # Save summary by resort
        summary_data = []
        for _, group in combined_detailed.groupby(['resort_id', 'resort_name', 'check_in', 'check_out']):
            total_units = group['available_count'].sum()
            min_points = group['current_points'].min() if len(group) > 0 else 0
            max_points = group['current_points'].max() if len(group) > 0 else 0
            unit_types = ', '.join(group['unit_type'].unique())
            
            summary_data.append({
                'resort_id': group['resort_id'].iloc[0],
                'resort_name': group['resort_name'].iloc[0],
                'check_in': group['check_in'].iloc[0],
                'check_out': group['check_out'].iloc[0],
                'total_available_units': total_units,
                'unit_types_available': unit_types,
                'min_points_required': min_points,
                'max_points_required': max_points,
                'accommodation_options': len(group)
            })
        
        summary_df = pd.DataFrame(summary_data)
        summary_csv_path = f"extracted_results/resort_summary_{timestamp}.csv"
        summary_df.to_csv(summary_csv_path, index=False)
        
        # Print results
        print(f"\n🎉 Processing Complete!")
        print(f"   📁 Processed: {processed_count} files")
        print(f"   ✅ Successful: {success_count} files with data")
        print(f"   📋 Total accommodations found: {len(combined_detailed)}")
        print(f"\n📄 Files created:")
        print(f"   📊 Detailed availability: {detailed_csv_path}")
        print(f"   📅 Daily breakdown: {daily_csv_path}")
        print(f"   📋 Summary by resort: {summary_csv_path}")
        
        # Show top resorts by availability
        if not summary_df.empty:
            print(f"\n🏆 Top Resorts by Total Available Units:")
            top_resorts = summary_df.nlargest(5, 'total_available_units')[['resort_name', 'check_in', 'check_out', 'total_available_units', 'accommodation_options']]
            print(top_resorts.to_string(index=False))
        
        return detailed_csv_path, daily_csv_path, summary_csv_path
    
    else:
        print("❌ No availability data found in any files")
        return None, None, None

def main():
    """Main function to process all API results"""
    
    print("🚀 Starting extraction of all API results...\n")
    
    # Process all files and create combined CSVs only
    detailed_path, daily_path, summary_path = process_all_api_results()
    
    if detailed_path:
        print(f"\n✅ All extraction completed!")
        print(f"📂 Check the 'extracted_results' folder for CSV files")
    else:
        print("❌ No data extracted")

if __name__ == "__main__":
    main()