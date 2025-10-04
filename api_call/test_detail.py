import json
import pandas as pd
from datetime import datetime

def extract_detailed_availability_from_json(json_file_path):
    """
    Extract detailed availability data from JSON response file using actual counts
    """
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        detailed_data = []
        resorts = data.get('resorts', [])
        
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
                            # (since different consumer types can access the same inventory)
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
                            'check_in': '2025-11-04',
                            'check_out': '2025-11-08',
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
                            'extraction_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        })
        
        return pd.DataFrame(detailed_data)
        
    except Exception as e:
        print(f"Error extracting detailed data from JSON: {e}")
        return pd.DataFrame()

def extract_daily_breakdown(json_file_path):
    """Extract day-by-day breakdown showing the logic"""
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        daily_data = []
        resorts = data.get('resorts', [])
        
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
                            
                            # Show each consumer type separately
                            for inv in inventory_offerings:
                                consumer_type = inv.get('consumerType', '')
                                available_count = int(inv.get('availableCount', 0))
                                
                                daily_data.append({
                                    'check_in': '2025-11-04',
                                    'check_out': '2025-11-08',
                                    'date': date,
                                    'unit_name': unit_name,
                                    'consumer_type': consumer_type,
                                    'available_count': available_count
                                })
        
        return pd.DataFrame(daily_data)
        
    except Exception as e:
        print(f"Error extracting daily breakdown: {e}")
        return pd.DataFrame()

def main():
    """Main function to extract and save availability data"""
    
    json_file_path = "test_results/api_test_20251004_225613.json"
    
    print("🔍 Extracting CORRECTED availability data from JSON file...\n")
    
    # Show daily breakdown first to understand the data
    df_daily = extract_daily_breakdown(json_file_path)
    if not df_daily.empty:
        print("📅 Daily Breakdown by Consumer Type:")
        print("=" * 80)
        print(f"Check-in: {df_daily['check_in'].iloc[0]} | Check-out: {df_daily['check_out'].iloc[0]}")
        print("=" * 80)
        
        for unit in df_daily['unit_name'].unique():
            unit_data = df_daily[df_daily['unit_name'] == unit]
            print(f"\n📦 {unit}")
            print(unit_data[['date', 'consumer_type', 'available_count']].to_string(index=False))
            
            # Show the logic for calculating minimum
            print(f"\n🧮 Calculation Logic for {unit}:")
            for date in unit_data['date'].unique():
                date_data = unit_data[unit_data['date'] == date]
                max_for_date = date_data['available_count'].max()
                print(f"   {date}: max({', '.join(map(str, date_data['available_count'].tolist()))}) = {max_for_date}")
            
            min_across_dates = unit_data.groupby('date')['available_count'].max().min()
            print(f"   Final Available Count: {min_across_dates} (minimum across all dates)")
        print("=" * 80)
    
    # Extract corrected detailed availability
    df_detailed = extract_detailed_availability_from_json(json_file_path)
    
    if not df_detailed.empty:
        detailed_csv_path = "results/corrected_availability_with_counts.csv"
        df_detailed.to_csv(detailed_csv_path, index=False)
        print(f"\n✅ CORRECTED availability data saved to: {detailed_csv_path}")
        print("\n📋 Corrected Summary:")
        print(f"Check-in: {df_detailed['check_in'].iloc[0]} | Check-out: {df_detailed['check_out'].iloc[0]}")
        print("-" * 50)
        for _, row in df_detailed.iterrows():
            print(f"   📦 {row['unit_name']}")
            print(f"      Available: {row['available_count']} units")
            print(f"      Points: {row['current_points']:,} PTS (was {row['original_points']:,})")
            print(f"      Savings: {row['savings']:,} PTS ({row['discount_percentage']}% off)")
            print()
        
        # Save daily breakdown too
        daily_csv_path = "results/daily_breakdown_with_checkin_checkout.csv"
        df_daily.to_csv(daily_csv_path, index=False)
        print(f"✅ Daily breakdown saved to: {daily_csv_path}")

if __name__ == "__main__":
    import os
    os.makedirs("results", exist_ok=True)
    main()