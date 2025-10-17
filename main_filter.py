import pandas as pd
import os
from datetime import datetime, timedelta

def create_data_folder():
    """Create data folder if it doesn't exist"""
    data_folder = os.path.join(os.path.dirname(__file__), 'data')
    os.makedirs(data_folder, exist_ok=True)
    return data_folder

def calculate_minimum_availability():
    """
    Calculate minimum available count for each order across their entire stay period
    """
    try:
        data_folder = create_data_folder()
        mapped_file = os.path.join(data_folder, 'mapped_orders_blackout_data.csv')
        
        # Read the mapped data
        mapped_df = pd.read_csv(mapped_file)
        
        if mapped_df.empty:
            print("No mapped data found. Please run map_orders_to_blackout_data() first.")
            return pd.DataFrame()
        
        print(f"Calculating minimum availability for {mapped_df['OrderId'].nunique()} unique orders...")
        
        # First, get additional data from the original orders file
        orders_df = pd.read_csv('all_resorts_simple_orders.csv')
        
        # Clean PropertyType column - handle N/A, None, null values
        orders_df['PropertyType'] = orders_df['PropertyType'].fillna('None')
        orders_df['PropertyType'] = orders_df['PropertyType'].replace(['N/A', 'n/a', 'NA', 'na', '', ' '], 'None')
        orders_df['PropertyType'] = orders_df['PropertyType'].astype(str).replace(['nan', 'NaN', 'null', 'NULL'], 'None')
        
        # Create a mapping of OrderId to RoomType, PropertyType, and Vendor
        order_details = orders_df[['OrderId', 'RoomType', 'PropertyType', 'Vendor']].drop_duplicates()
        
        # Group by order and calculate minimum availability across all dates
        min_availability = mapped_df.groupby(['OrderId', 'Resort', 'BedType', 'Arrival', 'Departure']).agg({
            'AvailableCount': 'min'
        }).round(2)
        
        # Flatten column names
        min_availability.columns = ['Min_Available']
        min_availability = min_availability.reset_index()
        
        # Merge with order details to get RoomType, PropertyType, and Vendor
        min_availability = min_availability.merge(order_details, on='OrderId', how='left')
        
        # Clean PropertyType column in the final result as well
        min_availability['PropertyType'] = min_availability['PropertyType'].fillna('None')
        min_availability['PropertyType'] = min_availability['PropertyType'].replace(['N/A', 'n/a', 'NA', 'na', '', ' '], 'None')
        min_availability['PropertyType'] = min_availability['PropertyType'].astype(str).replace(['nan', 'NaN', 'null', 'NULL'], 'None')
        
        # Reorder columns to match requested format - ONLY the essential columns
        column_order = [
            'Resort', 'BedType', 'Arrival', 'Departure', 'Min_Available',
            'RoomType', 'PropertyType', 'Vendor'
        ]
        
        # Ensure all columns exist before reordering
        existing_columns = [col for col in column_order if col in min_availability.columns]
        min_availability = min_availability[existing_columns]
        
        # Sort by Resort → BedType → Min_Available (ascending to show problematic ones first)
        min_availability = min_availability.sort_values(
            ['Resort', 'BedType', 'Min_Available'], 
            ascending=[True, True, True]
        ).reset_index(drop=True)
        
        # Save to CSV
        min_availability_file = os.path.join(data_folder, 'minimum_availability_per_order.csv')
        min_availability.to_csv(min_availability_file, index=False)
        
        # Display summary
        print(f"\nMinimum Availability Analysis Completed!")
        print(f"Total orders analyzed: {len(min_availability)}")
        
        # Show availability statistics
        available_orders = len(min_availability[min_availability['Min_Available'] > 0])
        not_available_orders = len(min_availability[min_availability['Min_Available'] == 0])
        
        print(f"\nAvailability Summary:")
        print(f"Orders with availability (Min > 0): {available_orders}")
        print(f"Orders with no availability (Min = 0): {not_available_orders}")
        print(f"Success rate: {(available_orders/len(min_availability)*100):.1f}%")
        
        # Show PropertyType distribution
        print(f"\nPropertyType Distribution:")
        property_summary = min_availability['PropertyType'].value_counts()
        for prop_type, count in property_summary.items():
            print(f"{prop_type}: {count} orders")
        
        # Show sample results with simplified format
        print(f"\nSample Results (sorted by Resort → Bed Type → Min Available):")
        print(min_availability.head(15).to_string(index=False))
        
        # Show problematic orders (Min = 0)
        problematic_orders = min_availability[min_availability['Min_Available'] == 0]
        if not problematic_orders.empty:
            print(f"\nProblematic Orders (No Availability):")
            print(problematic_orders.head(10).to_string(index=False))
        
        # Show best available orders
        best_orders = min_availability[min_availability['Min_Available'] > 0].head(10)
        if not best_orders.empty:
            print(f"\nBest Available Orders:")
            print(best_orders.to_string(index=False))
        
        # Resort-wise minimum availability summary
        resort_min_summary = min_availability.groupby('Resort').agg({
            'Min_Available': ['count', 'mean'],
            'Vendor': lambda x: x.iloc[0]  # Get first vendor (should be same for all - Wyndham)
        }).round(2)
        resort_min_summary.columns = ['Orders_Count', 'Avg_Min_Available', 'Vendor']
        resort_min_summary = resort_min_summary.reset_index().sort_values('Resort')
        
        print(f"\nResort-wise Minimum Availability Summary:")
        print(resort_min_summary.to_string(index=False))
        
        # Save resort summary
        resort_min_file = os.path.join(data_folder, 'resort_minimum_availability_summary.csv')
        resort_min_summary.to_csv(resort_min_file, index=False)
        
        # Show vendor summary (should all be Wyndham)
        print(f"\nVendor Summary:")
        vendor_summary = min_availability['Vendor'].value_counts()
        for vendor, count in vendor_summary.items():
            print(f"{vendor}: {count} orders")
        
        return min_availability
        
    except FileNotFoundError:
        print("Mapped data file not found. Please run map_orders_to_blackout_data() first.")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error calculating minimum availability: {e}")
        return pd.DataFrame()

def map_orders_to_blackout_data():
    """
    Map orders data to blackout scraping data and show search counts
    """
    
    # Read the CSV files
    orders_df = pd.read_csv('all_resorts_simple_orders.csv')
    blackout_df = pd.read_csv('latest_blackout_scrapping_data.csv')
    
    print(f"Loaded {len(orders_df)} orders and {len(blackout_df)} blackout records")
    
    # Clean PropertyType column in orders_df
    orders_df['PropertyType'] = orders_df['PropertyType'].fillna('None')
    orders_df['PropertyType'] = orders_df['PropertyType'].replace(['N/A', 'n/a', 'NA', 'na', '', ' '], 'None')
    orders_df['PropertyType'] = orders_df['PropertyType'].astype(str).replace(['nan', 'NaN', 'null', 'NULL'], 'None')
    
    # Convert date columns to datetime
    orders_df['Arrival'] = pd.to_datetime(orders_df['Arrival'])
    orders_df['Departure'] = pd.to_datetime(orders_df['Departure'])
    blackout_df['Date'] = pd.to_datetime(blackout_df['Date'])
    
    # Convert boolean columns in blackout_df to actual boolean type
    bool_columns = ['Studio', 'Bed1', 'Bed2', 'Bed3', 'Bed4']
    for col in bool_columns:
        if col in blackout_df.columns:
            blackout_df[col] = blackout_df[col].astype(bool)
    
    # Prepare results list
    results = []
    
    # Process each order
    for idx, order in orders_df.iterrows():
        # Calculate date range (arrival to departure-1) - FIXED LOGIC
        start_date = order['Arrival']
        end_date = order['Departure'] - timedelta(days=1)
        
        # Debug print for first few orders
        if idx < 3:
            print(f"Order {order['OrderId']}: Arrival={start_date.date()}, Departure={order['Departure'].date()}, End={end_date.date()}")
        
        # Only process if end_date is >= start_date
        if end_date >= start_date:
            # Create date range
            date_range = pd.date_range(start=start_date, end=end_date, freq='D')
            
            # Extract bed type information
            bed_columns = ['Studio', 'Bed1', 'Bed2', 'Bed3', 'Bed4']
            bed_type = None
            bed_value = None
            
            for col in bed_columns:
                if order[col] == True or str(order[col]).lower() == 'true':
                    bed_type = col
                    bed_value = True
                    break
            
            # Debug print for first order
            if idx == 0:
                print(f"First order bed type: {bed_type}")
                print(f"Date range: {len(date_range)} days from {date_range[0].date()} to {date_range[-1].date()}")
            
            # For each date in range, find matching blackout data
            for single_date in date_range:
                # Filter blackout data for matching criteria
                matching_blackout = blackout_df[
                    (blackout_df['ResortId'] == order['ResortId']) &
                    (blackout_df['PropertyTypeId'] == order['PropertyTypeId']) &
                    (blackout_df['Date'] == single_date)
                ]
                
                # If bed type is specified, filter by bed type
                if bed_type and not matching_blackout.empty:
                    matching_blackout = matching_blackout[matching_blackout[bed_type] == True]
                
                # Get available count (search count)
                available_count = matching_blackout['AvailableCount'].sum() if not matching_blackout.empty else 0
                
                # Debug print for first order's first few dates
                if idx == 0 and len(results) < 3:
                    print(f"Date {single_date.date()}: Found {len(matching_blackout)} matching records, Available: {available_count}")
                
                # Add result
                results.append({
                    'OrderId': order['OrderId'],
                    'ResortId': order['ResortId'],
                    'Resort': order['Resort'],
                    'PropertyTypeId': order['PropertyTypeId'],
                    'RoomTypeId': order['RoomTypeId'],
                    'BedType': bed_type,
                    'Date': single_date.strftime('%Y-%m-%d'),
                    'Arrival': order['Arrival'].strftime('%Y-%m-%d'),
                    'Departure': order['Departure'].strftime('%Y-%m-%d'),
                    'AvailableCount': available_count,
                    'Status': order['Status']
                })
        else:
            print(f"Skipping order {order['OrderId']}: Invalid date range (departure before or same as arrival)")
    
    # Convert results to DataFrame
    results_df = pd.DataFrame(results)
    
    print(f"Generated {len(results)} result records")
    
    # Only proceed if we have results
    if len(results_df) == 0:
        print("No results generated - check date ranges and data matching")
        return pd.DataFrame(), pd.DataFrame()
    
    # SORT BY RESORT NAME → BED TYPE → DATE → ORDER ID
    results_df = results_df.sort_values(['Resort', 'BedType', 'Date', 'OrderId'], ascending=[True, True, True, True])
    results_df = results_df.reset_index(drop=True)
    
    # Create data folder and save to CSV
    data_folder = create_data_folder()
    mapped_file = os.path.join(data_folder, 'mapped_orders_blackout_data.csv')
    results_df.to_csv(mapped_file, index=False)
    
    # Display summary
    print("Mapping completed successfully!")
    print(f"Total records processed: {len(results_df)}")
    print(f"Unique orders: {results_df['OrderId'].nunique()}")
    print(f"Unique resorts: {results_df['Resort'].nunique()}")
    print(f"Date range coverage: {results_df['Date'].min()} to {results_df['Date'].max()}")
    
    # Show sample results SORTED BY RESORT → BED TYPE → DATE
    print("\nSample results (sorted by Resort → Bed Type → Date):")
    sample_results = results_df[['Resort', 'BedType', 'Date', 'OrderId', 'AvailableCount']].head(15)
    print(sample_results.to_string(index=False))
    
    # Show summary statistics
    print("\nSummary Statistics:")
    print(f"Average Available Count: {results_df['AvailableCount'].mean():.2f}")
    print(f"Total Available Count: {results_df['AvailableCount'].sum()}")
    print(f"Zero availability dates: {(results_df['AvailableCount'] == 0).sum()}")
    
    # Group by order to show availability summary per order - SORTED BY RESORT → BED TYPE
    order_summary = results_df.groupby(['OrderId', 'Resort', 'BedType']).agg({
        'AvailableCount': ['sum', 'mean', 'min', 'max'],
        'Date': 'count'
    }).round(2)
    
    order_summary.columns = ['Total_Available', 'Avg_Available', 'Min_Available', 'Max_Available', 'Days_Checked']
    order_summary = order_summary.reset_index()
    
    # Sort order summary by resort name then bed type
    order_summary = order_summary.sort_values(['Resort', 'BedType', 'OrderId'], ascending=[True, True, True])
    order_summary = order_summary.reset_index(drop=True)
    
    print("\nAvailability Summary by Order (sorted by Resort → Bed Type):")
    print(order_summary.head(15).to_string(index=False))
    
    # Show resort and bed type combination summary
    print("\nResort + Bed Type Summary (alphabetical order):")
    resort_bed_summary = results_df.groupby(['Resort', 'BedType']).agg({
        'OrderId': 'nunique',
        'Date': 'count',
        'AvailableCount': ['sum', 'mean']
    }).round(2)
    resort_bed_summary.columns = ['Unique_Orders', 'Total_Date_Records', 'Total_Available', 'Avg_Available']
    resort_bed_summary = resort_bed_summary.reset_index()
    print(resort_bed_summary.to_string(index=False))
    
    # Show overall resort summary
    print("\nOverall Resort Summary (alphabetical order):")
    resort_summary = results_df.groupby('Resort').agg({
        'OrderId': 'nunique',
        'BedType': 'nunique',
        'Date': 'count',
        'AvailableCount': ['sum', 'mean']
    }).round(2)
    resort_summary.columns = ['Unique_Orders', 'Unique_BedTypes', 'Total_Date_Records', 'Total_Available', 'Avg_Available']
    resort_summary = resort_summary.reset_index().sort_values('Resort')
    print(resort_summary.to_string(index=False))
    
    # Save order summary to data folder
    summary_file = os.path.join(data_folder, 'order_availability_summary.csv')
    order_summary.to_csv(summary_file, index=False)
    
    # Save resort + bed type summary
    resort_bed_file = os.path.join(data_folder, 'resort_bedtype_summary.csv')
    resort_bed_summary.to_csv(resort_bed_file, index=False)
    
    return results_df, order_summary

def analyze_by_resort():
    """
    Additional analysis by resort
    """
    try:
        data_folder = create_data_folder()
        mapped_file = os.path.join(data_folder, 'mapped_orders_blackout_data.csv')
        results_df = pd.read_csv(mapped_file)
        
        if results_df.empty:
            print("No data found in mapped results file")
            return None
        
        # Resort-wise analysis - SORTED BY RESORT NAME
        resort_analysis = results_df.groupby(['ResortId', 'Resort']).agg({
            'AvailableCount': ['sum', 'mean', 'count'],
            'OrderId': 'nunique',
            'BedType': 'nunique'
        }).round(2)
        
        resort_analysis.columns = ['Total_Available', 'Avg_Available', 'Total_Days', 'Unique_Orders', 'Unique_BedTypes']
        resort_analysis = resort_analysis.reset_index()
        
        # Sort by resort name alphabetically
        resort_analysis = resort_analysis.sort_values('Resort', ascending=True)
        resort_analysis = resort_analysis.reset_index(drop=True)
        
        print("\nResort-wise Availability Analysis (alphabetical order):")
        print(resort_analysis.to_string(index=False))
        
        # Show bed type distribution across resorts
        print("\nBed Type Distribution by Resort:")
        bed_type_analysis = results_df.groupby(['Resort', 'BedType']).agg({
            'AvailableCount': ['sum', 'count'],
            'OrderId': 'nunique'
        }).round(2)
        bed_type_analysis.columns = ['Total_Available', 'Total_Records', 'Unique_Orders']
        bed_type_analysis = bed_type_analysis.reset_index()
        print(bed_type_analysis.to_string(index=False))
        
        # Also show top resorts by availability
        print("\nTop 10 Resorts by Total Availability:")
        top_resorts = resort_analysis.sort_values('Total_Available', ascending=False).head(10)
        try:
            print(top_resorts[['Resort', 'Total_Available', 'Avg_Available', 'Unique_Orders', 'Unique_BedTypes']].to_string(index=False))
        except Exception as e:
            print(f"Error displaying top resorts: {e}")
        
        # Save resort analysis to data folder
        analysis_file = os.path.join(data_folder, 'resort_availability_analysis.csv')
        resort_analysis.to_csv(analysis_file, index=False)
        
        # Save bed type analysis
        bed_type_file = os.path.join(data_folder, 'bed_type_analysis.csv')
        bed_type_analysis.to_csv(bed_type_file, index=False)
        
        return resort_analysis
    except FileNotFoundError:
        print("Please run map_orders_to_blackout_data() first to generate the mapped data.")
        return None

def calculate_overall_minimum_availability():
    """
    Calculate the absolute minimum available count across ALL orders for each Resort + Bed Type combination
    """
    try:
        data_folder = create_data_folder()
        mapped_file = os.path.join(data_folder, 'mapped_orders_blackout_data.csv')
        
        # Read the mapped data
        mapped_df = pd.read_csv(mapped_file)
        
        if mapped_df.empty:
            print("No mapped data found. Please run map_orders_to_blackout_data() first.")
            return pd.DataFrame()
        
        print(f"Calculating overall minimum availability across ALL orders...")
        print(f"Total records to analyze: {len(mapped_df)}")
        
        # Group by Resort + BedType and find absolute minimum across ALL dates/orders
        overall_min = mapped_df.groupby(['Resort', 'BedType']).agg({
            'AvailableCount': ['min', 'max', 'mean', 'count'],
            'OrderId': 'nunique',
            'Date': ['nunique', 'min', 'max']
        }).round(2)
        
        # Flatten column names
        overall_min.columns = [
            'Absolute_Min_Available', 'Max_Available', 'Avg_Available', 'Total_Records',
            'Unique_Orders', 'Unique_Dates', 'Earliest_Date', 'Latest_Date'
        ]
        overall_min = overall_min.reset_index()
        
        # Add availability status based on absolute minimum
        overall_min['Overall_Status'] = overall_min['Absolute_Min_Available'].apply(
            lambda x: 'Available' if x > 0 else 'Not Available'
        )
        
        overall_min['Risk_Level'] = overall_min['Absolute_Min_Available'].apply(
            lambda x: 'Critical' if x == 0 
                     else 'High Risk' if x <= 1
                     else 'Medium Risk' if x <= 3
                     else 'Low Risk'
        )
        
        # Sort by Resort → BedType → Absolute_Min_Available (ascending to show most critical first)
        overall_min = overall_min.sort_values(
            ['Resort', 'BedType', 'Absolute_Min_Available'], 
            ascending=[True, True, True]
        ).reset_index(drop=True)
        
        # Save to CSV
        overall_min_file = os.path.join(data_folder, 'overall_minimum_availability.csv')
        overall_min.to_csv(overall_min_file, index=False)
        
        # Display summary
        print(f"\nOverall Minimum Availability Analysis Completed!")
        print(f"Total Resort + Bed Type combinations analyzed: {len(overall_min)}")
        
        # Show availability statistics
        available_combinations = len(overall_min[overall_min['Absolute_Min_Available'] > 0])
        not_available_combinations = len(overall_min[overall_min['Absolute_Min_Available'] == 0])
        
        print(f"\nOverall Availability Summary:")
        print(f"Resort+BedType combinations with availability (Min > 0): {available_combinations}")
        print(f"Resort+BedType combinations with NO availability (Min = 0): {not_available_combinations}")
        print(f"Overall success rate: {(available_combinations/len(overall_min)*100):.1f}%")
        
        # Show risk distribution
        print(f"\nRisk Level Distribution:")
        risk_summary = overall_min['Risk_Level'].value_counts()
        for risk, count in risk_summary.items():
            print(f"{risk}: {count} combinations ({count/len(overall_min)*100:.1f}%)")
        
        # Show all results sorted by absolute minimum
        print(f"\nAll Results (sorted by Resort → Bed Type → Absolute Minimum):")
        display_cols = ['Resort', 'BedType', 'Absolute_Min_Available', 'Max_Available', 
                       'Avg_Available', 'Unique_Orders', 'Unique_Dates', 'Overall_Status', 'Risk_Level']
        print(overall_min[display_cols].to_string(index=False))
        
        # Show critical combinations (Min = 0)
        critical_combinations = overall_min[overall_min['Absolute_Min_Available'] == 0]
        if not critical_combinations.empty:
            print(f"\nCRITICAL: Resort+BedType combinations with ZERO availability:")
            print(critical_combinations[display_cols].to_string(index=False))
        
        # Show best combinations (highest minimum)
        best_combinations = overall_min[overall_min['Absolute_Min_Available'] > 0].sort_values(
            'Absolute_Min_Available', ascending=False
        ).head(10)
        if not best_combinations.empty:
            print(f"\nBEST: Top 10 Resort+BedType combinations with highest minimum availability:")
            print(best_combinations[display_cols].to_string(index=False))
        
        # Resort-wise summary (minimum of minimums per resort)
        resort_overall_summary = overall_min.groupby('Resort').agg({
            'Absolute_Min_Available': ['min', 'max', 'mean'],
            'BedType': 'count',
            'Unique_Orders': 'sum'
        }).round(2)
        resort_overall_summary.columns = ['Resort_Min_of_Mins', 'Resort_Max_Min', 'Resort_Avg_Min', 'BedType_Count', 'Total_Orders']
        resort_overall_summary = resort_overall_summary.reset_index().sort_values('Resort_Min_of_Mins')
        
        print(f"\nResort-wise Overall Summary (sorted by minimum of minimums):")
        print(resort_overall_summary.to_string(index=False))
        
        # Save resort overall summary
        resort_overall_file = os.path.join(data_folder, 'resort_overall_minimum_summary.csv')
        resort_overall_summary.to_csv(resort_overall_file, index=False)
        
        # Show specific examples of what this minimum represents
        print(f"\n" + "="*80)
        print("EXPLANATION OF RESULTS:")
        print("="*80)
        print("The 'Absolute_Min_Available' shows the LOWEST availability count found")
        print("across ALL arrival/departure date combinations for each Resort + Bed Type.")
        print("This represents the absolute bottleneck for that combination.")
        print("="*80)
        
        return overall_min
        
    except FileNotFoundError:
        print("Mapped data file not found. Please run map_orders_to_blackout_data() first.")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error calculating overall minimum availability: {e}")
        return pd.DataFrame()

# Update the main function to include the new analysis
if __name__ == "__main__":
    # Run the mapping
    results_df, order_summary = map_orders_to_blackout_data()
    
    # Only run analysis if we have results
    if not results_df.empty:
        # Run additional analysis
        resort_analysis = analyze_by_resort()
        
        # Calculate minimum availability per order
        print("\n" + "="*60)
        print("CALCULATING MINIMUM AVAILABILITY PER ORDER...")
        print("="*60)
        minimum_availability_df = calculate_minimum_availability()
        
        # Calculate OVERALL minimum availability across ALL orders
        print("\n" + "="*60)
        print("CALCULATING OVERALL MINIMUM AVAILABILITY ACROSS ALL ORDERS...")
        print("="*60)
        overall_minimum_df = calculate_overall_minimum_availability()
        
        print("\nFiles generated in 'data' folder:")
        print("1. data/mapped_orders_blackout_data.csv - Detailed mapping (sorted by Resort → Bed Type → Date)")
        print("2. data/order_availability_summary.csv - Summary by order (sorted by Resort → Bed Type)")
        print("3. data/resort_availability_analysis.csv - Analysis by resort (alphabetical)")
        print("4. data/resort_bedtype_summary.csv - Resort + Bed Type combinations")
        print("5. data/bed_type_analysis.csv - Bed Type distribution by resort")
        print("6. data/minimum_availability_per_order.csv - Minimum availability analysis per order")
        print("7. data/resort_minimum_availability_summary.csv - Resort-wise minimum availability summary")
        print("8. data/overall_minimum_availability.csv - OVERALL minimum across ALL orders by Resort+BedType")
        print("9. data/resort_overall_minimum_summary.csv - Resort summary of overall minimums")
    else:
        print("\nNo results to analyze. Check your data files and date ranges.")