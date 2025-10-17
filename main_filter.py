import pandas as pd
import os
from datetime import datetime, timedelta

def create_data_folder():
    """Create data folder if it doesn't exist"""
    data_folder = os.path.join(os.path.dirname(__file__), 'data')
    os.makedirs(data_folder, exist_ok=True)
    return data_folder

def map_orders_to_blackout_data():
    """
    Map orders data to blackout scraping data and show search counts
    """
    
    # Read the CSV files
    orders_df = pd.read_csv('all_resorts_simple_orders.csv')
    blackout_df = pd.read_csv('latest_blackout_scrapping_data.csv')
    
    print(f"Loaded {len(orders_df)} orders and {len(blackout_df)} blackout records")
    
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
        print(top_resorts[['Resort', 'Total_Available', 'Avg_Available', 'Unique_Orders', 'Unique_BedTypes']].to_string(index=False))
        
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

if __name__ == "__main__":
    # Run the mapping
    results_df, order_summary = map_orders_to_blackout_data()
    
    # Only run analysis if we have results
    if not results_df.empty:
        # Run additional analysis
        resort_analysis = analyze_by_resort()
        
        print("\nFiles generated in 'data' folder:")
        print("1. data/mapped_orders_blackout_data.csv - Detailed mapping (sorted by Resort → Bed Type → Date)")
        print("2. data/order_availability_summary.csv - Summary by order (sorted by Resort → Bed Type)")
        print("3. data/resort_availability_analysis.csv - Analysis by resort (alphabetical)")
        print("4. data/resort_bedtype_summary.csv - Resort + Bed Type combinations")
        print("5. data/bed_type_analysis.csv - Bed Type distribution by resort")
    else:
        print("\nNo results to analyze. Check your data files and date ranges.")