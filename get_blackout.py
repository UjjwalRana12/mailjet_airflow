import sys
import os
import pandas as pd
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), 'database'))
from db import SqlDatabaseConnection

def create_csv_folder():
    """Create csv folder if it doesn't exist"""
    csv_folder = os.path.join(os.path.dirname(__file__), 'csv')
    os.makedirs(csv_folder, exist_ok=True)
    return csv_folder

def save_query_result_to_csv(df):
    """Save the database query result to CSV file"""
    try:
        if df.empty:
            print("No data to save - query result is empty")
            return
        
        # Create CSV folder
        csv_folder = create_csv_folder()
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        query_result_file = os.path.join(csv_folder, f"blackout_scrapping_data_{timestamp}.csv")
        
        # Save to CSV
        df.to_csv(query_result_file, index=False)
        print(f"Database query result saved to: {query_result_file}")
        
        # Also save a latest version without timestamp
        latest_query_file = os.path.join("latest_blackout_scrapping_data.csv")
        df.to_csv(latest_query_file, index=False)
        print(f"Latest query result saved to: {latest_query_file}")
        
    except Exception as e:
        print(f"Error saving query result to CSV: {e}")

def show_db_fetch_summary(df):
    """Show summary of data fetched from database"""
    print("\n=== DATABASE FETCH SUMMARY ===")
    
    if df.empty:
        print("No data fetched from database")
        return
    
    # Resort-wise RunCount summary
    resort_summary = df.groupby('ResortId').agg({
        'RunCount': ['min', 'max', 'nunique'],
        'Date': ['min', 'max', 'nunique'],
        'AvailableCount': ['count', 'sum']
    }).round(2)
    
    resort_summary.columns = ['MinRunCount', 'MaxRunCount', 'UniqueRunCounts', 
                             'EarliestDate', 'LatestDate', 'UniqueDates', 
                             'TotalRecords', 'TotalAvailableUnits']
    
    print(f"Total records fetched: {len(df)}")
    print(f"Unique resorts: {df['ResortId'].nunique()}")
    print(f"Date range: {df['Date'].min()} to {df['Date'].max()}")
    print(f"Total available units: {df['AvailableCount'].sum()}")
    
    print("\nRunCount distribution:")
    runcount_dist = df['RunCount'].value_counts().sort_index()
    for runcount, count in runcount_dist.head(10).items():
        print(f"  RunCount {runcount}: {count} records")
    
    print("\nTop 10 resorts by record count:")
    resort_counts = df['ResortId'].value_counts().head(10)
    for resort_id, count in resort_counts.items():
        max_runcount = df[df['ResortId'] == resort_id]['RunCount'].max()
        print(f"  Resort {resort_id}: {count} records (Max RunCount: {max_runcount})")
    
    print("\nSample data (first 10 records):")
    sample_df = df[['ResortId', 'PropertyTypeId', 'RoomTypeId', 'Date', 'AvailableCount', 'RunCount']].head(10)
    print(sample_df.to_string(index=False))
    
    print("\n" + "="*50)

def get_all_availability_data():
    """Get today's availability data from BlackoutScrappingData with latest RunCount per resort"""
    query = """
    WITH LatestRunCounts AS (
        SELECT 
            ResortId,
            MAX(RunCount) as MaxRunCount
        FROM BlackoutScrappingData
        WHERE IsDeleted = 0
            AND CAST(CreationDate AS DATE) = CAST(GETDATE() AS DATE)
        GROUP BY ResortId
    ),
    FilteredData AS (
        SELECT 
            b.ResortId,
            b.PropertyTypeId,
            b.RoomTypeId,
            b.Studio,
            b.Bed1,
            b.Bed2,
            b.Bed3,
            b.Bed4,
            b.Date,
            b.AvailableCount,
            b.RunCount,
            b.CreationDate
        FROM BlackoutScrappingData b
        INNER JOIN LatestRunCounts l ON 
            b.ResortId = l.ResortId
            AND b.RunCount = l.MaxRunCount
        WHERE b.IsDeleted = 0
            AND CAST(b.CreationDate AS DATE) = CAST(GETDATE() AS DATE)
    )
    SELECT 
        ResortId,
        PropertyTypeId,
        RoomTypeId,
        Studio,
        Bed1,
        Bed2,
        Bed3,
        Bed4,
        Date,
        AvailableCount,
        RunCount,
        CreationDate
    FROM FilteredData
    ORDER BY ResortId, PropertyTypeId, RoomTypeId, Date
    """
    
    db_connection = SqlDatabaseConnection(use_fetching_db=False)
    
    try:
        connection = db_connection.get_connection()
        print("Fetching today's availability data with latest RunCount per resort...")
        
        df = pd.read_sql(query, connection)
        print(f"Retrieved {len(df)} availability records (today's data only)")
        
        # Save query result to CSV
        save_query_result_to_csv(df)
        
        # Show data fetched summary
        show_db_fetch_summary(df)
        
        return df
        
    except Exception as e:
        print(f"Error fetching availability data: {e}")
        return pd.DataFrame()
    
    finally:
        if db_connection.connection:
            db_connection.connection.close()

def main():
    """Main function to fetch and save blackout scrapping data"""
    print("=== BlackoutScrappingData Query Tool ===")
    
    try:
        start_time = datetime.now()
        
        # Fetch all availability data
        availability_df = get_all_availability_data()
        
        processing_time = datetime.now() - start_time
        print(f"\nQuery completed in: {processing_time}")
        
        if not availability_df.empty:
            print(f"\n=== FINAL RESULTS ===")
            print(f"Successfully fetched {len(availability_df)} records")
            print(f"Data saved to CSV files in the 'csv' folder")
            
            # Additional statistics
            print(f"\n=== AVAILABILITY STATISTICS ===")
            total_available = availability_df['AvailableCount'].sum()
            avg_available = availability_df['AvailableCount'].mean()
            records_with_availability = len(availability_df[availability_df['AvailableCount'] > 0])
            
            print(f"Total available units across all records: {total_available}")
            print(f"Average available units per record: {avg_available:.2f}")
            print(f"Records with availability > 0: {records_with_availability}/{len(availability_df)} ({(records_with_availability/len(availability_df)*100):.1f}%)")
            
        else:
            print("No data retrieved from database")
        
    except Exception as e:
        print(f"Error in main: {e}")

if __name__ == "__main__":
    main()