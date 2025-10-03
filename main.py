import sys
import os
import pandas as pd

sys.path.append(os.path.join(os.path.dirname(__file__), 'database'))
from db import SqlDatabaseConnection

def create_csv_folder():
    """Create csv folder if it doesn't exist"""
    csv_folder = os.path.join(os.path.dirname(__file__), 'csv')
    os.makedirs(csv_folder, exist_ok=True)
    return csv_folder

def get_single_resort_data(resort_id):
    """Get detailed order data for a single resort"""
    query = """
    SELECT  
        Orders.OrderId,
        Orders.CreatedOn AS Dated, 
        Resorts.ResortId,
        Resorts.Name AS Resort, 
        Vendors.VendorId,
        Vendors.Name as Vendor,
        Orders.Arrival,
        Orders.Departure,
        PropertyTypes.Name AS PropertyType,
        CiiRUSMappings.PropertyTypeId,
        RoomTypes.Name AS RoomType,
        CiiRUSMappings.RoomTypeId,
        CiiRUSMappings.Studio,
        CiiRUSMappings.Bed1,
        CiiRUSMappings.Bed2,
        CiiRUSMappings.Bed3,
        CiiRUSMappings.Bed4,
        Statuses.Name AS Status,
        IsNull((
            SELECT MIN(DRA.Units)
            FROM [dbo].[DateRangeAvailabilities] DRA
            INNER JOIN DateRanges dr ON DRA.DateRangeId = dr.DateRangeId
            WHERE dr.ResortId = Resorts.ResortId 
                AND dr.PropertyTypeId = CiiRUSMappings.PropertyTypeId
                AND dr.RoomTypeId = CiiRUSMappings.RoomTypeId
                AND dr.BedroomId = CASE 
                    WHEN CiiRUSMappings.Studio = 1 THEN 1
                    WHEN CiiRUSMappings.Bed1 = 1 THEN 2  
                    WHEN CiiRUSMappings.Bed2 = 1 THEN 3
                    WHEN CiiRUSMappings.Bed3 = 1 THEN 4
                    WHEN CiiRUSMappings.Bed4 = 1 THEN 5
                    ELSE 1
                END
                AND DRA.Dated BETWEEN Orders.Arrival AND DATEADD(DAY, -1, Orders.Departure)
        ),0) AS MinUnits
    FROM Orders
    INNER JOIN CiiRUSMappings ON CiiRUSMappings.CiiRUSID = Orders.PropertyRef
    INNER JOIN PropertyTypes ON CiiRUSMappings.PropertyTypeId = PropertyTypes.PropertyTypeId
    INNER JOIN RoomTypes ON CiiRUSMappings.RoomTypeId = RoomTypes.RoomTypeId
    INNER JOIN Sources ON Sources.SourceId = Orders.SourceId
    INNER JOIN Resorts ON Resorts.ResortId = CiiRUSMappings.ResortId
    INNER JOIN Vendors ON Vendors.VendorId = Resorts.VendorId
    LEFT JOIN OrderBookings ON OrderBookings.OrderId = Orders.OrderId
    LEFT JOIN Bookings ON Bookings.BookingId = OrderBookings.BookingId
    INNER JOIN Statuses ON Statuses.StatusId = Orders.StatusId
    WHERE Statuses.StatusId = 34 
        AND Resorts.ResortId = ?
    ORDER BY Arrival
    """
    
    # Create database connection using fetching_db
    db_connection = SqlDatabaseConnection(use_fetching_db=True)
    
    try:
        connection = db_connection.get_connection()
        
        if connection is None:
            print("Failed to establish database connection")
            return None
        
        print(f"Executing query for Resort ID: {resort_id}...")
        
        # Execute query with parameter
        df = pd.read_sql(query, connection, params=[resort_id])
        
        if df.empty:
            print(f"No data found for Resort ID: {resort_id}")
            return None
        
        print(f"Query executed successfully. Retrieved {len(df)} rows for Resort ID: {resort_id}")
        
        # Create CSV folder and save file
        csv_folder = create_csv_folder()
        resort_name = df['Resort'].iloc[0].replace(' ', '_').replace('-', '_')
        output_file = os.path.join(csv_folder, f"single_resort_{resort_id}_{resort_name}.csv")
        df.to_csv(output_file, index=False)
        print(f"Results saved to {output_file}")
        
        return df
        
    except Exception as e:
        print(f"Error executing single resort query: {e}")
        return None
    
    finally:
        if db_connection.connection:
            db_connection.connection.close()

def get_all_resorts_data():
    """Get detailed order data for all resorts"""
    query = """
    SELECT  
        Orders.OrderId,
        Orders.CreatedOn AS Dated, 
        Resorts.ResortId,
        Resorts.Name AS Resort, 
        Vendors.VendorId,
        Vendors.Name as Vendor,
        Orders.Arrival,
        Orders.Departure,
        PropertyTypes.Name AS PropertyType,
        CiiRUSMappings.PropertyTypeId,
        RoomTypes.Name AS RoomType,
        CiiRUSMappings.RoomTypeId,
        CiiRUSMappings.Studio,
        CiiRUSMappings.Bed1,
        CiiRUSMappings.Bed2,
        CiiRUSMappings.Bed3,
        CiiRUSMappings.Bed4,
        Statuses.Name AS Status,
        IsNull((
            SELECT MIN(DRA.Units)
            FROM [dbo].[DateRangeAvailabilities] DRA
            INNER JOIN DateRanges dr ON DRA.DateRangeId = dr.DateRangeId
            WHERE dr.ResortId = Resorts.ResortId 
                AND dr.PropertyTypeId = CiiRUSMappings.PropertyTypeId
                AND dr.RoomTypeId = CiiRUSMappings.RoomTypeId
                AND dr.BedroomId = CASE 
                    WHEN CiiRUSMappings.Studio = 1 THEN 1
                    WHEN CiiRUSMappings.Bed1 = 1 THEN 2  
                    WHEN CiiRUSMappings.Bed2 = 1 THEN 3
                    WHEN CiiRUSMappings.Bed3 = 1 THEN 4
                    WHEN CiiRUSMappings.Bed4 = 1 THEN 5
                    ELSE 1
                END
                AND DRA.Dated BETWEEN Orders.Arrival AND DATEADD(DAY, -1, Orders.Departure)
        ),0) AS MinUnits
    FROM Orders
    INNER JOIN CiiRUSMappings ON CiiRUSMappings.CiiRUSID = Orders.PropertyRef
    INNER JOIN PropertyTypes ON CiiRUSMappings.PropertyTypeId = PropertyTypes.PropertyTypeId
    INNER JOIN RoomTypes ON CiiRUSMappings.RoomTypeId = RoomTypes.RoomTypeId
    INNER JOIN Sources ON Sources.SourceId = Orders.SourceId
    INNER JOIN Resorts ON Resorts.ResortId = CiiRUSMappings.ResortId
    INNER JOIN Vendors ON Vendors.VendorId = Resorts.VendorId
    LEFT JOIN OrderBookings ON OrderBookings.OrderId = Orders.OrderId
    LEFT JOIN Bookings ON Bookings.BookingId = OrderBookings.BookingId
    INNER JOIN Statuses ON Statuses.StatusId = Orders.StatusId
    WHERE Statuses.StatusId = 34 
        AND Resorts.ResortId IN (SELECT ResortId FROM Resorts WHERE VendorId = 2)
    ORDER BY Resorts.ResortId, Arrival
    """
    
    # Create database connection using fetching_db
    db_connection = SqlDatabaseConnection(use_fetching_db=True)
    
    try:
        connection = db_connection.get_connection()
        
        if connection is None:
            print("Failed to establish database connection")
            return None
        
        print("Executing query for all resorts...")
        
        # Execute query
        df = pd.read_sql(query, connection)
        
        if df.empty:
            print("No data found for any resorts")
            return None
        
        print(f"Query executed successfully. Retrieved {len(df)} rows for all resorts")
        
        # Display summary by resort
        resort_summary = df.groupby(['ResortId', 'Resort']).size().reset_index()
        resort_summary.columns = ['ResortId', 'Resort', 'OrderCount']
        print("\nSummary by Resort:")
        print(resort_summary.to_string(index=False))
        
        # Create CSV folder and save file
        csv_folder = create_csv_folder()
        output_file = os.path.join(csv_folder, "all_resorts_detailed_orders.csv")
        df.to_csv(output_file, index=False)
        print(f"\nResults saved to {output_file}")
        
        return df
        
    except Exception as e:
        print(f"Error executing all resorts query: {e}")
        return None
    
    finally:
        if db_connection.connection:
            db_connection.connection.close()

def main():
    """Main function - automatically runs both queries"""
    print("=== Resort Data Query Tool ===")
    print("Automatically running both queries...\n")
    
    try:
        # Query 1: Single Resort Data (Flagstaff - Resort ID: 109)
        print("1. Running Single Resort Query (Flagstaff - ID: 109)")
        print("-" * 60)
        single_resort_df = get_single_resort_data(109)
        
        print("\n" + "=" * 60 + "\n")
        
        # Query 2: All Resorts Data
        print("2. Running All Resorts Query")
        print("-" * 60)
        all_resorts_df = get_all_resorts_data()
        
        print("\n" + "=" * 60)
        print("EXECUTION SUMMARY:")
        print("=" * 60)
        
        if single_resort_df is not None:
            print(f"✓ Single Resort Query: SUCCESS - {len(single_resort_df)} rows")
        else:
            print("✗ Single Resort Query: FAILED")
            
        if all_resorts_df is not None:
            print(f"✓ All Resorts Query: SUCCESS - {len(all_resorts_df)} rows")
        else:
            print("✗ All Resorts Query: FAILED")
        
        print("\nBoth CSV files have been created in the 'csv' folder.")
        
    except Exception as e:
        print(f"Error in main: {e}")

if __name__ == "__main__":
    main()
