import sys
import os
import pandas as pd
sys.path.append(os.path.join(os.path.dirname(__file__), 'database'))
from db import SqlDatabaseConnection

def create_csv_folder():
    """Create csv folder if it doesn't exist"""
    csv_folder = os.path.join(os.path.dirname(__file__), 'test_css')
    os.makedirs(csv_folder, exist_ok=True)
    return csv_folder

def get_all_resorts_data_simple():
    """Get detailed order data for all resorts - simple query without availability"""
    query = """
    SELECT  
        DISTINCT
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
        Statuses.Name AS Status
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
    ORDER BY Arrival
    """
    
    db_connection = SqlDatabaseConnection(use_fetching_db=True)
    
    try:
        connection = db_connection.get_connection()
        
        if connection is None:
            print("Failed to establish database connection")
            return None
        
        print("Executing simple query for all resorts...")
        
        df = pd.read_sql(query, connection)
        
        if df.empty:
            print("No data found for any resorts")
            return None
        
        print(f"Query executed successfully. Retrieved {len(df)} rows for all resorts")
        
        resort_summary = df.groupby(['ResortId', 'Resort']).size().reset_index()
        resort_summary.columns = ['ResortId', 'Resort', 'OrderCount']
        print("\nSummary by Resort:")
        print(resort_summary.to_string(index=False))
        
        csv_folder = create_csv_folder()
        output_file = os.path.join(csv_folder, "all_resorts_simple_orders.csv")
        df.to_csv(output_file, index=False)
        print(f"\nResults saved to {output_file}")
        
        return df
        
    except Exception as e:
        print(f"Error executing query: {e}")
        return None
    
    finally:
        if db_connection.connection:
            db_connection.connection.close()

def main():
    """Main function"""
    print("=== Resort Data Query Tool ===")
    print("Running simple query...\n")
    
    try:
        print("Running All Resorts Simple Query")
        print("-" * 60)
        all_resorts_df = get_all_resorts_data_simple()
        
        print("\n" + "=" * 60)
        print("EXECUTION SUMMARY:")
        print("=" * 60)
        
        if all_resorts_df is not None:
            print(f"Query SUCCESS - {len(all_resorts_df)} rows")
        else:
            print("Query FAILED")
        
        print("\nCSV file has been created in the csv folder.")
        
    except Exception as e:
        print(f"Error in main: {e}")

if __name__ == "__main__":
    main()
