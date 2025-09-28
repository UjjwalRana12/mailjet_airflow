import sys
import os
import pandas as pd

# Add the database directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'database'))

from db import SqlDatabaseConnection

def execute_query():
    # SQL query to execute
    query = """
    SELECT  
        Resorts.ResortId,
        Resorts.Name AS Resort, 
        CiiRUSMappings.Studio,
        CiiRUSMappings.Bed1,
        CiiRUSMappings.Bed2,
        CiiRUSMappings.Bed3,
        CiiRUSMappings.Bed4,
        Count(Distinct Orders.OrderId) as SearchingOrderCount
    FROM Orders
    INNER JOIN CiiRUSMappings ON CiiRUSMappings.CiiRUSID = Orders.PropertyRef
    INNER JOIN PropertyTypes ON CiiRUSMappings.PropertyTypeId = PropertyTypes.PropertyTypeId
    INNER JOIN RoomTypes ON CiiRUSMappings.RoomTypeId = RoomTypes.RoomTypeId
    INNER JOIN Sources ON Sources.SourceId = Orders.SourceId
    INNER JOIN Resorts ON Resorts.ResortId = CiiRUSMappings.ResortId
    LEFT JOIN OrderBookings ON OrderBookings.OrderId = Orders.OrderId
    LEFT JOIN Bookings ON Bookings.BookingId = OrderBookings.BookingId
    INNER JOIN Statuses ON Statuses.StatusId = Orders.StatusId
    WHERE Statuses.StatusId = 34 
        AND Resorts.ResortId IN (SELECT ResortId FROM Resorts WHERE VendorId = 2)
    GROUP BY Resorts.ResortId,
        Resorts.Name, 
        CiiRUSMappings.Studio,
        CiiRUSMappings.Bed1,
        CiiRUSMappings.Bed2,
        CiiRUSMappings.Bed3,
        CiiRUSMappings.Bed4
    ORDER BY ResortId,  
        CiiRUSMappings.Studio DESC,
        CiiRUSMappings.Bed1 DESC,
        CiiRUSMappings.Bed2 DESC,
        CiiRUSMappings.Bed3 DESC,
        CiiRUSMappings.Bed4 DESC
    """
    
   
    db_connection = SqlDatabaseConnection(use_fetching_db=True)
    
    try:
        
        connection = db_connection.get_connection()
        
        if connection is None:
            print("Failed to establish database connection")
            return None
        
        print("Executing query...")
        
        # Execute query and fetch results into a pandas DataFrame
        df = pd.read_sql(query, connection)
        
        print(f"Query executed successfully. Retrieved {len(df)} rows.")
        print("\nQuery Results:")
        print("=" * 80)
        
        # Display the results
        print(df.to_string(index=False))
        
        # Optional: Save to CSV
        output_file = "query_results.csv"
        df.to_csv(output_file, index=False)
        print(f"\nResults saved to {output_file}")
        
        return df
        
    except Exception as e:
        print(f"Error executing query: {e}")
        return None
    
    finally:
        # Close the connection
        if db_connection.connection:
            db_connection.connection.close()
            print("Database connection closed.")

if __name__ == "__main__":
    execute_query()