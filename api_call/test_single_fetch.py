from fetch_data import WyndhamAPIClient

def test_single_resort():
    client = WyndhamAPIClient()
    
    
    result = client.fetch_resort_availability(
        product_id="PI|R000000000067",  
        check_in_date="2025-11-04",
        check_out_date="2025-11-08",
        
    )
    
    if result:
        print("✅ Single resort test successful!")
        print(f"Available resorts: {result.get('availableResorts', {}).get('count', '0')}")
    else:
        print("❌ Single resort test failed!")

if __name__ == "__main__":
    test_single_resort()