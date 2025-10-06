import os
import pandas as pd
from mailjet_rest import Client
from dotenv import load_dotenv
import logging
from datetime import datetime

# Load environment variables
load_dotenv()

class MailjetEmailService:
    def __init__(self):
        """Initialize Mailjet client with credentials from .env file"""
        self.api_key = os.getenv('ApiKey')
        self.api_secret = os.getenv('ApiSecret')
        self.sender_email = os.getenv('SenderEmail')
        
        if not all([self.api_key, self.api_secret, self.sender_email]):
            raise ValueError("Missing Mailjet credentials in .env file")
        
        self.mailjet = Client(auth=(self.api_key, self.api_secret), version='v3.1')
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def process_data_file(self, file_path='csvs/all_resorts_detailed_orders.csv'):
        """Process CSV or Excel data and filter for MinUnits > 0"""
        try:
            # Determine file type and read accordingly
            if file_path.endswith('.xlsx'):
                df = pd.read_excel(file_path, sheet_name='All_Resort_Orders')
            elif file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            else:
                raise ValueError("Unsupported file format. Use .csv or .xlsx")
            
            # Filter for MinUnits > 0 (changed from > 1)
            filtered_df = df[df['MinUnits'] > 0].copy()
            
            if filtered_df.empty:
                self.logger.info("No records found with MinUnits > 0")
                return None
            
            # Create room type column based on boolean values
            def get_room_type(row):
                if row['Studio']:
                    return 'Studio'
                elif row['Bed1']:
                    return '1 Bedroom'
                elif row['Bed2']:
                    return '2 Bedroom'
                elif row['Bed3']:
                    return '3 Bedroom'
                elif row['Bed4']:
                    return '4 Bedroom'
                else:
                    return 'Unknown'
            
            # Add the room type column
            filtered_df['RoomTypeDescription'] = filtered_df.apply(get_room_type, axis=1)
            
            # Select only required columns (removed ID columns and added Vendor)
            # Changed from set {} to list []
            result_df = filtered_df[{
                'Vendor',
                'Resort', 
                'Arrival',
                'Departure',
                'PropertyType',
                'RoomType',
                'RoomTypeDescription',
                'MinUnits'
            }].copy()

            # Rename MinUnits to InventoryCount for clarity
            result_df = result_df.rename(columns={'MinUnits': 'InventoryCount'})
            
            self.logger.info(f"Found {len(result_df)} records with InventoryCount > 0")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error processing data file: {e}")
            return None

    def create_email_content(self, filtered_data):
        """Create HTML and text content for the email"""
        if filtered_data is None or filtered_data.empty:
            return None, None
        
        # Calculate summary statistics
        total_resorts = filtered_data['Resort'].nunique()
        total_records = len(filtered_data)
        avg_inventory = filtered_data['InventoryCount'].mean()
        max_inventory = filtered_data['InventoryCount'].max()
        
        # Import datetime and pytz for current time
        from datetime import datetime
        import pytz
        
        def format_date_display(date_str):
            """Format date for display - just show as is from database"""
            try:
                # Parse the date and format it nicely
                dt = pd.to_datetime(date_str)
                return dt.strftime('%Y-%m-%d')
            except:
                return str(date_str)
        
        # Create HTML content
        html_content = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #f4f4f4; padding: 20px; border-radius: 5px; }}
                .summary {{ background-color: #e8f4fd; padding: 15px; margin: 20px 0; border-radius: 5px; }}
                table {{ border-collapse: collapse; width: 100%; margin: 20px 0; font-size: 12px; }}
                th, td {{ padding: 6px; text-align: left; border: 1px solid #ddd; }}
                th {{ background-color: #4CAF50; color: white; }}
                tr:nth-child(even) {{ background-color: #f2f2f2; }}
                .inventory-high {{ background-color: #ffebee; font-weight: bold; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>Resort Inventory Alert (Inventory Count > 0)</h2>
                <p>Report generated on: {datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S (ET)')}</p>
            </div>
            
            <div class="summary">
                <h3>Summary Statistics</h3>
                <ul>
                    <li><strong>Status Filter:</strong> Searching</li>
                    <li><strong>Total Resorts:</strong> {total_resorts}</li>
                    <li><strong>Total Records:</strong> {total_records:,}</li>
                    <li><strong>Average Inventory Count:</strong> {avg_inventory:.1f}</li>
                    <li><strong>Highest Inventory Count:</strong> {max_inventory}</li>
                </ul>
            </div>
            
            <h3>Resort Details (Inventory Count > 0)</h3>
            <table>
                <thead>
                    <tr>
                        <th>Vendor Name</th>
                        <th>Resort Name</th>
                        <th>Arrival</th>
                        <th>Departure</th>
                        <th>Property Type</th>
                        <th>Room Type</th>
                        <th>Bed Type</th>
                        <th>Inventory Count</th>
                    </tr>
                </thead>
                <tbody>
        """
        
        # Add table rows
        for _, row in filtered_data.iterrows():
            inventory_class = "inventory-high" if row['InventoryCount'] > 10 else ""
            
            # Format dates - just display as they are from database
            arrival_formatted = format_date_display(row['Arrival'])
            departure_formatted = format_date_display(row['Departure'])
            
            # Handle PropertyType - show "None" instead of N/A
            property_type = row['PropertyType'] if pd.notna(row['PropertyType']) and str(row['PropertyType']).lower() != 'none' else 'None'
            
            html_content += f"""
                    <tr class="{inventory_class}">
                        <td>{row['Vendor']}</td>
                        <td>{row['Resort']}</td>
                        <td>{arrival_formatted}</td>
                        <td>{departure_formatted}</td>
                        <td>{property_type}</td>
                        <td>{row['RoomType']}</td>
                        <td>{row['RoomTypeDescription']}</td>
                        <td><strong>{row['InventoryCount']}</strong></td>
                    </tr>
            """
        
        html_content += """
                </tbody>
            </table>
            
            <div style="margin-top: 30px; font-size: 12px; color: #666;">
                <p>This is an automated report generated by the Intellypod Resort Monitoring System.</p>
                <p>Records with inventory count > 10 are highlighted in red.</p>
                <p>Report generation time is displayed in Eastern Time (ET).</p>
            </div>
        </body>
        </html>
        """
        
        # Create text content
        text_content = f"""
RESORT INVENTORY ALERT (Inventory Count > 0)
Report generated on: {datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S (ET)')}

SUMMARY STATISTICS:
- Status Filter: All records shown have status "Searching"
- Total Unique Resorts: {total_resorts}
- Total Records: {total_records:,}
- Average Inventory Count: {avg_inventory:.1f}
- Highest Inventory Count: {max_inventory}

RESORT DETAILS:
================================================================
"""
        
        # Rest of the text content
        for _, row in filtered_data.iterrows():
            # Format dates - just display as they are from database
            arrival_formatted = format_date_display(row['Arrival'])
            departure_formatted = format_date_display(row['Departure'])
            
            # Handle PropertyType - show "None" instead of N/A
            property_type = row['PropertyType'] if pd.notna(row['PropertyType']) and str(row['PropertyType']).lower() != 'none' else 'None'
            
            text_content += f"""
Vendor Name: {row['Vendor']}
Resort Name: {row['Resort']}
Arrival: {arrival_formatted}
Departure: {departure_formatted}
Property Type: {property_type}
Room Type: {row['RoomType']}
Bed Type: {row['RoomTypeDescription']}
Inventory Count: {row['InventoryCount']}
----------------------------------------
"""
        
        text_content += """

This is an automated report generated by the Intellypod Resort Monitoring System.
Report generation time is displayed in Eastern Time (ET).
"""
        
        return html_content, text_content
    
    def send_email_to_multiple(self, recipient_emails, subject=None, html_content=None, text_content=None):
        """Send email to multiple recipients using Mailjet"""
        if not subject:
            subject = f"Resort Inventory Alert - {datetime.now().strftime('%Y-%m-%d')}"
        
        # Convert string to list if single email provided
        if isinstance(recipient_emails, str):
            recipient_emails = [recipient_emails]
        
        try:
            # Prepare recipient list for Mailjet
            recipients = []
            for email in recipient_emails:
                recipients.append({
                    "Email": email,
                    "Name": "Resort Manager"
                })
            
            data = {
                'Messages': [
                    {
                        "From": {
                            "Email": self.sender_email,
                            "Name": "Intellypod Resort Monitoring"
                        },
                        "To": recipients,
                        "Subject": subject,
                        "TextPart": text_content,
                        "HTMLPart": html_content,
                        "CustomID": f"resort-inventory-alert-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
                    }
                ]
            }
            
            result = self.mailjet.send.create(data=data)
            
            if result.status_code == 200:
                self.logger.info(f"Email sent successfully to {len(recipient_emails)} recipients: {', '.join(recipient_emails)}")
                return True, f"Email sent successfully to {len(recipient_emails)} recipients"
            else:
                self.logger.error(f"Failed to send email: {result.status_code} - {result.json()}")
                return False, f"Failed to send email: {result.status_code}"
                
        except Exception as e:
            self.logger.error(f"Error sending email: {e}")
            return False, f"Error sending email: {e}"
    
    def process_and_send_alert(self, recipient_emails, file_path='csvs/all_resorts_detailed_orders.csv'):
        """Main method to process data and send email alert"""
        try:
            # Process data file
            filtered_data = self.process_data_file(file_path)
            
            if filtered_data is None or filtered_data.empty:
                self.logger.info("No records found with InventoryCount > 0")
                return False, "No records meet the criteria"
            
            # Create email content
            html_content, text_content = self.create_email_content(filtered_data)
            
            if not html_content:
                return False, "Failed to create email content"
            
            # Send email to multiple recipients
            subject = f"Resort Inventory Alert - {len(filtered_data)} Records Found (Inventory > 0)"
            success, message = self.send_email_to_multiple(recipient_emails, subject, html_content, text_content)
            
            return success, message
            
        except Exception as e:
            self.logger.error(f"Error in process_and_send_alert: {e}")
            return False, f"Error: {e}"

def main():
    """Main function to run the email service"""
    try:
        email_service = MailjetEmailService()
        
        # Multiple recipients - Add as many emails as you want
        recipient_emails = [
            "ujjwalr754@gmail.com",
            # "kumar@intellypod.com",
            # "sajol@intellypod.com"
            
        ]
        
        # Or you can also define them from environment variables
        # recipient_emails = os.getenv('RECIPIENT_EMAILS', 'ujjwalr754@gmail.com').split(',')
        
        print(f"Sending emails to {len(recipient_emails)} recipients:")
        for email in recipient_emails:
            print(f"  - {email}")
        
        # Process and send alert
        success, message = email_service.process_and_send_alert(
            recipient_emails=recipient_emails,
            file_path=r'csvs/all_resorts_detailed_orders.csv' 
        )
        
        if success:
            print(f"Success: {message}")
        else:
            print(f"Failed: {message}")
            
    except Exception as e:
        print(f"Error in main: {e}")

if __name__ == "__main__":
    main()