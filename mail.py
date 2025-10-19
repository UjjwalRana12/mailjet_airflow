import os
import pandas as pd
import logging
from datetime import datetime
from mailjet_rest import Client
from dotenv import load_dotenv
import pytz

# Load environment variables
load_dotenv()

class MailjetEmailService:
    def __init__(self):
        """Initialize Mailjet client with credentials from .env file"""
        self.api_key = os.getenv('ApiKey')
        self.api_secret = os.getenv('ApiSecret')
        self.sender_email = os.getenv('SenderEmail')
        if not all([self.api_key, self.api_secret, self.sender_email]):
            raise ValueError("Missing Mailjet credentials in .env file (ApiKey, ApiSecret, SenderEmail required)")
        self.mailjet = Client(auth=(self.api_key, self.api_secret), version='v3.1')
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def process_data_file(self, file_path='data/minimum_availability_per_order.csv'):
        """Read minimum_availability_per_order.csv and return rows with Min_Available > 0"""
        try:
            df = pd.read_csv(file_path)
            self.logger.info("Loaded %d records from %s", len(df), file_path)
            if 'Min_Available' not in df.columns:
                raise KeyError("Expected column 'Min_Available' not found in CSV")

            # Clean PropertyType values -> show "None" for N/A/null/blank
            if 'PropertyType' in df.columns:
                df['PropertyType'] = df['PropertyType'].fillna('None').astype(str)
                df['PropertyType'] = df['PropertyType'].replace(['N/A','n/a','NA','na','',' ','nan','NaN','null','NULL'], 'None')

            # Filter rows where minimum availability > 0
            filtered_df = df[df['Min_Available'] > 0].copy()
            self.logger.info("Filtered to %d records with Min_Available > 0", len(filtered_df))
            if filtered_df.empty:
                return pd.DataFrame()

            # Map BedType to user-friendly description
            def bed_to_desc(bed):
                if pd.isna(bed):
                    return 'Unknown'
                s = str(bed).strip()
                return {
                    'Studio': 'Studio',
                    'Bed1': '1 Bedroom',
                    'Bed2': '2 Bedroom',
                    'Bed3': '3 Bedroom',
                    'Bed4': '4 Bedroom'
                }.get(s, s)

            filtered_df['RoomTypeDescription'] = filtered_df['BedType'].apply(bed_to_desc)

            # Ensure Vendor exists (your file shows Wyndham already, but fallback)
            if 'Vendor' not in filtered_df.columns:
                filtered_df['Vendor'] = 'Wyndham'
            else:
                filtered_df['Vendor'] = filtered_df['Vendor'].fillna('Wyndham')

            # Ensure RoomType column exists (minimum file uses 'RoomType')
            if 'RoomType' not in filtered_df.columns and 'RoomType' in df.columns:
                filtered_df['RoomType'] = filtered_df['RoomType']
            elif 'RoomType' not in filtered_df.columns:
                filtered_df['RoomType'] = ''

            # Convert Arrival to datetime for proper sorting
            if 'Arrival' in filtered_df.columns:
                filtered_df['Arrival'] = pd.to_datetime(filtered_df['Arrival'])
                # Sort by Arrival date in ascending order
                filtered_df = filtered_df.sort_values('Arrival', ascending=True).reset_index(drop=True)

            # Select and reorder columns using lists (no sets) - Use Min_Available instead of InventoryCount
            cols = [
                'Vendor',
                'Resort',
                'Arrival',
                'Departure',
                'PropertyType',
                'RoomType',
                'BedType',
                'RoomTypeDescription',
                'Min_Available'
            ]
            # Keep only existing columns from the list
            cols_existing = [c for c in cols if c in filtered_df.columns]
            result_df = filtered_df[cols_existing].copy()

            self.logger.info("Processed data sample (sorted by Arrival date):\n%s", result_df.head().to_string(index=False))
            return result_df

        except Exception as e:
            self.logger.error("Error processing data file: %s", e)
            return pd.DataFrame()

    def create_email_content(self, filtered_data):
        """Create HTML and plain-text email content from processed DF"""
        if filtered_data is None or filtered_data.empty:
            return None, None

        total_resorts = filtered_data['Resort'].nunique()
        total_orders = len(filtered_data)
        avg_inventory = filtered_data['Min_Available'].mean()
        max_inventory = filtered_data['Min_Available'].max()

        def fmt_date(d):
            try:
                return pd.to_datetime(d).strftime('%Y-%m-%d')
            except:
                return str(d)

        now_et = datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S (ET)')

        # Enhanced HTML with alternating row colors for headers - REMOVED searching div
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 20px; background-color: #f5f5f5; color: #333; }}
                .container {{ max-width: 1200px; margin: 0 auto; background-color: white; padding: 30px; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
                .header {{ background-color: #f9f9f9; color: #333; padding: 25px; border-radius: 8px; margin-bottom: 25px; text-align: center; border: 2px solid #ddd; }}
                .header h2 {{ margin: 0; font-size: 32px; font-weight: bold; color: #333 !important; }}
                .header p {{ color: #666 !important; margin: 10px 0 0 0; font-size: 16px; }}
                .summary {{ background-color: #f8f9ff; padding: 20px; border-radius: 8px; margin-bottom: 25px; border-left: 4px solid #2196F3; }}
                .summary p {{ margin: 8px 0; font-size: 16px; color: #333; }}
                .summary h3 {{ color: #1976D2 !important; margin-top: 0; font-size: 20px; }}
                table {{ width: 100%; border-collapse: collapse; margin: 20px 0; font-size: 14px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
                th {{ background-color: #f9f9f9 !important; color: #333 !important; padding: 15px 10px; text-align: left; font-weight: bold; border-bottom: 2px solid #ddd; }}
                td {{ padding: 12px 10px; border-bottom: 1px solid #e0e0e0; color: #333; }}
                tr:nth-child(even) {{ background-color: #f9f9f9; }}
                tr:hover {{ background-color: #f5f5f5; }}
                .high-inventory {{ background-color: #ffebee !important; }}
                .high-inventory:hover {{ background-color: #ffcdd2 !important; }}
                .footer {{ margin-top: 30px; padding: 20px; background-color: #f5f5f5; border-radius: 8px; font-size: 12px; color: #666; text-align: center; }}
                h3 {{ color: #1976D2 !important; font-size: 22px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h2> Resort Minimum Availability Alert</h2>
                    <p>Report generated on: {now_et}</p>
                </div>
                
                <div class="summary">
                    <h3> Summary Statistics</h3>
                    <p><strong>Status:</strong> Searching</p>
                    <p><strong>Total Resorts:</strong> {total_resorts}</p>
                    <p><strong>Total Orders:</strong> {total_orders:,}</p>
                   
                </div>
                
                <h3> Order Details (Sorted by Arrival Date - Ascending)</h3>
                <table>
                    <thead>
                        <tr>
                            <th>Vendor</th>
                            <th>Resort</th>
                            <th>Arrival</th>
                            <th>Departure</th>
                            <th>Property Type</th>
                            <th>Room Type</th>
                            <th>Bed Type</th>
                            <th>Min Availability</th>
                        </tr>
                    </thead>
                    <tbody>
        """

        text = f"RESORT MINIMUM AVAILABILITY ALERT (Min Available > 0)\nReport time: {now_et}\n\nStatus: Searching\n\nData sorted by Arrival Date (Ascending)\n\n"

        for _, r in filtered_data.iterrows():
            prop = r.get('PropertyType', 'None') or 'None'
            arrival = fmt_date(r.get('Arrival', ''))
            departure = fmt_date(r.get('Departure', ''))
            min_available = int(r.get('Min_Available', 0))
            
            # Add red background class if min availability > 10
            row_class = 'high-inventory' if min_available > 10 else ''
            
            html += f"""
                        <tr class="{row_class}">
                            <td>{r.get('Vendor')}</td>
                            <td>{r.get('Resort')}</td>
                            <td>{arrival}</td>
                            <td>{departure}</td>
                            <td>{prop}</td>
                            <td>{r.get('RoomType','')}</td>
                            <td>{r.get('RoomTypeDescription','')}</td>
                            <td><strong>{min_available}</strong></td>
                        </tr>
            """

            text += (
                f"Vendor: {r.get('Vendor')}\n"
                f"Resort: {r.get('Resort')}\n"
                f"Arrival: {arrival}\n"
                f"Departure: {departure}\n"
                f"Property Type: {prop}\n"
                f"Room Type: {r.get('RoomType','')}\n"
                f"Bed Type: {r.get('RoomTypeDescription','')}\n"
                f"Min Availability: {min_available}\n"
                f"{'*** HIGH AVAILABILITY ***' if min_available > 10 else ''}\n"
                f"{'-'*40}\n\n"
            )

        html += """
                    </tbody>
                </table>
                
                <div class="footer">
                    <p> This is an automated report generated by the Intellypod Resort Monitoring System.</p>
                    <p> Records with availability > 10 are highlighted with red background for priority attention.</p>
                    <p> All times are displayed in Eastern Time (ET).</p>
                   
                </div>
            </div>
        </body>
        </html>
        """
        
        text += "\nThis is an automated report generated by the Intellypod Resort Monitoring System.\nRecords with availability > 10 are marked with *** HIGH AVAILABILITY ***\nData is sorted by Arrival Date in ascending order (earliest dates first)."

        return html, text

    def send_email_to_multiple(self, recipient_emails, subject=None, html_content=None, text_content=None):
        """Send email via Mailjet"""
        if not subject:
            subject = f"Resort Minimum Availability Alert - {datetime.now().strftime('%Y-%m-%d')}"
        if isinstance(recipient_emails, str):
            recipient_emails = [recipient_emails]

        try:
            recipients = [{"Email": e, "Name": "Resort Manager"} for e in recipient_emails]
            data = {
                "Messages": [
                    {
                        "From": {"Email": self.sender_email, "Name": "Intellypod Resort Monitoring"},
                        "To": recipients,
                        "Subject": subject,
                        "TextPart": text_content,
                        "HTMLPart": html_content
                    }
                ]
            }
            res = self.mailjet.send.create(data=data)
            if res.status_code in (200, 201):
                self.logger.info("Email sent to %d recipients", len(recipients))
                return True, "Email sent"
            self.logger.error("Mailjet error %s", res.status_code)
            return False, f"Mailjet error {res.status_code}"
        except Exception as e:
            self.logger.error("Error sending email: %s", e)
            return False, str(e)

    def process_and_send_alert(self, recipient_emails, file_path='data/minimum_availability_per_order.csv'):
        df = self.process_data_file(file_path)
        if df is None or df.empty:
            self.logger.info("No records found with Min_Available > 0")
            return False, "No records meet the criteria"
        html, text = self.create_email_content(df)
        if not html:
            return False, "Failed to create email content"
        subject = f" Resort Alert - {len(df)} Orders Found (Searching)"
        return self.send_email_to_multiple(recipient_emails, subject, html, text)

def main():
    try:
        service = MailjetEmailService()
        recipients = [
            "ujjwal@intellypod.com",
            "kumar@intellypod.com",
            "sajol@intellypod.com",
            "ujjwalrana12@outlook.com",
            "Robert.Zukowski@intellypod.com",
            "joshuagomez@tzort.com",
            "Cking@tzort.com ",
            "Jweldy@tzort.com",
        ]
        
        print(f"Sending emails to {len(recipients)} recipients:")
        for r in recipients:
            print(" -", r)
        success, msg = service.process_and_send_alert(recipients, file_path=r"data/minimum_availability_per_order.csv")
        print("Success:" if success else "Failed:", msg)
    except Exception as e:
        print("Error in main:", e)

if __name__ == "__main__":
    main()