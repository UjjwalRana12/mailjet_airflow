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

            # Rename Min_Available -> InventoryCount
            filtered_df = filtered_df.rename(columns={'Min_Available': 'InventoryCount'})

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

            # Select and reorder columns using lists (no sets)
            cols = [
                'Vendor',
                'Resort',
                'Arrival',
                'Departure',
                'PropertyType',
                'RoomType',
                'BedType',
                'RoomTypeDescription',
                'InventoryCount'
            ]
            # Keep only existing columns from the list
            cols_existing = [c for c in cols if c in filtered_df.columns]
            result_df = filtered_df[cols_existing].copy()

            # Add a Status column for compatibility
            result_df['Status'] = 'Available'

            self.logger.info("Processed data sample:\n%s", result_df.head().to_string(index=False))
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
        avg_inventory = filtered_data['InventoryCount'].mean()
        max_inventory = filtered_data['InventoryCount'].max()

        def fmt_date(d):
            try:
                return pd.to_datetime(d).strftime('%Y-%m-%d')
            except:
                return str(d)

        now_et = datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S (ET)')

        html = f"""
        <html><body>
        <h2>Resort Minimum Availability Alert (Min Available &gt; 0)</h2>
        <p>Report time: {now_et}</p>
        <p>Total Resorts: {total_resorts} — Total Orders: {total_orders}</p>
        <p>Avg Min Availability: {avg_inventory:.1f} — Max Min Availability: {max_inventory}</p>
        <table border="1" cellpadding="4" cellspacing="0" style="border-collapse:collapse">
        <thead>
        <tr><th>Vendor</th><th>Resort</th><th>Arrival</th><th>Departure</th><th>PropertyType</th>
        <th>RoomType</th><th>BedType</th><th>Min Availability</th><th>Status</th></tr>
        </thead><tbody>
        """

        text = f"RESORT MINIMUM AVAILABILITY ALERT (Min Available > 0)\nReport time: {now_et}\n\n"

        for _, r in filtered_data.iterrows():
            prop = r.get('PropertyType', 'None') or 'None'
            arrival = fmt_date(r.get('Arrival', ''))
            departure = fmt_date(r.get('Departure', ''))
            html += f"<tr><td>{r.get('Vendor')}</td><td>{r.get('Resort')}</td><td>{arrival}</td><td>{departure}</td>"
            html += f"<td>{prop}</td><td>{r.get('RoomType','')}</td><td>{r.get('BedType','')}</td>"
            html += f"<td><strong>{int(r.get('InventoryCount',0))}</strong></td><td>Available</td></tr>"

            text += (
                f"Vendor: {r.get('Vendor')}\nResort: {r.get('Resort')}\n"
                f"Arrival: {arrival}\nDeparture: {departure}\n"
                f"PropertyType: {prop}\nRoomType: {r.get('RoomType','')}\n"
                f"BedType: {r.get('BedType','')}\nMin Availability: {int(r.get('InventoryCount',0))}\n\n"
            )

        html += "</tbody></table><p>This is an automated report.</p></body></html>"
        text += "\nThis is an automated report."

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
                self.logger.info("Email sent to %d recipients", len(recipient_emails))
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
        subject = f"Resort Minimum Availability Alert - {len(df)} Orders Found"
        return self.send_email_to_multiple(recipient_emails, subject, html, text)

def main():
    try:
        service = MailjetEmailService()
        recipients = [
            "ujjwalr754@gmail.com",
            "ujjwal@intellypod.com"
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

