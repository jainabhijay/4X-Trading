import pandas as pd
import os
from datetime import datetime

# Function to create 1-minute OHLC data with separate date and time columns
def create_ohlc(df):
    df['DateTime'] = pd.to_datetime(df['Date'] + ' ' + df['Time'])
    df.set_index('DateTime', inplace=True)
    ohlc_data = df['LTP'].resample('1T').ohlc()
    ohlc_data['Volume'] = df['LTQ'].resample('1T').sum()
    ohlc_data['Date'] = ohlc_data.index.date
    ohlc_data['Time'] = ohlc_data.index.time
    return ohlc_data

# Function to check if open value is equal to the user-provided value
def is_open_value_equal_to(open_value, threshold_value):
    return open_value == threshold_value

# Folder containing CSV files
input_folder_path = '/Users/abhijayjain/Desktop/internship/Indira Securities/4x trading/input /Option Files/NIFTY/Put Option'

# New folder to save modified OHLC data
output_folder_path = '/Users/abhijayjain/Desktop/internship/Indira Securities/4x trading/Output files/nifty output/matching time data/put matching time'

# Set the open value threshold to 5
user_open_value = 5

# Iterate through CSV files in the folder
for filename in os.listdir(input_folder_path):
    if filename.endswith('.csv'):
        file_path = os.path.join(input_folder_path, filename)

        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)

        # Create 1-minute OHLC data with separate date and time columns
        ohlc_data = create_ohlc(df)

        # Check the open value after 9:30 AM
        ohlc_data_after_930am = ohlc_data[ohlc_data.index.time >= datetime.strptime("09:30:00", "%H:%M:%S").time()]

        if not ohlc_data_after_930am.empty:
            # Find the first matching time
            first_matching_time = None

            for idx, row in ohlc_data_after_930am.iterrows():
                if is_open_value_equal_to(row['open'], user_open_value):
                    first_matching_time = idx.time()
                    break

            if first_matching_time is not None:
                # Filter OHLC data to keep data only after the first matching time
                ohlc_data_after_matching = ohlc_data_after_930am[
                    ohlc_data_after_930am.index.time >= first_matching_time
                ]

                if not ohlc_data_after_matching.empty:
                    # Save the modified OHLC data to a new CSV file in the output folder
                    output_file_path = os.path.join(output_folder_path, filename.replace('.csv', '_ohlc.csv'))
                    ohlc_data_after_matching.to_csv(output_file_path)

                    # Print the filename and information about the first matching time
                    print(f"Match found in file: {filename}")
                    print(f"First matching time: {first_matching_time}")

                else:
                    print(f"No data found after the first matching time in file: {filename}")

            else:
                print(f"No trade entry points found in file: {filename}")

print("matching time found in the files saved")
#----------------------------------trade pice finder--------------------------------------------------------------------------
import os
import pandas as pd

# Specify the folder path containing your OHLC CSV files
folder_path = '/Users/abhijayjain/Desktop/internship/Indira Securities/4x trading/Output files/nifty output/matching time data/put matching time'

# Define the output folder path
output_folder = '/Users/abhijayjain/Desktop/internship/Indira Securities/4x trading/Output files/nifty output/trade entry point data/put trade entry point'

# Create the output folder if it doesn't exist
os.makedirs(output_folder, exist_ok=True)

# Define your trade entry conditions here
def is_trade_entry_point(open_time_4x, row):
    # Check if the open price is 4X the first open price
    return row['high'] >= open_time_4x

# Initialize a list to store the filenames without trade entry points
files_without_trade_entry = []

# Loop through CSV files in the folder
for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        file_path = os.path.join(folder_path, filename)
        
        try:
            # Load the CSV file as a DataFrame
            df = pd.read_csv(file_path)
            
            if not df.empty:
                first_open_price = df.iloc[0]['open']  # Get the first open price
                open_time_4x = first_open_price * 4   # Calculate the 4X value of the first open price
                
                # Find the index of the first trade entry point
                trade_entry_index = df[df.apply(lambda row: is_trade_entry_point(open_time_4x, row), axis=1)].index[0]
                
                # Keep only the rows from the trade entry point onwards
                modified_df = df.iloc[trade_entry_index:]
                
                # Save the modified DataFrame to a new CSV file in the output folder
                output_file_path = os.path.join(output_folder, filename)
                modified_df.to_csv(output_file_path, index=False)
                print(f"Trade entry points found in {filename}.")
                print(f"Modified file saved to {output_file_path}\n")
        except Exception as e:
            print(f"Error processing {filename}: {str(e)}")

# Print the filenames without trade entry points
if files_without_trade_entry:
    print("Files with no trade entry points:")
    for filename in files_without_trade_entry:
        print(filename)
else:
    print("All files have trade entry points.")
#------------------------------stats finder------------------------------------------
import os
import csv

folder_path = '/Users/abhijayjain/Desktop/internship/Indira Securities/4x trading/Output files/nifty output/trade entry point data/put trade entry point'
output_csv = '/Users/abhijayjain/Desktop/internship/Indira Securities/4x trading/Output files/nifty stats /nifty put stats/transaction for nifty put.csv'  # Output CSV file
output_data = []  # List to store CSV data
total_pnl = {}  # Dictionary to store total PNL for each file
total_sell_price = {}  # Dictionary to store total sell price for each file

for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        file_path = os.path.join(folder_path, filename)

        with open(file_path, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)

            header = next(csv_reader, None)

            if header:
                holding = None  # To keep track of your holding
                quantity = 0  # To keep track of the number of units held
                first_row = next(csv_reader, None)

                if first_row:
                    open_price = float(first_row[1])  # Convert open price to a float
                    entry_time = first_row[0]  # Assuming the entry time is in the first column (index 0)

                    # Initialize option type directly
                    option_type = "Call"  # or "Call" or any value you want

                    # Initialize data for the current file
                    row_data = {
                        'Date': None,
                        'Time': None,
                        'File': None,
                        'Option Type': option_type,  # Add the "Option Type" field
                        'Sell Quantity 50% Gain': 0,
                        'Sell Price 50% Gain': 0,
                        'Exit Time 50% Gain': None,
                        'Sell Quantity 100% Gain': 0,
                        'Sell Price 100% Gain': 0,
                        'Exit Time 100% Gain': None,
                        'Sell Quantity 200% Gain': 0,
                        'Sell Price 200% Gain': 0,
                        'Exit Time 200% Gain': None,
                        'Closing Price': None,
                        'Closing Time': None,
                        'Sell Quantity at Close': 0,
                        'Total PNL': 0  # Column for total Profit and Loss
                    }

                    # Set common values for all rows
                    row_data['Date'] = entry_time.split()[0]
                    row_data['Time'] = entry_time.split()[1]
                    row_data['File'] = filename

                    # Buy 50 units at the opening price
                    original_quantity = 50
                    quantity = original_quantity
                    holding = (entry_time, open_price)

                    # Flags to track whether specific gain targets have been reached
                    target_50_percent_reached = False
                    target_100_percent_reached = False
                    target_200_percent_reached = False

                    for row in csv_reader:
                        current_time = row[0]
                        current_price = float(row[2])  # Convert current price to a float

                        if holding:
                            # Calculate the gain
                            gain = (current_price - holding[1]) / holding[1]

                            if not target_50_percent_reached and gain >= 0.5 and quantity >= 25:
                                # Sell 25 units at a 50% gain
                                row_data['Sell Quantity 50% Gain'] = 25
                                row_data['Sell Price 50% Gain'] = current_price
                                row_data['Exit Time 50% Gain'] = current_time
                                total_sell_price[filename] = total_sell_price.get(filename, 0) + (25 * current_price)
                                pnl_50_percent = 25 * (current_price - holding[1])
                                row_data['Total PNL'] += pnl_50_percent  # Add PNL to the total
                                total_pnl[filename] = total_pnl.get(filename, 0) + pnl_50_percent
                                quantity -= 25
                                target_50_percent_reached = True  # Mark the target as reached

                            if not target_100_percent_reached and gain >= 1.0 and quantity >= 13:
                                # Sell 13 units at a 100% gain
                                row_data['Sell Quantity 100% Gain'] = 13
                                row_data['Sell Price 100% Gain'] = current_price
                                row_data['Exit Time 100% Gain'] = current_time
                                total_sell_price[filename] = total_sell_price.get(filename, 0) + (13 * current_price)
                                pnl_100_percent = 13 * (current_price - holding[1])
                                row_data['Total PNL'] += pnl_100_percent  # Add PNL to the total
                                total_pnl[filename] = total_pnl.get(filename, 0) + pnl_100_percent
                                quantity -= 13
                                target_100_percent_reached = True  # Mark the target as reached

                            if not target_200_percent_reached and gain >= 2.0 and quantity >= 12:
                                # Sell 12 units at a 200% gain
                                row_data['Sell Quantity 200% Gain'] = 12
                                row_data['Sell Price 200% Gain'] = current_price
                                row_data['Exit Time 200% Gain'] = current_time
                                total_sell_price[filename] = total_sell_price.get(filename, 0) + (12 * current_price)
                                pnl_200_percent = 12 * (current_price - holding[1])
                                row_data['Total PNL'] += pnl_200_percent  # Add PNL to the total
                                total_pnl[filename] = total_pnl.get(filename, 0) + pnl_200_percent
                                quantity -= 12
                                target_200_percent_reached = True  # Mark the target as reached
                                holding = None  # Reset holding after selling

                        # Store closing data
                        row_data['Closing Price'] = current_price
                        row_data['Closing Time'] = current_time
                        row_data['Sell Quantity at Close'] = quantity

                    # If there is any remaining quantity at the end, sell it at the closing price
                    if quantity > 0:
                        row_data['Sell Quantity 200% Gain'] = quantity
                        row_data['Sell Price 200% Gain'] = current_price
                        row_data['Exit Time 200% Gain'] = current_time
                        total_sell_price[filename] = total_sell_price.get(filename, 0) + (quantity * current_price)
                        pnl_at_close = quantity * (current_price - holding[1])
                        row_data['Total PNL'] += pnl_at_close  # Add PNL to the total
                        total_pnl[filename] = total_pnl.get(filename, 0) + pnl_at_close
                        quantity = 0
                        holding = None

                    # Append the row data to the output list
                    output_data.append(row_data)

# Print the data in the Python terminal, including sell price for each transaction
# Print the data in the Python terminal, including sell price for each transaction
for row in output_data:
    print(f"Date: {row['Date']}, Time: {row['Time']}, File: {row['File']}, "
          f"Sell Quantity 50% Gain: {row['Sell Quantity 50% Gain']}, Sell Price 50% Gain: {row['Sell Price 50% Gain']}, Exit Time 50% Gain: {row['Exit Time 50% Gain']}, "
          f"Sell Quantity 100% Gain: {row['Sell Quantity 100% Gain']}, Sell Price 100% Gain: {row['Sell Price 100% Gain']}, Exit Time 100% Gain: {row['Exit Time 100% Gain']}, "
          f"Sell Quantity 200% Gain: {row['Sell Quantity 200% Gain']}, Sell Price 200% Gain: {row['Sell Price 200% Gain']}, "
          f"Closing Price: {row['Closing Price']}, Closing Time: {row['Closing Time']}, Sell Quantity at Close: {row['Sell Quantity at Close']}, "
          f"Total PNL: {row['Total PNL']}")


# Print total PNL and total sell price for each file
for filename, pnl in total_pnl.items():
    sell_price = total_sell_price.get(filename, 0)
    print(f"File: {filename}, Total PNL: {pnl}, Total Sell Price: {sell_price}")

# Write the data to the CSV file
with open(output_csv, 'w', newline='') as csv_file:
    fieldnames = ['Date', 'Time', 'File', 'Option Type', 'Sell Quantity 50% Gain', 'Sell Price 50% Gain', 'Exit Time 50% Gain',
                  'Sell Quantity 100% Gain', 'Sell Price 100% Gain', 'Exit Time 100% Gain', 'Sell Quantity 200% Gain',
                  'Sell Price 200% Gain', 'Exit Time 200% Gain', 'Closing Price', 'Closing Time', 'Sell Quantity at Close', 'Total PNL']
    csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    csv_writer.writeheader()
    csv_writer.writerows(output_data)

print(f"Transactions saved to {output_csv}")
#----------------------------hehehehe------------------------------------------
