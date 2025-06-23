#!/usr/bin/env python3
import configparser
import csv
from pathlib import Path
import re

def validate_account(account):
    """Validate that account is exactly 12 digits."""
    return re.fullmatch(r'^\d{12}$', account) is not None

def generate_ini_file(output_path, config_data):
    """
    Generate an INI file with PostgreSQL configuration data.
    
    Args:
        output_path (str): Path to the output INI file
        config_data (dict): Dictionary containing configuration data
    """
    config = configparser.ConfigParser()
    
    # Add the PostgreSQL section
    config['postgresql'] = {
        'account': config_data['account'],
        'host': config_data['host'],
        'database': config_data['database'],
        'user': config_data['user'],
        'port': config_data['port'],
        'clusterid': config_data['clusterid']
    }
    
    # Write the configuration to file
    with open(output_path, 'w') as configfile:
        config.write(configfile)
    
    print(f"Generated: {output_path}")

def process_csv_file(csv_path):
    """
    Read and process the CSV file containing PostgreSQL configurations.
    
    Args:
        csv_path (str): Path to the CSV file
    
    Returns:
        list: List of dictionaries containing configuration data
    """
    configs = []
    
    with open(csv_path, mode='r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        
        for row_num, row in enumerate(reader, start=1):
            try:
                # Validate required fields
                if not all(field in row for field in ['account', 'host', 'database', 'user', 'port', 'clusterid']):
                    print(f"Row {row_num}: Missing required fields. Skipping.")
                    continue
                
                # Validate account number
                if not validate_account(row['account']):
                    print(f"Row {row_num}: Invalid account (must be 12 digits). Skipping.")
                    continue
                
                # Validate clusterid
                if not row['clusterid'].strip():
                    print(f"Row {row_num}: Empty clusterid. Skipping.")
                    continue
                
                # Set defaults if empty
                config = {
                    'account': row['account'],
                    'host': row['host'] or 'localhost',
                    'database': row['database'],
                    'user': row['user'],
                    'port': row['port'] or '5432',
                    'clusterid': row['clusterid'].strip()
                }
                
                configs.append(config)
                
            except Exception as e:
                print(f"Row {row_num}: Error processing row - {str(e)}. Skipping.")
    
    return configs

def main():
    # Get CSV file path
    csv_path = input("Enter path to CSV file: ").strip()
    
    try:
        # Process CSV file
        configs = process_csv_file(csv_path)
        
        if not configs:
            print("No valid configurations found in the CSV file.")
            return
        
        # Create output directory
        output_dir = Path("ini_output")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate INI files for each configuration
        for config in configs:
            output_file = output_dir / f"{config['clusterid']}.ini"
            generate_ini_file(output_file, config)
        
        print(f"\nSuccessfully generated {len(configs)} INI files in '{output_dir}' directory.")
    
    except FileNotFoundError:
        print(f"Error: File not found - {csv_path}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == '__main__':
    main()