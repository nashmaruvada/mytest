#!/usr/bin/env python3
import argparse
import configparser
from pathlib import Path

def generate_ini_file(output_path, sections):
    """
    Generate an INI file with the given sections and key-value pairs.
    
    Args:
        output_path (str): Path to the output INI file
        sections (dict): Dictionary containing section names as keys and 
                         dictionaries of key-value pairs as values
    """
    config = configparser.ConfigParser()
    
    for section_name, options in sections.items():
        config[section_name] = options
    
    with open(output_path, 'w') as configfile:
        config.write(configfile)
    
    print(f"INI file successfully generated at: {output_path}")

def parse_command_line():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Generate a text-based INI file from command line parameters.'
    )
    
    parser.add_argument(
        '-o', '--output', 
        type=str, 
        default='output.ini',
        help='Output file path (default: output.ini)'
    )
    
    # Allow multiple section specifications
    parser.add_argument(
        '-s', '--section', 
        action='append', 
        nargs='+',
        help='Add a section with key-value pairs. Format: "section_name key1=value1 key2=value2"'
    )
    
    return parser.parse_args()

def process_section_args(section_args):
    """
    Process the section arguments into a dictionary structure.
    
    Args:
        section_args (list): List of section specifications from command line
    
    Returns:
        dict: Processed sections with their key-value pairs
    """
    sections = {}
    
    if not section_args:
        return sections
    
    for section_spec in section_args:
        if not section_spec:
            continue
            
        # First element is section name
        section_name = section_spec[0]
        options = {}
        
        # Process key=value pairs
        for kv_pair in section_spec[1:]:
            if '=' in kv_pair:
                key, value = kv_pair.split('=', 1)
                options[key.strip()] = value.strip()
            else:
                # Handle keys without values
                options[kv_pair.strip()] = ""
        
        sections[section_name] = options
    
    return sections

def main():
    args = parse_command_line()
    
    # Create parent directories if they don't exist
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Process section arguments
    sections = process_section_args(args.section)
    
    # If no sections provided, create a simple example
    if not sections:
        sections = {
            'DEFAULT': {
                'key1': 'value1',
                'key2': 'value2'
            },
            'Section1': {
                'setting1': 'true',
                'setting2': '42'
            }
        }
        print("No sections provided. Generating example INI file.")
    
    generate_ini_file(output_path, sections)

if __name__ == '__main__':
    main()
