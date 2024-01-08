
import logging

import requests
import glob
import gzip
import shutil
import os
import sys
import csv


def download_file(url, filename):
    """Download a file"""
    
    with requests.get(url, stream=True) as response:
        if response.status_code == 200:
            total_length = int(response.headers.get('content-length', 0))
            downloaded = 0
            with open(filename, 'wb') as file:
                logging.info(f"Downloading {filename}...")
                for data in response.iter_content(chunk_size=8192):
                    size = file.write(data)
                    downloaded += size
                    done = int(50 * downloaded / total_length)
                    sys.stdout.write(f"\r[{'=' * done}{' ' * (50-done)}] {downloaded}B/{total_length}B")
                    sys.stdout.flush()
                logging.info("\nDownload complete.")
        else:
            logging.error(f"Failed to download {filename}")


def save_all_text_lines_to_file(input_filename, output_filename):
    """Save all text lines from a CSV file to a text file, filtering by predicted_language."""
    
    with open(input_filename, 'r', encoding='utf-8') as input_file, \
         open(output_filename, 'w', encoding='utf-8') as output_file:
        line_count = 0
        for line in input_file:
            line_count += 1
            if 'kaz' in line:
                text = line.split(',')[0].strip('"')
                output_file.write(text + '\n')
        logging.info(f"Processed {line_count} lines and saved to {output_filename}")



def merge_csv_files(input_pattern, output_filename):
    """Merge multiple CSV files into one large file with low memory usage, without using CSV module."""
    
    logging.info(f"Starting to merge files matching {input_pattern} into {output_filename}")
    
    headers_written = False
    
    with open(output_filename, 'w', encoding='utf-8') as output_file:
        for input_filename in glob.glob(input_pattern):
            logging.info(f"Processing file: {input_filename}")
            with open(input_filename, 'r', encoding='utf-8') as input_file:
                for i, line in enumerate(input_file):
                    if i == 0 and not headers_written:
                        output_file.write(line)
                        headers_written = True
                    elif i > 0:
                        output_file.write(line)
    
    logging.info(f"Merging complete. Output saved to {output_filename}")



def filter_and_write_lines(input_filename, output_filename):
    """
    Read lines from a CSV file, filter out rows where the second column is 'rus',
    and write the remaining lines to an output file.

    Args:
        input_filename (str): The name of the input CSV file.
        output_filename (str): The name of the output file where the filtered lines will be written.
    """
    line_count = 0
    with open(input_filename, 'r', encoding='utf-8') as input_file, \
         open(output_filename, 'w', encoding='utf-8') as output_file:
        csv_reader = csv.reader(input_file)
        for row in csv_reader:
            if row[1] != 'rus':
                text = '"' + row[0].strip('"') + '"'
                output_file.write(text + '\n')
                line_count += 1
    logging.info(f"Filtered and wrote {line_count} lines excluding Russian language entries.")



def show_unique_characters_in_file(filename):
    """Print the sorted list of unique characters in a file."""
    
    with open(filename, 'r', encoding='utf-8') as file:
        contents = file.read()
    unique_characters = set(contents)
    logging.info(''.join(sorted(unique_characters)))



def clean_file_contents(input_filename, output_filename):
    """Clean the file contents by allowing only specified characters."""
    
    allowed_chars = set('\n !(),-.0123456789?АаӘәБбВвГгҒғДдЕеЁёЖжЗзИиЙйКкҚқЛлМмНнҢңОоӨөПпРрСсТтУуҰұҮүФфХхҺһЦцЧчШшЩщЪъЫыІіЬьЭэЮюЯя')
    logging.info(f"Cleaning the contents of {input_filename} and saving to {output_filename}")
    
    with open(input_filename, 'r', encoding='utf-8') as input_file, \
         open(output_filename, 'w', encoding='utf-8') as output_file:
        line_count = 0
        char_count = 0
        for line in input_file:
            cleaned_line = ''.join(filter(allowed_chars.__contains__, line))
            output_file.write(cleaned_line)
            line_count += 1
            char_count += len(cleaned_line)
            logging.info(f"Processed {line_count} lines and {char_count} characters...")
    logging.info("\nCleaning complete. The cleaned contents have been saved.")



def compress_file(input_filename, output_filename):
    """Compress a file using gzip with logging and simple text progress status."""
    
    logging.info(f"Compressing {input_filename} to {output_filename}")
    total_size = os.path.getsize(input_filename)
    processed_size = 0
    with open(input_filename, 'rb') as file_in:
        with gzip.open(output_filename, 'wb') as file_out:
            while True:
                chunk = file_in.read(1024)
                if not chunk:
                    break
                file_out.write(chunk)
                processed_size += len(chunk)
                progress = (processed_size / total_size) * 100
                logging.info(f"Progress: {progress:.2f}%", end='\r')
    logging.info(f"\nCompression complete. The file {output_filename} is ready.")



def copy_compressed_file_to_content_folder(src, dst_folder):
    """Copy a compressed file to the specified content folder with logging and simple text progress status."""
    
    logging.info(f"Copying {src} to {dst_folder}")
    total_size = os.path.getsize(src)
    processed_size = 0
    with open(src, 'rb') as file_in:
        with open(os.path.join(dst_folder, os.path.basename(src)), 'wb') as file_out:
            while True:
                chunk = file_in.read(1024)
                if not chunk:
                    break
                file_out.write(chunk)
                processed_size += len(chunk)
                progress = (processed_size / total_size) * 100
                logging.info(f"Progress: {progress:.2f}%", end='\r')
    logging.info(f"\nCopy complete. The file {src} has been copied to {dst_folder}")
