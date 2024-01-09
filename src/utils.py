"""Utility functions for data processing."""


import logging
import glob
import gzip
import os
import sys
import requests


def download_file(url, filename):
    """Download a file"""
    with requests.get(url, stream=True, timeout=10) as response:
        if response.status_code == 200:
            total_length = int(response.headers.get('content-length', 0))
            downloaded = 0
            with open(filename, 'wb') as file:
                logging.info("Downloading %s...", filename)
                for data in response.iter_content(chunk_size=8192):
                    size = file.write(data)
                    downloaded += size
                    done = int(50 * downloaded / total_length)
                    progress_bar = f"\r[{'=' * done}{' ' * (50-done)}]"
                    progress_info = f" {downloaded}B/{total_length}B"
                    sys.stdout.write(progress_bar + progress_info)
                    sys.stdout.flush()
                logging.info("\nDownload complete.")
        else:
            logging.error("Failed to download %s", filename)


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
        logging.info("Processed %d lines and saved to %s", line_count, output_filename)



def merge_csv_files(input_pattern, output_filename):
    """Merge multiple CSV files into one large file with low memory usage,
    without using the CSV module."""

    logging.info("Starting to merge files matching %s into %s", input_pattern, output_filename)

    temp_output_filename = output_filename + '.tmp'

    input_files = glob.glob(input_pattern)
    if len(input_files) < 2:
        logging.error("Need at least two files to merge.")
        return

    # Merge first two files
    first_file, second_file = input_files[:2]
    with open(first_file, 'r', encoding='utf-8') as f1, \
         open(second_file, 'r', encoding='utf-8') as f2, \
         open(temp_output_filename, 'w', encoding='utf-8') as output_file:
        for line in f1:
            output_file.write(line)
        for i, line in enumerate(f2):
            if i > 0:  # Skip header of the second file
                output_file.write(line)

    # Delete the original files
    os.remove(first_file)
    os.remove(second_file)

    # Merge remaining files one by one
    for input_filename in input_files[2:]:
        with open(input_filename, 'r', encoding='utf-8') as input_file, \
             open(temp_output_filename, 'r', encoding='utf-8') as temp_output_file, \
             open(output_filename, 'w', encoding='utf-8') as final_output_file:
            for line in temp_output_file:
                final_output_file.write(line)
            for i, line in enumerate(input_file):
                if i > 0:  # Skip header
                    final_output_file.write(line)

        # Delete the original file and the old temp file
        os.remove(input_filename)
        os.remove(temp_output_filename)
        # Rename the new output file to be the temp file for the next iteration
        os.rename(output_filename, temp_output_filename)

    # Rename the temp file to the final output file
    os.rename(temp_output_filename, output_filename)
    logging.info("Merging complete. Output saved to %s", output_filename)



def filter_and_write_lines(input_filename, output_filename):
    """
    Read lines from a CSV file, filter out rows where the second column is 'rus',
    and write the first column (without quotes) to an output file.

    Args:
        input_filename (str): The name of the input CSV file.
        output_filename (str): The name of the output file where the filtered lines will be written.
    """
    with open(input_filename, 'r', encoding='utf-8') as input_file, \
         open(output_filename, 'w', encoding='utf-8') as output_file:
        header = input_file.readline()
        output_file.write(header.strip('"').replace('""', '') + '\n')  # Remove quotes from header
        line_count = 0
        for line in input_file:
            columns = line.split(',')
            if columns[1].strip() == 'kaz':
                first_column = columns[0].strip('"')  # Remove quotes from the first column
                output_file.write(f'{first_column}\n')
                line_count += 1
    logging.info("Filtered and wrote %d lines excluding 'rus' language entries.", line_count)


def show_unique_characters_in_file(filename):
    """Print the sorted list of unique characters in a file."""

    with open(filename, 'r', encoding='utf-8') as file:
        contents = file.read()
    unique_characters = set(contents)
    logging.info(''.join(sorted(unique_characters)))


def clean_file_contents(input_filename, output_filename):
    """Clean the file contents by allowing only specified characters."""
    allowed_chars = set(
        '\n !(),-.0123456789?АаӘәБбВвГгҒғДдЕеЁёЖжЗзИиЙйКкҚқЛлМмНнҢңОоӨөПпРрСсТт'
        'УуҰұҮүФфХхҺһЦцЧчШшЩщЪъЫыІіЬьЭэЮюЯя'
    )
    logging.info(
        "Cleaning the contents of %s and saving to %s",
        input_filename, output_filename
    )

    with open(input_filename, 'r', encoding='utf-8') as input_file, \
         open(output_filename, 'w', encoding='utf-8') as output_file:
        line_count = 0
        char_count = 0
        for line in input_file:
            cleaned_line = ''.join(filter(allowed_chars.__contains__, line))
            if cleaned_line:  # Write only non-empty lines to save HDD space
                output_file.write(cleaned_line)
                char_count += len(cleaned_line)
            line_count += 1
            if line_count % 1000 == 0:  # Log progress every 1000 lines to reduce I/O operations
                logging.info("Processed %d lines and %d characters...", line_count, char_count)
    logging.info("\nCleaning complete. Processed %d lines and %d characters in total.",
                  line_count, char_count)
    logging.info("The cleaned contents have been saved to %s", output_filename)

    # Instead of deleting the source file, rename it as a backup
    backup_filename = input_filename + '.bak'
    os.rename(input_filename, backup_filename)
    logging.info("Renamed the source file to: %s", backup_filename)


def compress_file(input_filename, output_filename):
    """Compress a file using gzip with logging and simple text progress status."""

    logging.info("Compressing %s to %s", input_filename, output_filename)
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
                logging.info("Progress: %.2f%%", progress, end='\r')
    logging.info("\nCompression complete. The file %s is ready.", output_filename)



def copy_compressed_file_to_content_folder(src, dst_folder):
    """Copy a compressed file to the specified content folder.
    Includes logging and simple text progress status."""

    logging.info("Copying %s to %s", src, dst_folder)
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
                logging.info("Progress: %.2f%%", progress, end='\r')
    logging.info("\nCopy complete. The file %s has been copied to %s", src, dst_folder)
