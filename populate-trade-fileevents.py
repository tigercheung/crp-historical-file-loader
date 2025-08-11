import os
import shutil
import gzip
import pyodbc
import time
import pickle
import yaml
import re
import logging
from datetime import datetime, date
import argparse
from typing import List, Set, Dict, Any, Tuple, Optional
from logger import logger
from caching import FileCache
from pathlib import Path

# === Constants ===
DEFAULT_CONFIG_PATH = "config-dev.yaml"
CACHE_TMP_SUFFIX = ".tmp"
APP_NAME = "CRP Data Initialisation"

# === Helper Functions ===
def load_config(path: str = DEFAULT_CONFIG_PATH) -> Dict[str, Any]:
    """Load YAML configuration file."""
    with open(path, "r") as file:
        return yaml.safe_load(file)
    
def get_datafiletype_id_from_filename(filename, filename_pattern):
    """
    Extract DataFileTypeId from filename using regex pattern
    Returns: DataFileTypeId (int) or None if no match
    """
    match = re.match(filename_pattern, filename)
    
    if match:
        file_type = match.group(2)  # Extract IRS, OIS, or BS
        
        # Map file types to DataFileTypeId
        type_mapping = {
            'IRS': 1,
            'OIS': 2,
            'BS': 3   # Added BS mapping (you can adjust this value)
        }
        
        return type_mapping.get(file_type)


def insert_fileevent(server, database, sql_template_file_path, data_file_type_id, market_date, file_name, file_location):
    """
    Insert a new FileEvent row if it doesn't already exist.
    """
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
        "Encrypt=no;"
    )

    MarketDate = date.fromisoformat(market_date)
    DataFileTypeId = int(data_file_type_id)
    FileName = file_name
    FileLocation = file_location

    # Check for existing entry
    check_sql = """
        SELECT COUNT(*) FROM FileEvent
        WHERE FileName = ? AND FileLocation = ? AND MarketDate = ? AND DataFileTypeId = ?
    """

    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute(check_sql, (FileName, FileLocation, MarketDate, DataFileTypeId))
        count = cursor.fetchone()[0]

        if count > 0:
            logger.info(f"⏩ Skipped existing entry: {FileName}")
            return False  # Entry already exists

        # Proceed with insert
        with open(sql_template_file_path, "r", encoding="utf-8") as file:
            sql_query = file.read()

        logger.info(f"📝 Inserting new entry: {FileName}")
        cursor.execute(sql_query, (
            MarketDate,
            DataFileTypeId,
            FileName,
            FileLocation,
            'Monitor',
            0,
            'Completed',
            'DLSTAP202',
            datetime.now(),
            datetime.now(),
            "CRP FileEvent populator",
            "CRP FileEvent populator",
            "",
            True
        ))
        conn.commit()
        return True

    # try:
    #     with pyodbc.connect(conn_str) as conn:
    #         cursor = conn.cursor()
    #         cursor.execute(sql)
    #         return {str(row[0]).strip() for row in cursor.fetchall()}   
    # except Exception as e:
    #     logger.info(f"DB query failed: {e}")
    #     return set() 

def populate_fileevents(file_list, sql_server, sql_db, sql_template_file_path, filename_pattern):
    inserted = []
    for src_full_path, filename, formatted_date, _ in file_list:
        data_file_type_id = get_datafiletype_id_from_filename(filename, filename_pattern)
        if data_file_type_id is None:
            logger.warning(f"❓ Unknown file type for: {filename}")
            continue

        success = insert_fileevent(
            sql_server,
            sql_db,
            sql_template_file_path,
            data_file_type_id,
            formatted_date,
            filename,
            src_full_path
        )
        if success:
            inserted.append(filename)
    return inserted


def main(dataFileType_arg: str):

    logger.info(f"Started:: {APP_NAME}.")
    logger.info(f"Config file: {DEFAULT_CONFIG_PATH}.")

    audit_entries = []
    error_message = None
    file_list = []
    total_copied = 0
    filename_pattern = ""
    start_date = ""
    end_date = ""
    start = time.perf_counter()
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")  

    #try:
    logger.info(f"dataFileType: {dataFileType_arg}")
    config = load_config()    
    data_file_type = dataFileType_arg
    dft_config = config.get(data_file_type, {})
        
    # file location
    file_location = dft_config.get("DESTINATION_LOCATION")
    if not file_location:
        file_location = config["DEFAULT_DESTINATION_LOCATION"]
    logger.info(f"File location (DESTINATION_LOCATION): {file_location}")

    # audit file
    audit_file_folder = config.get("AUDIT_FILE_FOLDER", "audit")

    # filename pattern
    filename_pattern = dft_config.get("FILENAME_PATTERN")
  
    # file caching            
    max_num_subfolders = 0 # unlimited
    use_cached = config["USE_CACHED"]  # Toggle this to rebuild cache
    cache_file_folder = config.get("CACHE_FILE_FOLDER")
    cache_file_path = os.path.join(cache_file_folder, f"{data_file_type}_cache.pkl")
    logger.info(f"Cache file: {cache_file_path}")
    
    cache = FileCache(source_location=file_location, cache_path=cache_file_path, max_num_subfolders=max_num_subfolders)
    if use_cached:
        if not cache.load():
            cache.build()
            cache.save()
    else:
        cache.build()
        cache.save()            

    # do the work
    sql_server = config["SQL_SERVER"]
    sql_database = config["SQL_DATABASE"]
    sql_template_file_path = config["SQL_INSERT_TEMPLATE_FILE_PATH"]

    audit_entries = populate_fileevents(cache.file_list, sql_server=sql_server, sql_db=sql_database
                                        , sql_template_file_path=sql_template_file_path, filename_pattern=filename_pattern)      


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataFileType", required=True, help="Data File Type: eg. ClearedPositions")
    args = parser.parse_args()    
    main(args.dataFileType)
