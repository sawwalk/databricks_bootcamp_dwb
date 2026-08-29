# Bronze Layer Configuration
# Configuration for ingesting CSV files from source systems into bronze tables

# Base configuration
BASE_VOLUME_PATH = "/Volumes/databricks_bootcamp_dwb/bronze/source_system/engineering"
TARGET_SCHEMA = "databricks_bootcamp_dwb.bronze"

# Source folder configurations
# Each source folder maps to a table name prefix
SOURCE_CONFIGS = [
    {
        "folder_name": "source_crm",
        "table_prefix": "crm_",
        "source_path": f"{BASE_VOLUME_PATH}/source_crm"
    },
    {
        "folder_name": "source_erp",
        "table_prefix": "erp_",
        "source_path": f"{BASE_VOLUME_PATH}/source_erp"
    }
]

# Helper functions
def get_table_name(file_name, table_prefix):
    """
    Generate table name from file name and prefix.
    Removes .csv extension and applies prefix.
    
    Args:
        file_name: Original CSV file name (e.g., 'customers.csv')
        table_prefix: Prefix to add (e.g., 'crm_')
    
    Returns:
        Full table name (e.g., 'crm_customers')
    """
    base_name = file_name.replace('.csv', '').replace('.CSV', '')
    return f"{table_prefix}{base_name}"

def get_full_table_name(file_name, table_prefix):
    """
    Generate fully qualified table name.
    
    Args:
        file_name: Original CSV file name
        table_prefix: Prefix to add
    
    Returns:
        Fully qualified table name (e.g., 'databricks_bootcamp_dwb.bronze.crm_customers')
    """
    table_name = get_table_name(file_name, table_prefix)
    return f"{TARGET_SCHEMA}.{table_name}"

def get_all_csv_files(source_path, dbutils_instance):
    """
    Get all CSV files from a source path.
    
    Args:
        source_path: Path to the source folder
        dbutils_instance: dbutils object from Databricks notebook
    
    Returns:
        List of CSV file paths
    """
    csv_files = []
    
    try:
        # List all files in the directory
        files = dbutils_instance.fs.ls(source_path)
        csv_files = [f.path for f in files if f.name.lower().endswith('.csv')]
    except Exception as e:
        print(f"Error reading from {source_path}: {str(e)}")
    
    return csv_files

def get_ingestion_config(dbutils_instance):
    """
    Generate complete ingestion configuration.
    Returns a list of dictionaries with file paths and target table names.
    
    Args:
        dbutils_instance: dbutils object from Databricks notebook
    
    Returns:
        List of dicts with keys: source_file_path, file_name, table_name, full_table_name
    """
    ingestion_items = []
    
    for config in SOURCE_CONFIGS:
        csv_files = get_all_csv_files(config["source_path"], dbutils_instance)
        
        for file_path in csv_files:
            file_name = file_path.split('/')[-1]
            
            item = {
                "source_file_path": file_path,
                "file_name": file_name,
                "table_prefix": config["table_prefix"],
                "table_name": get_table_name(file_name, config["table_prefix"]),
                "full_table_name": get_full_table_name(file_name, config["table_prefix"]),
                "source_folder": config["folder_name"]
            }
            ingestion_items.append(item)
    
    return ingestion_items