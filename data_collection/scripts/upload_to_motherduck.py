#!/usr/bin/env python3
"""
Upload local DuckDB data to MotherDuck.

This script syncs data from your local DuckDB file to MotherDuck,
useful when direct upload has SSL/network issues.
"""

import duckdb
import os
from dotenv import load_dotenv

load_dotenv()

def upload_to_motherduck():
    """Upload data from local DuckDB to MotherDuck."""
    
    # Configuration
    motherduck_token = os.getenv('MOTHERDUCK_TOKEN')
    if not motherduck_token:
        raise ValueError("MOTHERDUCK_TOKEN not set in .env file")
    
    local_db = 'data/football_analytics.duckdb'
    database = os.getenv('DATABASE_NAME', 'football_analytics')
    schema = os.getenv('SCHEMA_NAME', 'raw')
    
    if not os.path.exists(local_db):
        raise FileNotFoundError(f"Local DuckDB file not found: {local_db}")
    
    print(f"📤 Uploading from {local_db} to MotherDuck...\n")
    
    try:
        # Disable SSL verification for corporate networks - multiple approaches
        os.environ['DUCKDB_MOTHERDUCK_DISABLE_SSL_VERIFICATION'] = '1'
        os.environ['GRPC_DEFAULT_SSL_ROOTS_FILE_PATH'] = ''
        os.environ['GRPC_VERBOSITY'] = 'ERROR'
        
        # Try disabling SSL verification at Python level
        import ssl
        try:
            ssl._create_default_https_context = ssl._create_unverified_context
        except:
            pass
        
        # Method 1: Try direct MotherDuck connection
        print("Attempting direct MotherDuck connection (SSL verification disabled)...")
        md_conn = duckdb.connect(f'md:{database}?motherduck_token={motherduck_token}')
        
        # Attach local database
        md_conn.execute(f"ATTACH '{local_db}' AS local_db")
        
        # Create schema in MotherDuck
        md_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        
        # Get list of tables from local database
        tables = md_conn.execute(f"SELECT table_name FROM local_db.information_schema.tables WHERE table_schema = '{schema}'").fetchall()
        
        print(f"\nFound {len(tables)} table(s) to upload:\n")
        
        for (table_name,) in tables:
            print(f"  Uploading {schema}.{table_name}...", end=" ")
            
            # Copy data from local to MotherDuck
            md_conn.execute(f"CREATE OR REPLACE TABLE {schema}.{table_name} AS SELECT * FROM local_db.{schema}.{table_name}")
            
            # Verify
            count = md_conn.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}").fetchone()[0]
            print(f"✓ {count} rows")
        
        md_conn.close()
        print(f"\n✅ Successfully uploaded to MotherDuck database: {database}")
        print(f"\nQuery your data at: https://app.motherduck.com/")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        print("\n💡 Alternative: Export as Parquet and upload via MotherDuck UI:")
        print(f"   1. Export: duckdb {local_db} -c \"COPY {schema}.* TO 'export/{{table_name}}.parquet' (FORMAT PARQUET)\"")
        print(f"   2. Upload at: https://app.motherduck.com/")
        raise


def export_to_parquet():
    """Export local DuckDB tables to Parquet files."""
    local_db = 'data/football_analytics.duckdb'
    schema = os.getenv('SCHEMA_NAME', 'raw')
    export_dir = 'data/export'
    
    os.makedirs(export_dir, exist_ok=True)
    
    print(f"📦 Exporting tables to Parquet files...\n")
    
    conn = duckdb.connect(local_db)
    
    # Get list of tables
    tables = conn.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'").fetchall()
    
    for (table_name,) in tables:
        output_file = f"{export_dir}/{table_name}.parquet"
        print(f"  Exporting {table_name}...", end=" ")
        
        conn.execute(f"COPY {schema}.{table_name} TO '{output_file}' (FORMAT PARQUET)")
        
        file_size = os.path.getsize(output_file) / 1024 / 1024  # MB
        print(f"✓ {file_size:.2f} MB")
    
    conn.close()
    
    print(f"\n✅ Files exported to: {export_dir}/")
    print(f"\n📤 Upload these files to MotherDuck:")
    print(f"   1. Visit: https://app.motherduck.com/")
    print(f"   2. Click 'Import Data' → Upload Parquet files")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'export':
        export_to_parquet()
    else:
        try:
            upload_to_motherduck()
        except Exception:
            print("\n" + "="*60)
            print("Falling back to Parquet export...")
            print("="*60 + "\n")
            export_to_parquet()
