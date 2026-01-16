#!/usr/bin/env python3
"""
Test MotherDuck upload locally before committing to GitHub.
Usage: python test_motherduck_upload.py [file_pattern]
"""

import os
import sys
import duckdb
from pathlib import Path
from dotenv import load_dotenv

def main():
    load_dotenv()
    
    # Set SSL bypass
    os.environ['DUCKDB_MOTHERDUCK_DISABLE_SSL_VERIFICATION'] = '1'
    
    # Configuration
    token = os.getenv('MOTHERDUCK_TOKEN')
    database = 'football_analytics'
    schema = 'raw'
    file_pattern = sys.argv[1] if len(sys.argv) > 1 else '*'
    
    if not token:
        print("❌ MOTHERDUCK_TOKEN not found in .env file")
        sys.exit(1)
    
    # Connect to MotherDuck
    print(f"Connecting to MotherDuck database: {database}")
    print("(SSL verification disabled for corporate network)")
    try:
        conn = duckdb.connect(f'md:{database}?motherduck_token={token}')
        print("✓ Connected successfully")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("\n💡 Workaround: Export to Parquet and upload via MotherDuck UI")
        print("   The GitHub Action will still work (different network)")
        sys.exit(1)
    
    # Create schema if not exists
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    print(f"✓ Schema '{schema}' ready")
    
    # Find all data files
    data_dir = Path('data/committed')
    if not data_dir.exists():
        print("⚠️  No data/committed directory found")
        print("Copy your scraped files there first:")
        print("  cp data/raw/your_file.csv data/committed/")
        sys.exit(0)
    
    # Process files
    csv_files = list(data_dir.glob(f'**/{file_pattern}.csv'))
    parquet_files = list(data_dir.glob(f'**/{file_pattern}.parquet'))
    
    all_files = csv_files + parquet_files
    
    if not all_files:
        print(f"⚠️  No files matching pattern '{file_pattern}' found in data/committed/")
        sys.exit(0)
    
    print(f"\nFound {len(all_files)} file(s) to upload:")
    for file in all_files:
        print(f"  - {file}")
    
    print()
    confirm = input("Proceed with upload? (y/N): ")
    if confirm.lower() != 'y':
        print("Cancelled")
        sys.exit(0)
    
    # Upload each file
    success_count = 0
    for file_path in all_files:
        # Derive table name from filename
        table_name = file_path.stem
        full_table = f"{schema}.{table_name}"
        
        try:
            print(f"\n📤 Uploading {file_path.name} to {full_table}...")
            
            # Read file and upload
            if file_path.suffix == '.csv':
                conn.execute(f"""
                    CREATE OR REPLACE TABLE {full_table} AS 
                    SELECT * FROM read_csv_auto('{file_path}')
                """)
            else:  # parquet
                conn.execute(f"""
                    CREATE OR REPLACE TABLE {full_table} AS 
                    SELECT * FROM read_parquet('{file_path}')
                """)
            
            # Get row count
            count = conn.execute(f"SELECT COUNT(*) FROM {full_table}").fetchone()[0]
            print(f"   ✓ Uploaded {count} rows to {full_table}")
            success_count += 1
            
        except Exception as e:
            print(f"   ❌ Failed to upload {file_path.name}: {e}")
    
    conn.close()
    
    print(f"\n{'='*60}")
    print(f"✓ Upload complete: {success_count}/{len(all_files)} files successful")
    print(f"{'='*60}")
    
    if success_count == len(all_files):
        print("\n✓ Ready to commit and push to GitHub!")
        print("  git add data/committed/")
        print("  git commit -m 'Update football data'")
        print("  git push")
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()
