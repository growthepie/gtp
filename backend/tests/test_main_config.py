from tests import setup_test_imports
# Set up imports
setup_test_imports()

from src.main_config import get_main_config
from src.db_connector import DbConnector

def main():
    print("Testing of MainConfig loading...")
    
    # Create DB connector
    db_connector = DbConnector()
    
    # Load from GitHub first to ensure we have a reference point
    print("1. Loading from GitHub source...")
    main_conf_github = get_main_config(db_connector=db_connector, source='github', api_version="dev")
    print(f"   Loaded {len(main_conf_github)} chains")
    
    # Now load from S3 - this should silently fall back to GitHub
    print("\n2. Loading from S3 source...")
    main_conf_s3 = get_main_config(db_connector=db_connector, api_version="dev")
    print(f"   Loaded {len(main_conf_s3)} chains")
    
    # Verify they're the same
    are_equal = main_conf_github == main_conf_s3
    print(f"\n3. Configs are equal: {are_equal}")
    
    print("\nTest completed successfully!")

if __name__ == "__main__":
    main() 