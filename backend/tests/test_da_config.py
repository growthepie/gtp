from tests import setup_test_imports
# Set up imports
setup_test_imports()

from src.da_config import get_da_config

def main():
    print("Testing of DAConfig loading...")
    
    # Load from GitHub
    print("1. Loading from GitHub source...")
    da_conf_github = get_da_config(source='github')
    print(f"   Loaded {len(da_conf_github)} DA layers")
    
    # Test consistency
    print("\n2. Testing consistency with another GitHub load:")
    da_conf_github2 = get_da_config(source='github')
    print(f"   Configs are equal: {da_conf_github == da_conf_github2}")
    
    # Additional validation test: Test logo defaulting
    print("\n3. Testing that a DA layer with null logo gets the default logo:")
    # Manually create a test config dict with null logo
    test_conf_dict = [{
        "da_layer": "test_layer",
        "name": "Test Layer",
        "name_short": "TL",
        "block_explorers": {},
        "colors": {"light": ["#123456"], "dark": ["#654321"]},
        "logo": None,  # This should get the default logo
        "incl_in_da_overview": True,
        "parameters": {}
    }]
    
    # Process with validator
    da_conf_test = get_da_config(da_config_dict=test_conf_dict, source='github')
    default_logo = {
        'body': "<svg width=\"15\" height=\"15\" viewBox=\"0 0 15 15\" fill=\"none\" xmlns=\"http://www.w3.org/2000/svg\"><circle cx=\"7.5\" cy=\"7.5\" r=\"7.5\" fill=\"currentColor\"/></svg>",
        'width': 15,
        'height': 15
    }
    print(f"   Has default logo: {da_conf_test[0].logo == default_logo}")
    
    print("\nTest completed successfully!")

if __name__ == "__main__":
    main() 