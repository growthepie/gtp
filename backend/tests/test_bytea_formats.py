import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.adapters.rpc_funcs.utils import process_bytea_value

def test_process_bytea_value():
    """
    Test the process_bytea_value function with various input formats,
    including the problematic b''\xa9\... format from the logs.
    """
    print("Testing process_bytea_value function with various formats...")

    test_cases = [
        # Standard cases
        {'input': '0x1234', 'expected': '\\x1234', 'name': 'Hex string with 0x prefix'},
        {'input': b'test', 'expected': '\\x74657374', 'name': 'Bytes object'},
        {'input': '\\x1234', 'expected': '\\x1234', 'name': 'Already formatted'},
        
        # Simple string representations of bytes
        {'input': "b'test'", 'expected': '\\x74657374', 'name': 'String representation of bytes (text)'},
        {'input': 'b"test"', 'expected': '\\x74657374', 'name': 'String representation of bytes with double quotes (text)'},
        
        # Hex escape sequences
        {'input': "b'\\x01\\x02'", 'expected': '\\x0102', 'name': 'String representation with hex escapes'},
        {'input': 'b"\\x01\\x02"', 'expected': '\\x0102', 'name': 'String rep with double quotes and hex escapes'},
        
        # Nested quotes (the problematic case from logs)
        {'input': "b''\\xa9\\x05'", 'expected': '\\xa905', 'name': 'Nested quotes with hex escapes'},
        {'input': 'b""\\xa9\\x05"', 'expected': '\\xa905', 'name': 'Nested double quotes with hex escapes'},
        
        # Complex cases
        {'input': "b'\\xa9\\x05\\xf7'", 'expected': '\\xa905f7', 'name': 'Multiple hex escapes'},
        {'input': "b'test\\x00binary'", 'expected': '\\x746573740062696e617279', 'name': 'Mixed text and binary'}
    ]
    
    for i, case in enumerate(test_cases):
        try:
            result = process_bytea_value(case['input'])
            if result == case['expected']:
                print(f"✓ Test {i+1} ({case['name']}) passed: {repr(case['input'])} → {result}")
            else:
                print(f"✗ Test {i+1} ({case['name']}) failed: Expected {case['expected']}, got {result}")
        except Exception as e:
            print(f"✗ Test {i+1} ({case['name']}) error: {str(e)}")
    
    print("\nTesting completed!")

if __name__ == "__main__":
    test_process_bytea_value() 