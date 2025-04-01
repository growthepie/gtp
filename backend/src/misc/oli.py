import json
import secrets
import time
import requests
from web3 import Web3
from eth_abi.abi import encode
import eth_account
from eth_keys import keys

class oliAPI:
    def __init__(self, private_key, is_production=True):
        """
        Initialize the OLI API client.
        
        Args:
            private_key (str): The private key to sign attestations
            is_production (bool): Whether to use production or testnet
        """
        # Set network based on environment
        if is_production:
            self.rpc = "https://mainnet.base.org"
            self.rpc_chain_number = 8453
            self.eas_api_url = "https://base.easscan.org/offchain/store"
            self.eas_address = "0x4200000000000000000000000000000000000021"  # EAS contract address on mainnet
        else:
            self.rpc = "https://sepolia.base.org"  # Updated to match your script
            self.rpc_chain_number = 84532
            self.eas_api_url = "https://base-sepolia.easscan.org/offchain/store"
            self.eas_address = "0x4200000000000000000000000000000000000021"  # EAS contract address on testnet
            
        # Initialize Web3 and account
        self.w3 = Web3(Web3.HTTPProvider(self.rpc))
        if not self.w3.is_connected():
            raise Exception("Failed to connect to the Ethereum node")
            
        # Convert the hex private key to the proper key object
        self.private_key = private_key
        if private_key.startswith('0x'):
            private_key_bytes = private_key[2:]
        else:
            private_key_bytes = private_key
        private_key_obj = keys.PrivateKey(bytes.fromhex(private_key_bytes))
        
        # Create account from private key
        self.account = eth_account.Account.from_key(private_key_obj)
        self.address = self.account.address
        
        # Label Pool Schema for OLI
        self.oli_label_pool_schema = '0xb763e62d940bed6f527dd82418e146a904e62a297b8fa765c9b3e1f0bc6fdd68'
        
        # Load EAS ABI
        self.eas_abi = '[{"inputs": [{"components": [{"internalType": "bytes32","name": "schema","type": "bytes32"},{"components": [{"internalType": "address","name": "recipient","type": "address"},{"internalType": "uint64","name": "expirationTime","type": "uint64"},{"internalType": "bool","name": "revocable","type": "bool"},{"internalType": "bytes32","name": "refUID","type": "bytes32"},{"internalType": "bytes","name": "data","type": "bytes"},{"internalType": "uint256","name": "value","type": "uint256"}],"internalType": "struct AttestationRequestData","name": "data","type": "tuple"}],"internalType": "struct AttestationRequest","name": "request","type": "tuple"}],"name": "attest","outputs": [{"internalType": "bytes32","name": "","type": "bytes32"}],"stateMutability": "payable","type": "function"},{"inputs": [{"components": [{"internalType": "bytes32","name": "schema","type": "bytes32"},{"components": [{"internalType": "bytes32","name": "uid","type": "bytes32"},{"internalType": "uint256","name": "value","type": "uint256"}],"internalType": "struct RevocationRequestData","name": "data","type": "tuple"}],"internalType": "struct RevocationRequest","name": "request","type": "tuple"}],"name": "revoke","outputs": [],"stateMutability": "payable","type": "function"},{"inputs": [{"components": [{"internalType": "bytes32","name": "schema","type": "bytes32"},{"components": [{"internalType": "address","name": "recipient","type": "address"},{"internalType": "uint64","name": "expirationTime","type": "uint64"},{"internalType": "bool","name": "revocable","type": "bool"},{"internalType": "bytes32","name": "refUID","type": "bytes32"},{"internalType": "bytes","name": "data","type": "bytes"},{"internalType": "uint256","name": "value","type": "uint256"}],"internalType": "struct AttestationRequestData[]","name": "data","type": "tuple[]"}],"internalType": "struct MultiAttestationRequest[]","name": "multiRequests","type": "tuple[]"}],"name": "multiAttest","outputs": [{"internalType": "bytes32[]","name": "","type": "bytes32[]"}],"stateMutability": "payable","type": "function"},{"inputs": [{"components": [{"internalType": "bytes32","name": "schema","type": "bytes32"},{"components": [{"internalType": "bytes32","name": "uid","type": "bytes32"},{"internalType": "uint256","name": "value","type": "uint256"}],"internalType": "struct RevocationRequestData[]","name": "data","type": "tuple[]"}],"internalType": "struct MultiRevocationRequest[]","name": "multiRequests","type": "tuple[]"}],"name": "multiRevoke","outputs": [],"stateMutability": "payable","type": "function"}]'
        
        # Initialize EAS contract
        self.eas = self.w3.eth.contract(address=self.eas_address, abi=self.eas_abi)
    
    def get_eas_abi(self):
        """
        Returns the ABI for the EAS contract
        """
        # This is a simplified ABI with just the attest function
        # For a production environment, you should use the full ABI
        return [
            {
                "inputs": [
                    {
                        "components": [
                            {"internalType": "bytes32", "name": "schema", "type": "bytes32"},
                            {
                                "components": [
                                    {"internalType": "address", "name": "recipient", "type": "address"},
                                    {"internalType": "uint64", "name": "expirationTime", "type": "uint64"},
                                    {"internalType": "bool", "name": "revocable", "type": "bool"},
                                    {"internalType": "bytes32", "name": "refUID", "type": "bytes32"},
                                    {"internalType": "bytes", "name": "data", "type": "bytes"},
                                    {"internalType": "uint256", "name": "value", "type": "uint256"}
                                ],
                                "internalType": "struct AttestationRequestData",
                                "name": "data",
                                "type": "tuple"
                            }
                        ],
                        "internalType": "struct AttestationRequest",
                        "name": "request",
                        "type": "tuple"
                    }
                ],
                "name": "attest",
                "outputs": [{"internalType": "bytes32", "name": "", "type": "bytes32"}],
                "stateMutability": "payable",
                "type": "function"
            }
        ]
    
    def encode_label_data(self, chain_id, tags_json):
        """
        Encode label data in the OLI format.
        
        Args:
            chain_id (str): Chain ID in CAIP-2 format of the label (e.g. 'eip155:8453')
            tags_json (dict): Dictionary of tag data following the OLI format
            
        Returns:
            str: Hex-encoded ABI data
        """
        # Convert dict to JSON string if needed
        if isinstance(tags_json, dict):
            tags_json = json.dumps(tags_json)
            
        # ABI encode the data
        encoded_data = encode(['string', 'string'], [chain_id, tags_json])
        return f"0x{encoded_data.hex()}"
    
    def create_offchain_attestation(self, recipient, schema, data, revocable=True, expiration_time=0, ref_uid="0x0000000000000000000000000000000000000000000000000000000000000000"):
        """
        Create an attestation with the given parameters.
        
        Args:
            recipient (str): Ethereum address of the contract to be labeled
            schema (str): Schema hash
            data (str): Hex-encoded data
            revocable (bool): Whether the attestation is revocable
            expiration_time (int): Expiration time in seconds since epoch
            ref_uid (str): Reference UID
            
        Returns:
            dict: The signed attestation and UID
        """
        # Create a random salt
        salt = f"0x{secrets.token_hex(32)}"
        
        # Current time in seconds
        current_time = int(time.time())
        
        # Typed data for the attestation
        typed_data = {
            "version": 2,
            "recipient": recipient,
            "time": current_time,
            "revocable": revocable,
            "schema": schema,
            "refUID": ref_uid,
            "data": data,
            "expirationTime": expiration_time,
            "salt": salt,
        }
        
        # EIP-712 typed data format
        types = {
            "domain": {
                "name": "EAS Attestation",
                "version": "1.2.0",
                "chainId": self.rpc_chain_number,
                "verifyingContract": self.eas_contract
            },
            "primaryType": "Attest",
            "message": typed_data,
            "types": {
                "Attest": [
                    {"name": "version", "type": "uint16"},
                    {"name": "schema", "type": "bytes32"},
                    {"name": "recipient", "type": "address"},
                    {"name": "time", "type": "uint64"},
                    {"name": "expirationTime", "type": "uint64"},
                    {"name": "revocable", "type": "bool"},
                    {"name": "refUID", "type": "bytes32"},
                    {"name": "data", "type": "bytes"},
                    {"name": "salt", "type": "bytes32"}
                ]
            }
        }

        # Make sure to install correct version of eth-account (pip install eth-account==0.13.5)
        signed_message = self.account.sign_typed_data(
            domain_data=types["domain"],
            message_types=types["types"],
            message_data=typed_data
        )
        
        # Calculate the UID
        attester = '0x0000000000000000000000000000000000000000'  # for offchain UID calculation
        uid = self.calculate_attestation_uid_v2(schema, recipient, attester, current_time, data, expiration_time, revocable, ref_uid, salt=salt)
        uid_hex = '0x' + uid.hex()
        
        # Package the result
        result = {
            "sig": {
                "domain": types["domain"],
                "primaryType": types["primaryType"],
                "types": types["types"],
                "message": typed_data,
                "uid": uid_hex,
                "version": 2,
                "signature": {
                    "r": hex(signed_message.r),
                    "s": hex(signed_message.s),
                    "v": signed_message.v
                }
            },
            "signer": self.address
        }
        
        return result
    
    def submit_offchain_attestation(self, attestation, filename="OLI.txt"):
        """
        Submit an attestation to the EAS API.
        
        Args:
            attestation (dict): The attestation package
            filename (str): Custom filename
            
        Returns:
            dict: API response
        """
        # Convert numerical values to strings for JSON serialization
        attestation["sig"]["message"]["time"] = str(attestation["sig"]["message"]["time"])
        attestation["sig"]["message"]["expirationTime"] = str(attestation["sig"]["message"]["expirationTime"])
        attestation["sig"]["domain"]["chainId"] = str(attestation["sig"]["domain"]["chainId"])
        
        # Prepare payload for the API endpoint
        payload = {
            "filename": filename,
            "textJson": json.dumps(attestation, separators=(',', ':'))
        }
        
        headers = {
            "Content-Type": "application/json"
        }
        
        # Post the data to the API
        response = requests.post(self.eas_api_url, json=payload, headers=headers)
        return response.json()
    
    def create_offchain_label(self, address, tags, chain_id=None):
        """
        Create an offchain OLI label attestation for a contract.
        
        Args:
            address (str): The contract address to label
            tags (dict): Tag information (name, version, etc.)
            chain_id (str): Chain ID in CAIP-2 format
            
        Returns:
            dict: API response
        """
        # Use Base chain ID if not specified
        if chain_id is None:
            chain_id = f"eip155:{self.rpc_chain_number}"
            
        # Encode the label data
        data = self.encode_label_data(chain_id, tags)
        
        # Create the attestation
        attestation = self.create_offchain_attestation(
            recipient=address,
            schema=self.oli_label_pool_schema,
            data=data
        )
        
        # Submit to the API
        return self.submit_offchain_attestation(attestation)
    
    def calculate_attestation_uid_v2(self, schema, recipient, attester, timestamp, data, expiration_time=0, revocable=True, ref_uid="0x0000000000000000000000000000000000000000000000000000000000000000", bump=0, salt=None):
        """
        Calculate the UID for an offchain attestation (v2).
        
        Args:
            schema (str): Schema hash
            recipient (str): Recipient address
            attester (str): Attester address
            timestamp (int): Timestamp
            data (str): Attestation data
            expiration_time (int): Expiration time
            revocable (bool): Whether attestation is revocable
            ref_uid (str): Reference UID
            bump (int): Bump value
            salt (str): Salt value
            
        Returns:
            bytes: The calculated UID
        """
        # Generate salt if not provided
        if salt is None:
            salt = f"0x{secrets.token_hex(32)}"
            
        # Version
        version = 2
        version_bytes = version.to_bytes(2, byteorder='big')
        
        # Handle schema formatting
        if not schema.startswith('0x'):
            schema = '0x' + schema
        schema_utf8_bytes = schema.encode('utf-8')
        schema_bytes = schema_utf8_bytes
        
        # Convert values to bytes
        recipient_bytes = Web3.to_bytes(hexstr=recipient)
        attester_bytes = Web3.to_bytes(hexstr=attester)
        timestamp_bytes = timestamp.to_bytes(8, byteorder='big')
        expiration_bytes = expiration_time.to_bytes(8, byteorder='big')
        revocable_bytes = bytes([1]) if revocable else bytes([0])
        ref_uid_bytes = Web3.to_bytes(hexstr=ref_uid)
        data_bytes = Web3.to_bytes(hexstr=data)
        salt_bytes = Web3.to_bytes(hexstr=salt)
        bump_bytes = bump.to_bytes(4, byteorder='big')
        
        # Pack all values
        packed_data = (
            version_bytes + schema_bytes + recipient_bytes + attester_bytes + 
            timestamp_bytes + expiration_bytes + revocable_bytes + ref_uid_bytes + 
            data_bytes + salt_bytes + bump_bytes
        )
        
        # Calculate keccak256 hash
        uid = Web3.keccak(packed_data)
        return uid
    
    def create_onchain_label(self, address, tags, chain_id=None, gas_limit=1000000, ref_uid="0x0000000000000000000000000000000000000000000000000000000000000000"):
        """
        Create an onchain OLI label attestation for a contract.
        
        Args:
            address (str): The contract address to label
            tags (dict): Tag information (name, version, etc.)
            chain_id (str): Chain ID in CAIP-2 format
            gas_limit (int): Gas limit for the transaction
            ref_uid (str): Reference UID
            
        Returns:
            str: Transaction hash
            str: UID of the attestation
        """
        # Use Base chain ID if not specified
        if chain_id is None:
            chain_id = f"eip155:{self.rpc_chain_number}"
            
        # Encode the label data
        data = self.encode_label_data(chain_id, tags)
        
        # Prepare transaction parameters
        transaction = self.eas.functions.attest({
            'schema': self.w3.to_bytes(hexstr=self.oli_label_pool_schema),
            'data': {
                'recipient': self.w3.to_checksum_address(address),
                'expirationTime': 0,
                'revocable': True,
                'refUID': self.w3.to_bytes(hexstr=ref_uid),
                'data': self.w3.to_bytes(hexstr=data),
                'value': 0
            }
        }).build_transaction({
            'chainId': self.rpc_chain_number,
            'gas': gas_limit,
            'gasPrice': self.w3.eth.gas_price,
            'nonce': self.w3.eth.get_transaction_count(self.address),
        })
        
        # Sign the transaction with the private key
        signed_txn = self.w3.eth.account.sign_transaction(transaction, private_key=self.private_key)
        
        # Send the transaction
        txn_hash = self.w3.eth.send_raw_transaction(signed_txn.raw_transaction)
        
        # Wait for the transaction receipt
        txn_receipt = self.w3.eth.wait_for_transaction_receipt(txn_hash)
        
        # Check if the transaction was successful
        if txn_receipt.status == 1:
            return f"0x{txn_hash.hex()}", f"0x{txn_receipt.logs[0].data.hex()}"
        else:
            raise Exception(f"Transaction failed: {txn_receipt}")
        
    def revoke_attestation(self, schema, uid_hex, gas_limit=1000000):
        """
        Revoke an attestation (onchain or offchain) using its UID.
        
        Args:
            schema (str): Schema hash used for the attestation
            uid_hex (str): UID of the attestation to revoke (in hex format)
            gas_limit (int): Gas limit for the transaction
            
        Returns:
            str: Transaction hash
        """
        # Create the transaction to revoke an attestation
        transaction = self.eas.functions.revoke({
            'schema': self.w3.to_bytes(hexstr=schema),
            'data': {
                'uid': self.w3.to_bytes(hexstr=uid_hex),
                'value': 0
            }
        }).build_transaction({
            'chainId': self.rpc_chain_number,
            'gas': gas_limit,
            'gasPrice': self.w3.eth.gas_price,
            'nonce': self.w3.eth.get_transaction_count(self.address),
        })

        # Sign the transaction
        signed_txn = self.w3.eth.account.sign_transaction(transaction, private_key=self.private_key)

        # Send the transaction
        txn_hash = self.w3.eth.send_raw_transaction(signed_txn.raw_transaction)

        # Get the transaction receipt
        txn_receipt = self.w3.eth.wait_for_transaction_receipt(txn_hash)
        
        # Check if the transaction was successful
        if txn_receipt.status == 1:
            return f"0x{txn_hash.hex()}"
        else:
            raise Exception(f"Transaction failed: {txn_receipt}")
    
    def multi_revoke_attestations(self, schema, uids, gas_limit=1000000):
        """
        Revoke multiple attestations (onchain or offchain) in a single transaction.
        
        Args:
            schema (str): Schema hash used for the attestations
            uids (list): List of UIDs to revoke (in hex format)
            gas_limit (int): Gas limit for the transaction
            
        Returns:
            str: Transaction hash
            int: Number of attestations revoked
        """
        # Dynamically create the revocation data array based on the uids list
        revocation_data = []
        for uid in uids:
            revocation_data.append({
                'uid': self.w3.to_bytes(hexstr=uid),
                'value': 0
            })

        # Create the multi-revocation request with all UIDs
        multi_requests = [{
            'schema': self.w3.to_bytes(hexstr=schema),
            'data': revocation_data
        }]

        # Create the transaction to call the multiRevoke function
        transaction = self.eas.functions.multiRevoke(
            multi_requests
        ).build_transaction({
            'chainId': self.rpc_chain_number,
            'gas': gas_limit,  # Increased gas limit for large revocations
            'gasPrice': self.w3.eth.gas_price,
            'nonce': self.w3.eth.get_transaction_count(self.address),
        })

        # Sign the transaction
        signed_txn = self.w3.eth.account.sign_transaction(transaction, private_key=self.private_key)

        # Send the transaction
        txn_hash = self.w3.eth.send_raw_transaction(signed_txn.raw_transaction)

        # Get the transaction receipt
        txn_receipt = self.w3.eth.wait_for_transaction_receipt(txn_hash)
        
        # Check if the transaction was successful
        if txn_receipt.status == 1:
            return f"0x{txn_hash.hex()}", len(uids)
        else:
            raise Exception(f"Transaction failed: {txn_receipt}")
        
"""

# Example usage:
if __name__ == "__main__":
    # Initialize the API
    private_key = "..."  # Replace with your private key
    oli = oliAPI(private_key, is_production=False)
    
    # Create an onchain label
    contract_address = "0x498581ff718922c3f8e6a244956af099b2652b2b"
    tags = {
        'contract_name': 'Pool Manager v4',
        'is_eoa': False, 
        'deployment_tx': '0x25f482fbd94cdea11b018732e455b8e9a940b933cabde3c0c5dd63ea65e85349',
        'deployer_address': '0x2179a60856E37dfeAacA0ab043B931fE224b27B6',
        'owner_project': 'uniswap',
        'version': 4,
        'deployment_date': '2025-01-21 20:28:43',
        'source_code_verified': 'https://repo.sourcify.dev/contracts/partial_match/8453/0x498581fF718922c3f8e6A244956aF099B2652b2b/',
        'is_proxy': False
    }
    
    # Submit onchain attestation
    try:
        tx_hash = oli.create_onchain_label(contract_address, tags)
        print(f"Transaction successful with hash: {tx_hash}")
    except Exception as e:
        print(f"Error: {e}")"""

# Example usage:
# Make sure to install the correct version of eth-account (pip install eth-account==0.13.5)
#"""
# Initialize the API
private_key = "..."  # Your private key here
api = oliAPI(private_key, is_production=False)  # Use testnet

# Create and submit a label
address = '0x498581ff718922c3f8e6a244956af099b2652b2b'
tags = {
    'is_eoa': False, 
    'is_proxy': False,
    'contract_name': 'Pool Manager v4',
    'deployment_tx': '0x25f482fbd94cdea11b018732e455b8e9a940b933cabde3c0c5dd63ea65e85349',
    'deployer_address': '0x2179a60856E37dfeAacA0ab043B931fE224b27B6',
    'owner_project': 'uniswap',
    'deployment_date': '2025-01-21 20:28:43',
    'version': 4,
    'source_code_verified': 'https://repo.sourcify.dev/contracts/partial_match/8453/0x498581fF718922c3f8e6A244956aF099B2652b2b/'
}

# Submit label to OLI Label Pool as an offchain attestation
response = api.create_offchain_label(address, tags)
print(json.dumps(response, indent=2))
#"""