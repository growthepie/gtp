from sqlalchemy import text
from src.db_connector import DbConnector

# Initialize the database connector
db = DbConnector()

# Connect to the database
with db.engine.connect() as conn:
    # Set the appropriate role
    conn.execute(text("SET ROLE data_team_write;"))
    
    # Query transactions from the block we loaded
    query = text("""
        SELECT 
            block_number, 
            tx_hash::text, 
            tx_type,
            from_address::text,
            to_address::text
        FROM lisk_tx 
        WHERE block_number >= 15590154 
        AND block_number < 15590164
        ORDER BY block_number, tx_type
    """)
    
    result = conn.execute(query)
    
    # Print the headers
    print("\nBLOCK  | TX_TYPE | FROM (truncated)       | TO (truncated)")
    print("-" * 90)
    
    # Print each row
    for row in result:
        # Truncate the long values for display
        tx_hash = row.tx_hash[:20] + "..." if row.tx_hash else None
        from_addr = row.from_address[:20] + "..." if row.from_address else None
        to_addr = row.to_address[:20] + "..." if row.to_address else None
        
        print(f"{row.block_number} | {row.tx_type:7} | {from_addr or 'None':25} | {to_addr or 'None'}")
    
    # Get statistics
    stats_query = text("""
        SELECT 
            tx_type, 
            COUNT(*) as count
        FROM lisk_tx
        WHERE block_number >= 15590154 
        AND block_number < 15590164
        GROUP BY tx_type
        ORDER BY tx_type
    """)
    
    stats = conn.execute(stats_query).fetchall()
    
    # Print the statistics
    print("\n--- Transaction Type Distribution ---")
    for tx_type, count in stats:
        print(f"Type {tx_type}: {count} transactions") 