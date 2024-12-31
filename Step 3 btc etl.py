

import pandas as pd
from bitcoinrpc.authproxy import AuthServiceProxy
import hashlib
from io import StringIO
import psycopg2
from sqlalchemy import create_engine, delete, Table, MetaData
import time
from concurrent.futures import ThreadPoolExecutor
import base58


# Connect to the Bitcoin RPC
# Bitcoin RPC is the API to get information from your btc core node 

rpc_user = "miumiu"
rpc_password = "xxxxxx"
rpc_host = "127.0.0.1"
rpc_port = "8332"

rpc_connection = AuthServiceProxy(f"http://{rpc_user}:{rpc_password}@{rpc_host}:{rpc_port}")


def generate_short_id(str_1, str_2):
    """
    Combines the block height with the first 8 characters of the transaction hash.
    """
    str_2 = str_2[-8:]
    return f"{str_1}_{str_2}"

# Function to decode P2PKH (Pay-to-PubKey-Hash) script and get address
def decode_p2pkh(script):
    # Extract the 20-byte public key hash (after OP_HASH160)
    pubkey_hash = script[3:23]  # The 20-byte hash is at positions 3-22 in the script

    # Perform Base58Check encoding
    address = base58.b58encode_check(b'\x00' + pubkey_hash).decode('utf-8')  # Prefix with 0x00 for mainnet
    return address

# Function to decode P2PK (Pay-to-PubKey) script and get address
def decode_p2pk(script):
    # Extract the 65-byte public key (after the PUSH opcode 0x41)
    pubkey = script[1:66]  # The 65-byte public key is at positions 1-65 in the script

    # Perform SHA-256 and RIPEMD-160 to get the public key hash
    sha256_pubkey = hashlib.sha256(pubkey).digest()
    pubkey_hash = hashlib.new('ripemd160', sha256_pubkey).digest()

    # Perform Base58Check encoding
    address = base58.b58encode_check(b'\x00' + pubkey_hash).decode('utf-8')  # Prefix with 0x00 for mainnet
    return address

# Function to decode multisig address
def decode_multisig(script):
    # Extract the number of signatures required and the public keys involved
    sig_count = script[0]  # The number of signatures required
    pubkeys = script[1:-2]  # Exclude OP_CHECKMULTISIG and OP_N (last 2 elements)

    # Create the multisig script by combining the required signatures and public keys
    multisig_script = bytes([sig_count]) + pubkeys + bytes([script[-2]])  # Add OP_CHECKMULTISIG (last element)

    # Get the script hash (RIPEMD160(SHA256(script)))
    script_hash = hashlib.new('ripemd160', hashlib.sha256(multisig_script).digest()).digest()

    # Create the P2SH address by adding prefix 0x05 (for mainnet) to the script hash and encoding with Base58Check
    address = base58.b58encode_check(b'\x05' + script_hash).decode('utf-8')
    
    return address


def get_input_details(spent_txid, spent_index):
    """
    Fetches the value, addresses, and required_signatures of a vin by looking up the referenced vout.
    Decodes the script if the address is not found.
    """
    rpc_connection = AuthServiceProxy(f"http://{rpc_user}:{rpc_password}@{rpc_host}:{rpc_port}")

    try:
        # Fetch the transaction that the input is spending
        spent_tx = rpc_connection.getrawtransaction(spent_txid, True)
        # Get the referenced output
        spent_vout = spent_tx['vout'][spent_index]
        value = spent_vout.get('value', None)
        addresses = spent_vout.get('scriptPubKey', {}).get('address', None)
        typee = spent_vout.get('scriptPubKey', {}).get('type', None)
        
        # Decode the address if not present in vout
        if not addresses:
            script_hex = spent_vout.get('scriptPubKey', {}).get('hex', '')
            if typee == 'pubkeyhash':  # P2PKH
                addresses = decode_p2pkh(bytes.fromhex(script_hex))
            elif typee == 'pubkey':  # P2PK
                addresses = decode_p2pk(bytes.fromhex(script_hex))
            elif typee == 'multisig':  # Multisig (P2SH)
                script = bytes.fromhex(script_hex)
                addresses = decode_multisig(script)

        return value, addresses, typee
    except Exception as e:
        return None, None, None  # Return None if the referenced output cannot be found


def get_block_data_with_details(block_height):
    """
    Fetches block data, transactions, vin (inputs), and vout (outputs),
    calculates fees, and adds total input/output value columns to txn_df.
    """
    rpc_connection = AuthServiceProxy(f"http://{rpc_user}:{rpc_password}@{rpc_host}:{rpc_port}")

    try:
        # Get block hash and block data
        block_hash = rpc_connection.getblockhash(block_height)
        block = rpc_connection.getblock(block_hash, 2)

        # Prepare DataFrames
        block_data = []
        txn_data = []
        vin_data = []
        vout_data = []

        # Block-level details
        block_entry = {
            'block_hash': block['hash'],
            'size': block.get('size', None),
            'stripped_size': block.get('strippedsize', None),
            'weight': block.get('weight', None),
            'block_height': block_height,
            'version': block.get('version', None),
            'timestamp': pd.to_datetime(block.get('time', None), unit='s').strftime('%Y%m%d'),
            'transaction_count': len(block['tx'])
        }
        block_data.append(block_entry)

        # Transaction-level details
        for tx in block['tx']:
            total_input_value = 0
            total_output_value = 0
            fee = None

            # Process inputs (vin) and calculate total input value
            for index, vin in enumerate(tx['vin']):
                input_value = None
                input_addresses = None
                if 'txid' in vin and 'vout' in vin:  # Check for coinbase vin
                    input_value, input_addresses, typee = get_input_details(vin['txid'], vin['vout'])
                else:
                    input_value, input_addresses, typee = None, None, 'coinbase'
                if input_value is not None:
                    total_input_value += input_value

                # Decode the input address if missing
                if not input_addresses:
                    input_addresses = "Unknown Address"  # Fallback if no address is found

                vin_entry = {
                    'txn_id': generate_short_id(block_height, tx['txid']),
                    'block_height': block_height,
                    'block_timestamp': pd.to_datetime(block.get('time', None), unit='s').strftime('%Y%m%d'),
                    'index': index,
                    'type': typee,
                    'addresses': input_addresses,
                    'value': input_value
                }
                vin_data.append(vin_entry)

            # Process outputs (vout) and calculate total output value
            for index, vout in enumerate(tx['vout']):
                total_output_value += vout.get('value', 0)

                # Check if the address exists in the vout first
                address = vout.get('scriptPubKey', {}).get('address', None)
                
                # If no address is found, decode the script
                if not address:
                    script_hex = vout.get('scriptPubKey', {}).get('hex', '')
                    typee = vout.get('scriptPubKey', {}).get('type', None)
                    if typee == 'pubkeyhash':  # P2PKH
                        address = decode_p2pkh(bytes.fromhex(script_hex))
                    elif typee == 'pubkey':  # P2PK
                        address = decode_p2pk(bytes.fromhex(script_hex))
                    elif typee == 'multisig':  # Multisig (P2SH)
                        script = bytes.fromhex(script_hex)
                        address = decode_multisig(script)[0]  # Take the first address in the multisig set

                vout_entry = {
                    'txn_id': generate_short_id(block_height, tx['txid']),
                    'block_height': block_height,
                    'block_timestamp': pd.to_datetime(block.get('time', None), unit='s').strftime('%Y%m%d'),
                    'index': index,
                    'type': vout.get('scriptPubKey', {}).get('type', None),
                    'addresses': address,  # Use the decoded address if no address found
                    'value': vout.get('value', None)
                }
                vout_data.append(vout_entry)

            # Calculate transaction fee (if not coinbase)
            if not (len(tx['vin']) == 1 and 'coinbase' in tx['vin'][0]):
                fee = total_input_value - total_output_value
            else:
                fee = 0

            txn_entry = {
                'transaction_hash': tx['txid'],
                'txn_id': generate_short_id(block_height, tx['txid']),
                'block_height': block_height,
                'block_timestamp': pd.to_datetime(block.get('time', None), unit='s').strftime('%Y%m%d'),
                'version': tx.get('version', None),
                'input_count': len(tx['vin']),
                'output_count': len(tx['vout']),
                'is_coinbase': len(tx['vin']) == 1 and 'coinbase' in tx['vin'][0],
                'total_input_value': total_input_value,
                'total_output_value': total_output_value,
                'fee': fee
            }
            txn_data.append(txn_entry)

        # Create DataFrames
        block_df = pd.DataFrame(block_data)
        txn_df = pd.DataFrame(txn_data)
        vin_df = pd.DataFrame(vin_data)
        vout_df = pd.DataFrame(vout_data)

        return block_df, txn_df, vin_df, vout_df

    except Exception as e:
        print(f"Error: {e}")
        return None, None, None, None


# Database connection parameters
db_params = {
    'dbname': 'postgres',   # Replace with your database name
    'user': 'postgres',         # Replace with your PostgreSQL user
    'password': 'xxxxxxx', # Replace with your PostgreSQL password
    'host': 'localhost',         # PostgreSQL server host
    'port': '5432'               # Default port for PostgreSQL
}

engine = create_engine(f'postgresql+psycopg2://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["dbname"]}')




def dump_data(data , table_name , engine):

    df = data
    df.columns = [col.lower() for col in df.columns]
    # Dump data to PostgreSQL (Append data to the table if it exists)
    df.to_sql(table_name, engine, if_exists='append', index=False)


# Connect to PostgreSQL
connection = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="meomeo",
    host="localhost",
    port="5432"
)


def delete_table(connection, table_name, column_name, value):
    try:
        # Create a cursor object
        cursor = connection.cursor()

        # Use parameterized queries to prevent SQL injection
        query = f"DELETE FROM {table_name} WHERE {column_name} = %s"
        cursor.execute(query, (value,))

        # Commit the transaction
        connection.commit()
        print(f"Rows deleted from {table_name} where {column_name} = {value}")

    except Exception as e:
        # Rollback in case of error
        connection.rollback()
        print("An error occurred:", e)

    finally:
        # Close the cursor
        cursor.close()




def import_block_to_db(block_start, block_end):
    number_retry = 1000

    for i in range(block_start, block_end+1, 1):
        for attempt in range(number_retry):
            try:
                start_time = time.time()  # Start the timer

                block_df, txn_df, vin_df, vout_df = get_block_data_with_details(i)

                dump_data(block_df, 'btc_block_dim', engine)
                dump_data(txn_df, 'btc_transaction_dim', engine)
                dump_data(vin_df, 'btc_vin_fact', engine)
                dump_data(vout_df, 'btc_vout_fact', engine)

                end_time = time.time()  # End the timer
                elapsed_time = end_time - start_time  # Calculate the time taken

                print(f"Time taken to import block {i}: {elapsed_time:.2f} seconds")  # Print the time taken  
                break
            except Exception as e:
                print(f"Error processing block {i}: {e}. Retry {attempt + 1}/{number_retry}")
#                delete_table(connection, 'btc_block_dim', 'block_height', i)
#                delete_table(connection, 'btc_transaction_dim', 'block_height', i)
#                delete_table(connection, 'btc_vin_fact', 'block_height', i)
#                delete_table(connection, 'btc_vout_fact', 'block_height', i)
                time.sleep(5)  # Optional delay before retry




# Divide the block range among threads
def run_import_blocks_concurrently(start_block, end_block, num_threads):
    total_blocks = end_block - start_block + 1
    blocks_per_thread = total_blocks // num_threads

    # Create ranges for each thread
    ranges = [
        (start_block + i * blocks_per_thread, start_block + (i + 1) * blocks_per_thread - 1)
        for i in range(num_threads)
    ]
    # Adjust the last range to include remaining blocks
    ranges[-1] = (ranges[-1][0], end_block)

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(import_block_to_db, start, end) for start, end in ranges]

        # Wait for all threads to complete
        for future in futures:
            future.result()

# Example usage
run_import_blocks_concurrently(300001, 400000, 5)
