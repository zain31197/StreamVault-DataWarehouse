import pandas as pd
import threading
import queue
import sqlalchemy
from sqlalchemy import text
from datetime import datetime
import time
import sys
import getpass

# DB_URI = "mysql+pymysql://root:zain3119@localhost:3306/walmart_warehouse"
TRANSACTION_CSV = 'transactional_data.csv'
CUSTOMER_CSV = 'customer_master_data.csv'
PRODUCT_CSV = 'product_master_data.csv'


HASH_TABLE_SIZE = 10000       # hS: total slots for stream tuples
DISK_PARTITION_SIZE = 500     # vP: tuples to load from R into disk buffer
STREAM_BUFFER_SIZE = 5000
FACT_BATCH_SIZE = 1000        # How many facts to insert at once




print("--- Database Configuration ---")
db_user = input("Username (default: root): ").strip() or "root"
db_pass = getpass.getpass(f"Password for {db_user}: ") # Hides password
db_host = input("Host (default: localhost): ").strip() or "localhost"
db_port = input("Port (default: 3306): ").strip() or "3306"
db_name = input("Database Name (default: walmart_warehouse): ").strip() or "walmart_warehouse"


DB_URI = f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
print(f"Connecting to: mysql+pymysql://{db_user}:***@{db_host}:{db_port}/{db_name}")


# ----------------- Global Control ------------------
engine = sqlalchemy.create_engine(DB_URI, pool_size=10, max_overflow=20)
stream_buffer = queue.Queue(maxsize=STREAM_BUFFER_SIZE)
stop_event = threading.Event() 

# ----------------- Data Loading (R Relations) ------------------
try:
    print("Loading and indexing customer master data (R1)...")
    customer_df = pd.read_csv(CUSTOMER_CSV, dtype={'Customer_ID': str})
    
    # FIX: Strip spaces from column names
    customer_df.columns = customer_df.columns.str.strip()


    customer_df.rename(columns={
        'Customer_ID': 'CustomerID',
        'Stay_In_Current_City_Years': 'StayInCurrentCityYears',
        'Marital_Status': 'MaritalStatus'
    }, inplace=True)
    
    customer_df.set_index('CustomerID', inplace=True)
    print(f"Customer master data (R1) loaded. ({len(customer_df)} rows)")

    print("Loading product master data (R2) into memory...")
    product_df = pd.read_csv(PRODUCT_CSV, dtype={'Product_ID': str, 'storeID': str, 'supplierID': str})

    # FIX: Strip spaces from column names
    product_df.columns = product_df.columns.str.strip()
    
    # Rename ALL CSV columns to match SQL Schema
    product_df.rename(columns={
        'Product_ID': 'ProductID',
        'Product_Category': 'ProductCategory',
        'price$': 'Price',
        'storeID': 'StoreID',
        'supplierID': 'SupplierID',
        'storeName': 'StoreName',
        'supplierName': 'SupplierName'
    }, inplace=True)

    product_map = {row['ProductID'].strip(): row for _, row in product_df.iterrows()}
    print(f"Product master data (R2) loaded. ({len(product_map)} rows)")

    store_df = product_df[['StoreID', 'StoreName']].drop_duplicates()
    supplier_df = product_df[['SupplierID', 'SupplierName']].drop_duplicates()

except FileNotFoundError as e:
    print(f"Error: Missing master data file. {e}")
    sys.exit(1)
except KeyError as e:
    print(f"FATAL CSV ERROR: A column name is wrong in your CSV file. {e}")
    sys.exit(1)
except Exception as e:
    print(f"Error loading master data: {e}")
    sys.exit(1)


# ----------------- Upsert Utilities -----------------
# (All functions are updated to use lowercase table names)
def upsert_dim_time(conn, date_value):
    try:
        as_date = pd.to_datetime(date_value)
        params = {
            "DateKey": as_date.strftime('%Y-%m-%d'),
            "DayOfWeek": as_date.strftime('%A'),
            "Month": as_date.strftime('%B'),
            "Quarter": f"Q{((as_date.month-1)//3)+1}",
            "Year": as_date.year
        }
        conn.execute(text("""
            INSERT IGNORE INTO dim_time (DateKey, DayOfWeek, Month, Quarter, Year)
            VALUES (:DateKey, :DayOfWeek, :Month, :Quarter, :Year)
        """), params)
        return params["DateKey"]
    except Exception as e:
        print(f"Error upserting time: {date_value}, {e}")
        return None

# ----------------- Stream Loader Thread -----------------
def stream_loader():
    print("[LoaderThread] Started.")
    try:
        seen = set()
        txn_df_full = pd.read_csv(TRANSACTION_CSV, dtype={'Customer_ID': str, 'Product_ID': str, 'orderID': str})
        
        # FIX: Strip spaces from column names
        txn_df_full.columns = txn_df_full.columns.str.strip()
        
        txn_df_full.rename(columns={
            'Customer_ID': 'CustomerID',
            'Product_ID': 'ProductID'
        }, inplace=True)

        while not stop_event.is_set():
            new_txns = 0
            for idx, txn in txn_df_full.iterrows():
                txn_id = f"{txn['orderID']}-{txn['ProductID']}-{idx}" 
                if txn_id not in seen:
                    while not stop_event.is_set():
                        try:
                            stream_buffer.put(txn, timeout=1) 
                            seen.add(txn_id)
                            new_txns += 1
                            break 
                        except queue.Full:
                            continue 
            
            if new_txns == 0:
                print("[LoaderThread] No new transactions found. Stopping.")
                break 
            
            print(f"[LoaderThread] Added {new_txns} new txns to buffer. Stopping (end of file).")
            break # Read the file once and stop
    except FileNotFoundError:
        print(f"[LoaderThread] Error: '{TRANSACTION_CSV}' not found.")
    except Exception as e:
        print(f"[LoaderThread] Error: {e}")
    finally:
        print("[LoaderThread] Stopped.")
        stop_event.set() 

# ----------------- HYBRIDJOIN Data Structures -----------------
class Node:
    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.prev = None
        self.next = None

class DoublyLinkedList:
    def __init__(self):
        self.head = None
        self.tail = None
        self.size = 0
    def is_empty(self): return self.size == 0
    def append(self, node):
        if self.is_empty(): self.head = self.tail = node
        else:
            self.tail.next = node
            node.prev = self.tail
            self.tail = node
        self.size += 1
    def get_oldest_node(self): return self.head
    def remove_node(self, node):
        if node.prev: node.prev.next = node.next
        else: self.head = node.next
        if node.next: node.next.prev = node.prev
        else: self.tail = node.prev
        self.size -= 1
        node.prev = node.next = None

# ----------------- HYBRIDJOIN Algorithm Thread -----------------
def hybridjoin_and_load():
    print("[JoinThread] Started. Populating initial dimension tables...")

  
    try:
        with engine.connect() as conn:
            with conn.begin():
                
                print("[JoinThread] Loading dim_customer...")
                cust_records = customer_df.reset_index().to_dict('records')
                conn.execute(text("""
                    INSERT IGNORE INTO dim_customer (
                        CustomerID, Gender, Age, Occupation, 
                        City_Category, StayInCurrentCityYears, MaritalStatus
                    ) VALUES (
                        :CustomerID, :Gender, :Age, :Occupation, 
                        :City_Category, :StayInCurrentCityYears, :MaritalStatus
                    )
                """), cust_records)

                print("[JoinThread] Loading dim_product...")
                prod_records = product_df[['ProductID', 'ProductCategory', 'Price']].to_dict('records')
                conn.execute(text("""
                    INSERT IGNORE INTO dim_product (
                        ProductID, ProductCategory, Price
                    ) VALUES (
                        :ProductID, :ProductCategory, :Price
                    )
                """), prod_records)

                print("[JoinThread] Loading dim_store...")
                store_records = store_df.to_dict('records')
                conn.execute(text("""
                    INSERT IGNORE INTO dim_store (StoreID, StoreName) 
                    VALUES (:StoreID, :StoreName)
                """), store_records)
                
                print("[JoinThread] Loading dim_supplier...")
                supplier_records = supplier_df.to_dict('records')
                conn.execute(text("""
                    INSERT IGNORE INTO dim_supplier (SupplierID, SupplierName) 
                    VALUES (:SupplierID, :SupplierName)
                """), supplier_records)

    except Exception as e:
        print(f"[JoinThread] FATAL Error during initial dimension load: {e}")
        stop_event.set() 
        return 

    print("[JoinThread] Initial dimension tables populated.")

  
    fact_batch = [] 
    seen_dates = set() 
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT DateKey FROM dim_time"))
            for row in result:
                seen_dates.add(row[0].strftime('%Y-%m-%d'))
        print(f"[JoinThread] Pre-populated {len(seen_dates)} dates from dim_time.")
    except Exception as e:
        print(f"[JoinThread] Could not pre-populate dates: {e}")


    print("[JoinThread] Starting HYBRIDJOIN main loop...")
    total_facts_loaded = 0
    

    w = HASH_TABLE_SIZE
    hash_table = dict() 
    queue = DoublyLinkedList()
    
    try:
        with engine.connect() as conn: 
           
            conn.execute(text("SET FOREIGN_KEY_CHECKS=0;"))
            conn.commit()
            
            while not stop_event.is_set() or not queue.is_empty() or not stream_buffer.empty():
                
                # --- Step 2: Load Stream S ---
                loaded_count = 0
                while loaded_count < w and not stream_buffer.empty():
                    try:
                        txn = stream_buffer.get_nowait()
                        key = str(txn['CustomerID']).strip()
                        node = Node(key=key, value=txn)
                        queue.append(node)
                        hash_table.setdefault(key, []).append(node)
                        loaded_count += 1
                    except queue.Empty: break
                w -= loaded_count
                
                if queue.is_empty():
                    if stop_event.is_set() and stream_buffer.empty():
                        break 
                    time.sleep(0.1) 
                    continue

                # --- Step 3: Load Relation R ---
                oldest_node = queue.get_oldest_node()
                probe_key = oldest_node.key
                
                try:
                    start_index = customer_df.index.get_loc(probe_key)
                    disk_buffer = customer_df.iloc[start_index : start_index + DISK_PARTITION_SIZE]
                except KeyError:
                    queue.remove_node(oldest_node)
                    if probe_key in hash_table and oldest_node in hash_table[probe_key]:
                        hash_table[probe_key].remove(oldest_node)
                        if not hash_table[probe_key]: del hash_table[probe_key]
                    w += 1 
                    continue
                    
                # --- Step 4: Probe H with R; Join, and BATCH ---
                freed_slots = 0
                
                with conn.begin(): 
                    for cust_id_str, customer_row in disk_buffer.iterrows():
                        disk_key = str(cust_id_str).strip()
                        
                        if disk_key in hash_table:
                            matched_nodes = hash_table[disk_key]
                            
                            for node in list(matched_nodes):
                                txn = node.value
                                prod_id = str(txn["ProductID"]).strip()
                                prod_row = product_map.get(prod_id)

                                if prod_row is not None:
                                    try:
                                        date_key_str = pd.to_datetime(txn["date"]).strftime('%Y-%m-%d')
                                        if date_key_str not in seen_dates:
                                            upsert_dim_time(conn, date_key_str)
                                            seen_dates.add(date_key_str)
                                        
                                        qty = int(txn["quantity"])
                                        price = float(prod_row['Price'])
                                        
                                        fact_batch.append({
                                            "OrderID": str(txn['orderID']), 
                                            "CustomerID": disk_key,
                                            "ProductID": prod_id,
                                            "StoreID": str(prod_row['StoreID']), 
                                            "SupplierID": str(prod_row['SupplierID']), 
                                            "DateKey": date_key_str,
                                            "Quantity": qty,
                                            "TotalAmount": qty * price
                                        })
                                        
                                    except Exception as e:
                                        print(f"Error processing txn: {e} | DATA: {txn}")

                                queue.remove_node(node)
                                matched_nodes.remove(node)
                                freed_slots += 1
                            
                            if not matched_nodes: del hash_table[disk_key]

                    # --- Execute Batch Insert (inside the chunk transaction) ---
                    if fact_batch and len(fact_batch) >= FACT_BATCH_SIZE:
                        conn.execute(text("""
                            INSERT INTO fact_sales 
                                (OrderID, CustomerID, ProductID, StoreID, SupplierID, DateKey, Quantity, TotalAmount)
                            VALUES 
                                (:OrderID, :CustomerID, :ProductID, :StoreID, :SupplierID, :DateKey, :Quantity, :TotalAmount)
                        """), fact_batch)
                        total_facts_loaded += len(fact_batch)
                        fact_batch = [] # Clear batch after insert

                w += freed_slots
            
            # --- Load any remaining facts ---
            if fact_batch:
                with conn.begin():
                    conn.execute(text("""
                        INSERT INTO fact_sales 
                            (OrderID, CustomerID, ProductID, StoreID, SupplierID, DateKey, Quantity, TotalAmount)
                        VALUES 
                            (:OrderID, :CustomerID, :ProductID, :StoreID, :SupplierID, :DateKey, :Quantity, :TotalAmount)
                    """), fact_batch)
                    total_facts_loaded += len(fact_batch)
                    print(f"[JoinThread]... Final batch loaded {len(fact_batch)} facts. Total: {total_facts_loaded}")
                    fact_batch = []



                    


            with conn.begin():
                conn.execute(text("SET FOREIGN_KEY_CHECKS=1;"))
            
    except Exception as e:
        print(f"[JoinThread] FATAL ERROR in main loop: {e}")
        stop_event.set()
    finally:
        print(f"[JoinThread] Stopped. Total facts loaded: {total_facts_loaded}")

# ----------------- Main Execution -----------------
if __name__ == "__main__":
    print("Starting ETL process... Press Ctrl+C to stop.")
    
    loader_thread = threading.Thread(target=stream_loader)
    join_thread = threading.Thread(target=hybridjoin_and_load)
    
    try:
        loader_thread.start()
        join_thread.start()
        while join_thread.is_alive():
            join_thread.join(timeout=1)
    except KeyboardInterrupt:
        print("\nShutdown signal received...")
        stop_event.set()
    finally:
        loader_thread.join()
        join_thread.join()
        print("ETL process finished.")