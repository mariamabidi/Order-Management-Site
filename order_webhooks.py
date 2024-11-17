import signal
import smtplib
from datetime import timedelta, datetime
from email.mime.text import MIMEText
import threading
import redis
from pymongo import MongoClient
import csv


def add_order_to_stream(order_id, st_code, quant):
    event_data = {
        "order_id": order_id,
        "stock_code": st_code,
        "quantity": quant
    }
    # Append the event to the "orders" stream
    r.xadd("orders", event_data)
    print(f"Order {order_id} added to stream.")


def get_orders(start_time):
    invoice_date_str = start_time['InvoiceDate']

    start_time = datetime.strptime(invoice_date_str, '%m/%d/%Y %H:%M')
    end_time = start_time + timedelta(minutes=10)

    query = {
        "InvoiceDate": {
            "$gte": start_time,
            "$lt": end_time
        }
    }

    orders_in_time_range = list(orders_collection.find(query))

    return orders_in_time_range, end_time


def order_webhook_creation(orders_collection):

    final = []
    with open('data.csv', mode='r', encoding='ISO-8859-1') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)

        for row in csv_reader:

            map =  {
                "order_id": row[0],
                "StockCode":row[1],
                "Description": row[2],
                "Quantity": int(row[3]),
                "InvoiceDate": row[4],
                "UnitPrice": float(row[5]),
                "CustomerID": row[6],
                "Country": row[7]
            }
            final.append(map)


    orders_collection.insert_many(final)


def stockCode_to_OMSStockCode_map():
    master = 1
    r = redis.Redis(host='localhost', port=6379, db=0)

    client = MongoClient('mongodb://localhost:27017/')
    db = client['DBSI']
    orders_collection = db['ORDER_WEBHOOKS']

    unique_values = orders_collection.distinct("StockCode")

    for value in unique_values:
        r.set(value, master)
        r.set(master,100)
        master = master + 1


def send_alert(oms_stock_code, new_quantity):
    msg = MIMEText(f"Alert: Stock for OMSStockCode {oms_stock_code} is low. Current quantity: {new_quantity}")
    msg['Subject'] = "Inventory Replenishment Alert"
    msg['From'] = "mariamdbsii@gmail.com"
    msg['To'] = "sv6218@rit.edu"

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login("mariamdbsii@gmail.com", "xuminliu")
        server.send_message(msg)
    print(f"Alert sent for OMSStockCode {oms_stock_code}. Current quantity: {new_quantity}")


def listen_for_low_stock():
    r = redis.Redis(host='localhost', port=6379, db=0)
    pubsub = r.pubsub()
    pubsub.subscribe("low-stock")
    for message in pubsub.listen():
        if message['type'] == 'message':
            print(f"Alert: {message['data'].decode()} \n")


def decrement_stock(stock_code, quantity):

    r = redis.Redis(host='localhost', port=6379, db=0)
    oms_stock_code = r.get(stock_code)

    if oms_stock_code is None:
        print(f"StockCode '{stock_code}' does not exist in Redis.")
        return

    oms_stock_code = int(oms_stock_code)
    current_quantity = r.get(oms_stock_code)

    if current_quantity is None:
        print(f"No stock found for OMSStockCode '{oms_stock_code}'.")
        return

    current_quantity = int(current_quantity)
    new_quantity = current_quantity - quantity
    r.set(oms_stock_code, new_quantity)

    threshold = int(r.get(f"{stock_code}_threshold") or 0)
    if new_quantity < threshold:
        r.publish("low-stock", f"OMSStockCode {oms_stock_code} has low stock: {new_quantity}")

    print(f"Stock for OMSStockCode {oms_stock_code} decremented. New quantity: {new_quantity}\n")


def process_orders_from_stream(r):
    last_id = '0'  # Starting point to read from the beginning of the stream
    running = True  # Flag to control the loop

    def handle_exit_signal(signal_number, frame):
        nonlocal running
        running = False
        print("\nGraceful shutdown initiated...")


    while running:
        # Read new entries from the stream with a timeout to periodically check the running flag
        events = r.xread({"orders": last_id}, block=1000)  # Block for 1000 milliseconds (1 second)

        if not events:  # If no new events are found, continue the loop
            handle_exit_signal(signal.SIGINT, None)

        for stream, messages in events:
            for message_id, message_data in messages:
                order_id = message_data[b"order_id"].decode()
                stock_code = message_data[b"stock_code"].decode()
                quantity = int(message_data[b"quantity"])

                print(f"Processing order {order_id}: {quantity} units of {stock_code}")

                # Process the order (e.g., update stock, send alerts, etc.)
                decrement_stock(stock_code, quantity)

                # Update the last processed ID
                last_id = message_id

    print("Stopped processing orders.")


def add_order_to_sorted_set(stock_code, quantity):
    # Increment the score of the item in the "top_orders" sorted set
    r.zincrby("top_orders", quantity, stock_code)


def get_top_ordered_items(n=10):
    # Retrieve the top n items from the sorted set
    top_items = r.zrevrange("top_orders", 0, n - 1, withscores=True)
    print("Top ordered items:")
    for item, score in top_items:
        print(f"Stock Code: {item.decode()}, Orders: {int(score)}")
    return top_items


def track_stock_update(stock_code, day_of_month):
    """
    Track stock updates for a stock item on a specific day.
    stock_code: Stock item code (e.g., 'A123')
    day_of_month: Day of the month (1-31)
    """
    # Bitmap key is stock code, bit position corresponds to the day of the month
    bitmap_key = f"stock_update:{stock_code}"

    # Use BITSET to set the bit for the given day
    r.setbit(bitmap_key, day_of_month - 1, 1)  # day_of_month - 1 because bit positions start from 0

    print(f"Stock update for {stock_code} on Day {day_of_month} marked as processed.")


# Helper function to check if stock was updated on a specific day
def was_stock_updated(stock_code, day_of_month):
    """
    Check if a stock item was updated on a specific day.
    stock_code: Stock item code (e.g., 'A123')
    day_of_month: Day of the month (1-31)
    """
    bitmap_key = f"stock_update:{stock_code}"

    # Use BITGET to check if the bit for the specific day is set to 1
    updated = r.getbit(bitmap_key, day_of_month - 1)  # day_of_month - 1 to match bit positions

    if updated:
        print(f"Stock {stock_code} was updated on Day {day_of_month}.")
        return True
    else:
        print(f"Stock {stock_code} was NOT updated on Day {day_of_month}.")
        return False


def track_order_completion(order_id, day_of_month):
    """
    Track order completion status for a specific order ID on a specific day.
    """
    bitmap_key = f"order_completion:{order_id}"
    r.setbit(bitmap_key, day_of_month - 1, 1)  # Mark as completed on the given day
    print(f"Order {order_id} completed on Day {day_of_month}.")

def was_order_completed(order_id, day_of_month):
    """
    Check if an order was completed on a specific day.
    """
    bitmap_key = f"order_completion:{order_id}"
    completed = r.getbit(bitmap_key, day_of_month - 1)
    if completed:
        print(f"Order {order_id} was completed on Day {day_of_month}.")
        return True
    else:
        print(f"Order {order_id} was NOT completed on Day {day_of_month}.")
        return False






if __name__ == '__main__':

    listener_thread = threading.Thread(target=listen_for_low_stock, daemon=True)
    listener_thread.start()

    client = MongoClient('mongodb://localhost:27017/')
    db = client['DBSI']
    orders_collection = db['ORDER_WEBHOOKS']
    r = redis.Redis(host='localhost', port=6379, db=0)

    #order_webhook_creation(orders_collection)
    #stockCode_to_OMSStockCode_map()

    start_time = orders_collection.find_one({}, {"InvoiceDate": 1})


    while start_time:
        orders, next_start_time = get_orders(start_time)

        if not orders:
            break

        for order in orders:
            stock_code = order["StockCode"]
            quantity = order["Quantity"]
            r.set(f"{stock_code}_threshold", 70)
            add_order_to_stream(order["order_id"], stock_code, quantity)
            add_order_to_sorted_set(stock_code, quantity)

        process_orders_from_stream(r)
        start_time = {"InvoiceDate": next_start_time.strftime('%m/%d/%Y %H:%M')}

    get_top_ordered_items()

    # Updating stocks


