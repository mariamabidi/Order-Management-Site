import smtplib
from datetime import timedelta, datetime
from email.mime.text import MIMEText

import redis
from pymongo import MongoClient
import csv


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
            print(f"Alert: {message['data'].decode()}")


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
        # send_alert(oms_stock_code, new_quantity)
        r.publish("low-stock", f"OMSStockCode {oms_stock_code} has low stock: {new_quantity}")
        listen_for_low_stock()

    print(f"Stock for OMSStockCode {oms_stock_code} decremented. New quantity: {new_quantity}\n")



if __name__ == '__main__':

    client = MongoClient('mongodb://localhost:27017/')
    db = client['DBSI']
    orders_collection = db['ORDER_WEBHOOKS']
    r = redis.Redis(host='localhost', port=6379, db=0)

    stockCode_to_OMSStockCode_map()

    start_time = orders_collection.find_one({}, {"InvoiceDate": 1})


    while start_time:
        orders, next_start_time = get_orders(start_time)

        if not orders:
            break

        for order in orders:
            stock_code = order["StockCode"]
            quantity = order["Quantity"]
            r.set(f"{stock_code}_threshold", 70)
            decrement_stock(stock_code, quantity)

        start_time = {"InvoiceDate": next_start_time.strftime('%m/%d/%Y %H:%M')}


