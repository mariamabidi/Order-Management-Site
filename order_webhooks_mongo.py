import signal
import smtplib
from datetime import timedelta, datetime
from email.mime.text import MIMEText
import threading
import redis
from pymongo import MongoClient
import csv
from datetime import datetime
import json


def stockCode_to_OMSStockCode_map():
    master = 1
    r = redis.Redis(host='localhost', port=6379, db=0)

    client = MongoClient('mongodb://localhost:27017/')
    db = client['DBSI']
    orders_collection = db['ORDER_WEBHOOKS_MONGO']

    unique_values = orders_collection.distinct("StockCode")

    set_stocks = []
    market_to_oms = []
    OMS_to_Marketplace_map = []

    for value in unique_values:
        market_to_oms.append({"marketplace":str(value),"oms":str(master)})
        set_stocks.append({"oms_code": str(master),"quantity":str(100)})

        marketplace_list = [str(value), str(value) + "_amazon", str(value) + "_ebay",
                            str(value) + "_temu", str(value) + "_shopify"]
        OMS_to_Marketplace_map.append(
            {"oms_code": str(master), "marketplaces": str(marketplace_list)})
        master = master + 1

    quantity = db['QUANTITY']
    market2oms = db['MARKETPLACE_TO_OMS']
    OMS_to_all_market = db['OMS_TO_ALL_MARKETPLACE']

    quantity.insert_many(set_stocks)
    market2oms.insert_many(market_to_oms)
    OMS_to_all_market.insert_many(OMS_to_Marketplace_map)





def order_webhook_creation(orders_collection):

    final = []
    with open('data.csv', mode='r', encoding='ISO-8859-1') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)

        for row in csv_reader:
            date_string = row[4]

            # Convert the string to a datetime object
            date_object = datetime.strptime(date_string, '%m/%d/%Y %H:%M')
            map =  {
                "order_id": row[0],
                "StockCode":row[1],
                "Description": row[2],
                "Quantity": int(row[3]),
                "InvoiceDate": date_object,
                "UnitPrice": row[5],
                "CustomerID": row[6],
                "Country": row[7],
                "Status" : "Created"
            }
            final.append(map)


    orders_collection.insert_many(final)

def get_orders(start_time):

    #start_time = datetime.strptime(invoice_date_str, '%m/%d/%Y %H:%M')
    end_time = start_time + timedelta(minutes=10)

    query = {
        "InvoiceDate": {
            "$gte": start_time,
            "$lt": end_time
        }
    }

    orders_in_time_range = list(orders_collection.find(query))

    return orders_in_time_range, end_time

def send_newQuantity(oms_code, quantity,time):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['DBSI']
    orders_collection = db['ORDER_WEBHOOKS_MONGO']
    current_stock = db['QUANTITY']
    OMS_to_market = db['MARKETPLACE_TO_OMS']
    OMS_to_all_market = db['OMS_TO_ALL_MARKETPLACE']

    marketplace_list = OMS_to_all_market.find_one({"oms_code":str(oms_code)})
    marketplace_list = marketplace_list["marketplaces"]
    marketplace_list = marketplace_list.split(",")
    inventory_update = []
    for marketplace in marketplace_list:
        update = {"StockCode": marketplace, "quantity": quantity ,"UpdateTime":str(time)}
        inventory_update.append(update)

    with open('invntory_updates_mongo.json', 'a') as file:
        for item in inventory_update:
            json.dump(item, file)
            file.write('\n')


def process_orders(order_list,start_time):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['DBSI']
    orders_collection = db['ORDER_WEBHOOKS_MONGO']
    current_stock = db['QUANTITY']
    OMS_to_market = db['MARKETPLACE_TO_OMS']
    OMS_to_all_market = db['OMS_TO_ALL_MARKETPLACE']

    for order in order_list:
        oms_code = OMS_to_market.find_one(
            {"marketplace": str(order["stock_code"])})
        stock_now = current_stock.find_one({"oms_code": str(oms_code["oms"])})

        if stock_now is None:
            print(
                f"StockCode '{order['stock_code']}' does not exist in Database.")
        else:
            # Convert stock quantity to int if it's stored as a string
            try:
                stock_now_quantity = int(
                    stock_now["quantity"])  # Ensure it's an integer
            except ValueError:
                print(
                    f"Invalid quantity for StockCode '{order['stock_code']}', not a number.")
                continue  # Skip this order if quantity isn't numeric

            if stock_now_quantity - order["quantity"] < 0:
                print("No stock left. Alert sent. Order cancelled\n")
                orders_collection.update_one({"StockCode": order["stock_code"]},
                                             {"$set": {"Status": "Cancelled"}})
                print("Order " + order["order_id"] + " cancelled.")
            else:
                # Use $inc operation to decrement the quantity
                current_stock.update_one(
                    {"oms_code": str(oms_code["oms"])},
                    {"$set": {"quantity": str(stock_now_quantity - order["quantity"])}})


                print(
                    f"Stock for StockCode {order['stock_code']} decremented. New quantity: {stock_now_quantity - order['quantity']}\n")
                orders_collection.update_one({"order_id": order["order_id"]},
                                             {"$set": {"Status": "Completed"}})
                print("Order "+order["order_id"]+" completed successfully.")
                send_newQuantity(oms_code["oms"], stock_now_quantity - order["quantity"], start_time)

if __name__ == '__main__':


    progress_path = "progress.json"
    with open(progress_path, 'r') as json_file:
        loaded_state = json.load(json_file)
        start_time = datetime.strptime(loaded_state['start_time'], '%Y-%m-%d %H:%M:%S')



    client = MongoClient('mongodb://localhost:27017/')
    db = client['DBSI']
    orders_collection = db['ORDER_WEBHOOKS_MONGO']
    current_stock = db['QUANTITY']

    #order_webhook_creation(orders_collection)
    #stockCode_to_OMSStockCode_map()

    count = 0
    order_list = []
    while count < 3:
        orders, start_time = get_orders(start_time)

        if not orders:
            break

        for order in orders:
            stock_code = order["StockCode"]
            quantity = order["Quantity"]
            process = {"order_id": order["order_id"], "quantity": quantity, "stock_code": stock_code}
            order_list.append(process)
        process_orders(order_list,start_time)
        order_list = []
        count = count + 1

        # start_time = {"InvoiceDate": next_start_time.strftime('%m/%d/%Y %H:%M')}


    current_state = {"start_time": str(start_time)}

    with open('progress.json', 'w') as json_file:
        json.dump(current_state, json_file)
