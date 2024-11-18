import pymongo
import pprint
import order_webhooks
import json
import redis


def get_inventroy():
        # Connect to Redis (assuming default settings, change as needed)
    r = redis.Redis(host='localhost', port=6379, db=0)


    result = {}

    for key in range(1,100):
            # Retrieve value for each key from Redis
        value = r.get(str(key))  # Assuming the values are stored as simple strings
        if value:  # Check if the key exists in Redis
                result[key] = value
        else:
            result[key] = "Key not found"

        # Pretty print the result
    pprint.pprint(result)

def get_updates():
    with open('invntory_updates.json', 'r') as file:
        # Read the JSON data from the file
        data = file.read()

        # Pretty print the first 20 lines (or less, depending on the size of the JSON)
        # Using pprint for better formatting
        for line in data.split('\n')[:20]:
            pprint.pprint(line)


def display_orders():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client['DBSI']
    collection = db['ORDER_WEBHOOKS']

    # Fetch data from MongoDB
    data = collection.find_one({"Status": "Created"}, {'_id': 0})
    data1 = collection.find_one({"Status": "Created"}, {'_id': 0})
    data2 = collection.find_one({"Status": "Created"}, {'_id': 0})

    print("\n")
    # Pretty print the data
    pprint.pprint(data, indent=4)
    print("\n")

    pprint.pprint(data1, indent=4)
    print("\n")

    pprint.pprint(data2, indent=4)


def tracking_orders():
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client['DBSI']
    collection = db['ORDER_WEBHOOKS']

    # Fetch data from MongoDB
    data = collection.find_one({"Status": "Created"}, {'_id': 0})
    data1 = collection.find_one({"Status": "Cancelled"}, {'_id': 0})
    data2 = collection.find_one({"Status": "Completed"}, {'_id': 0})

    print("\n")
    # Pretty print the data
    pprint.pprint(data, indent=4)
    print("\n")

    pprint.pprint(data1, indent=4)
    print("\n")

    pprint.pprint(data2, indent=4)




def view_inventory():
    print("Viewing inventory...")
    get_inventroy()



def process_order():
    print("Processing order...")
    order_webhooks.main()



def track_order():
    print("Tracking order...")
    tracking_orders()


def send_alerts():
    print("Sending updates...")
    get_updates()



def exit_program():
    print("Exiting program...")
    exit()


# Function to display the menu and get user input
def display_menu():
    while True:
        print("\n=== Main Menu ===")
        print("1. View Inventory")
        print("2. Process Order")
        print("3. Track Order")
        print("4. Send Updates")
        print("Press 'q' to Exit")

        # Get user input
        choice = input("Enter your choice (1-4) or 'q' to exit: ")

        # Execute function based on user choice
        if choice == "1":
            view_inventory()
        elif choice == "2":
            process_order()
        elif choice == "3":
            track_order()
        elif choice == "4":
            send_alerts()
        elif choice.lower() == 'q':
            exit_program()
        else:
            print(
                "Invalid choice. Please enter a number between 1 and 4, or 'q' to exit.")


# Call the menu function to run the program
display_menu()