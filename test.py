import order_webhooks
import order_webhooks_mongo
import time




if __name__ == '__main__':
    start_time1 = time.time()  # Start time
    order_webhooks.main()  # Call function one
    end_time1 = time.time()  # End time

    # Measure the time for the second function
    start_time2 = time.time()  # Start time
    order_webhooks_mongo.main() # Call function two
    end_time2 = time.time()  # End time
    print(f"Function One execution time: {end_time1 - start_time1:.4f} seconds")

    print(f"Function Two execution time: {end_time2 - start_time2:.4f} seconds")
