import csv
import random
import datetime
from typing import List, Dict, Any

# Configuration for mock data
NUM_USERS = 50
NUM_PRODUCTS = 100
NUM_ORDERS = 500
PRODUCT_CATEGORIES = ["Electronics", "Books", "Clothing", "Home Goods", "Sports", "Toys"]
CITIES = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego"]

def generate_users(filename: str) -> List[Dict[str, Any]]:
    users = []
    for i in range(NUM_USERS):
        user_id = f"user{i+1:03}"
        name = f"User {i+1}" # Simple name, can be enhanced with Faker if available
        age = random.randint(18, 70)
        city = random.choice(CITIES)
        users.append({"user_id": user_id, "name": name, "age": age, "city": city})
    
    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["user_id", "name", "age", "city"])
        writer.writeheader()
        writer.writerows(users)
    print(f"Generated {len(users)} users in {filename}")
    return users

def generate_products(filename: str) -> List[Dict[str, Any]]:
    products = []
    for i in range(NUM_PRODUCTS):
        product_id = f"prod{i+1:03}"
        product_name = f"{random.choice(PRODUCT_CATEGORIES)} Item {i+1}"
        category = random.choice(PRODUCT_CATEGORIES)
        price = round(random.uniform(5.0, 500.0), 2)
        products.append({
            "product_id": product_id,
            "product_name": product_name,
            "category": category,
            "price": price
        })

    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["product_id", "product_name", "category", "price"])
        writer.writeheader()
        writer.writerows(products)
    print(f"Generated {len(products)} products in {filename}")
    return products

def generate_orders(filename: str, users: List[Dict[str, Any]], products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    orders = []
    start_date = datetime.date(2022, 1, 1)
    end_date = datetime.date(2023, 12, 31)
    
    for i in range(NUM_ORDERS):
        order_id = f"order{i+1:03}"
        user = random.choice(users)
        product = random.choice(products)
        quantity = random.randint(1, 5)
        
        # Generate a random date for the order
        time_between_dates = end_date - start_date
        days_between_dates = time_between_dates.days
        random_number_of_days = random.randrange(days_between_dates)
        order_date = start_date + datetime.timedelta(days=random_number_of_days)
        
        orders.append({
            "order_id": order_id,
            "user_id": user["user_id"],
            "product_id": product["product_id"],
            "quantity": quantity,
            "order_date": order_date.strftime("%Y-%m-%d")
        })

    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["order_id", "user_id", "product_id", "quantity", "order_date"])
        writer.writeheader()
        writer.writerows(orders)
    print(f"Generated {len(orders)} orders in {filename}")
    return orders

if __name__ == "__main__":
    print("Starting mock data generation...")
    # Define file paths
    users_file = "data/mock/users.csv"
    products_file = "data/mock/products.csv"
    orders_file = "data/mock/orders.csv"

    # Ensure the script can be run from the project root or its own directory
    import os
    if not os.path.exists("data/mock"):
         if os.path.exists("../data/mock"): # If run from scripts/
             users_file = "../data/mock/users.csv"
             products_file = "../data/mock/products.csv"
             orders_file = "../data/mock/orders.csv"
         else: # Fallback if structure is unexpected, assumes data/mock exists or will be created by this script
             os.makedirs("data/mock", exist_ok=True)


    generated_users = generate_users(users_file)
    generated_products = generate_products(products_file)
    generate_orders(orders_file, generated_users, generated_products)
    print("Mock data generation complete.")
    print(f"Mock data saved in: {os.path.abspath(os.path.join(os.getcwd(), users_file, '..'))}")
