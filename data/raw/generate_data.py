#!/usr/bin/env python3
"""
Data generator for SQL Query Optimization Benchmarking.
Generates synthetic, reproducible data for the e-commerce schema.
"""

import random
import csv
import argparse
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Fixed seed for reproducibility
random.seed(42)

# Output directory
OUTPUT_DIR = Path(__file__).parent
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Dataset size configurations by scale
SCALE_CONFIGS = {
    "small": {
        "num_categories": 15,
        "num_products": 150,
        "num_customers": 800,
        "num_orders": 3000,
        "min_items_per_order": 1,
        "max_items_per_order": 5,
    },
    "medium": {
        "num_categories": 30,
        "num_products": 1500,
        "num_customers": 8000,
        "num_orders": 30000,
        "min_items_per_order": 1,
        "max_items_per_order": 5,
    },
    "large": {
        "num_categories": 50,
        "num_products": 5000,
        "num_customers": 50000,
        "num_orders": 200000,
        "min_items_per_order": 1,
        "max_items_per_order": 5,
    }
}

# Default to small for backward compatibility
DEFAULT_SCALE = "small"

# Sample data pools
COUNTRIES = [
    "United States", "United Kingdom", "Germany", "France", "Italy",
    "Spain", "Canada", "Australia", "Japan", "Brazil", "Mexico",
    "Netherlands", "Sweden", "Norway", "Poland"
]

CITIES = {
    "United States": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"],
    "United Kingdom": ["London", "Manchester", "Birmingham", "Liverpool", "Leeds"],
    "Germany": ["Berlin", "Munich", "Hamburg", "Frankfurt", "Cologne"],
    "France": ["Paris", "Lyon", "Marseille", "Toulouse", "Nice"],
    "Italy": ["Rome", "Milan", "Naples", "Turin", "Palermo"],
    "Spain": ["Madrid", "Barcelona", "Valencia", "Seville", "Bilbao"],
    "Canada": ["Toronto", "Vancouver", "Montreal", "Calgary", "Ottawa"],
    "Australia": ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"],
    "Japan": ["Tokyo", "Osaka", "Yokohama", "Nagoya", "Sapporo"],
    "Brazil": ["Sao Paulo", "Rio de Janeiro", "Brasilia", "Salvador", "Fortaleza"],
    "Mexico": ["Mexico City", "Guadalajara", "Monterrey", "Puebla", "Tijuana"],
    "Netherlands": ["Amsterdam", "Rotterdam", "The Hague", "Utrecht", "Eindhoven"],
    "Sweden": ["Stockholm", "Gothenburg", "Malmo", "Uppsala", "Vasteras"],
    "Norway": ["Oslo", "Bergen", "Trondheim", "Stavanger", "Bodo"],
    "Poland": ["Warsaw", "Krakow", "Gdansk", "Wroclaw", "Poznan"]
}

FIRST_NAMES = [
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer",
    "Michael", "Linda", "William", "Elizabeth", "David", "Barbara",
    "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah",
    "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa"
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
    "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez",
    "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson",
    "Martin", "Lee", "Thompson", "White", "Harris", "Sanchez"
]

CATEGORY_NAMES = [
    "Electronics", "Clothing", "Home & Garden", "Sports & Outdoors",
    "Books", "Toys & Games", "Health & Beauty", "Automotive",
    "Food & Beverages", "Pet Supplies", "Office Supplies", "Musical Instruments",
    "Jewelry", "Furniture", "Baby Products"
]

PRODUCT_NAMES = {
    "Electronics": ["Smartphone", "Laptop", "Tablet", "Headphones", "Smartwatch", "Camera", "Speaker", "Monitor", "Keyboard", "Mouse"],
    "Clothing": ["T-Shirt", "Jeans", "Jacket", "Dress", "Shoes", "Hat", "Sweater", "Shorts", "Socks", "Belt"],
    "Home & Garden": ["Lamp", "Plant Pot", "Garden Tool", "Cushion", "Curtain", "Rug", "Vase", "Mirror", "Clock", "Frame"],
    "Sports & Outdoors": ["Bicycle", "Tent", "Backpack", "Running Shoes", "Yoga Mat", "Dumbbells", "Tennis Racket", "Basketball", "Soccer Ball", "Swimming Goggles"],
    "Books": ["Novel", "Textbook", "Cookbook", "Biography", "Mystery", "Science Fiction", "Fantasy", "History", "Poetry", "Comic"],
    "Toys & Games": ["Board Game", "Puzzle", "Action Figure", "Doll", "Building Blocks", "Remote Car", "Card Game", "Stuffed Animal", "Art Set", "Musical Toy"],
    "Health & Beauty": ["Shampoo", "Soap", "Toothbrush", "Moisturizer", "Perfume", "Makeup Kit", "Hair Dryer", "Razor", "Vitamins", "Face Mask"],
    "Automotive": ["Car Battery", "Tire", "Oil Filter", "Brake Pad", "Headlight", "Wiper Blade", "Air Freshener", "Car Cover", "Floor Mat", "Steering Wheel Cover"],
    "Food & Beverages": ["Coffee", "Tea", "Chocolate", "Cereal", "Pasta", "Olive Oil", "Honey", "Jam", "Nuts", "Crackers"],
    "Pet Supplies": ["Dog Food", "Cat Litter", "Pet Toy", "Leash", "Collar", "Pet Bed", "Food Bowl", "Treats", "Grooming Brush", "Pet Carrier"],
    "Office Supplies": ["Notebook", "Pen", "Stapler", "Folder", "Binder", "Desk Organizer", "Calculator", "Printer Paper", "Envelope", "Paper Clip"],
    "Musical Instruments": ["Guitar", "Piano", "Drums", "Violin", "Flute", "Trumpet", "Microphone", "Amplifier", "Metronome", "Music Stand"],
    "Jewelry": ["Necklace", "Ring", "Earrings", "Bracelet", "Watch", "Brooch", "Anklet", "Charm", "Pendant", "Cufflinks"],
    "Furniture": ["Chair", "Table", "Sofa", "Bed", "Desk", "Bookshelf", "Cabinet", "Stool", "Wardrobe", "Coffee Table"],
    "Baby Products": ["Diapers", "Baby Bottle", "Pacifier", "Baby Clothes", "Stroller", "Car Seat", "Baby Food", "Rattle", "Bib", "Baby Monitor"]
}

ORDER_STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled"]

# Date range for orders (last 2 years)
START_DATE = datetime.now() - timedelta(days=730)
END_DATE = datetime.now()


def generate_categories(config):
    """Generate categories data."""
    categories = []
    num_categories = config["num_categories"]
    for i in range(1, num_categories + 1):
        category_name = CATEGORY_NAMES[i - 1]
        categories.append({
            "category_id": i,
            "name": category_name,
            "description": f"Products in the {category_name} category",
            "created_at": (START_DATE + timedelta(days=random.randint(0, 100))).isoformat()
        })
    return categories


def generate_products(categories, config):
    """Generate products data."""
    products = []
    product_id = 1
    num_products_target = config["num_products"]
    
    for category in categories:
        category_id = category["category_id"]
        category_name = category["name"]
        product_templates = PRODUCT_NAMES.get(category_name, ["Product"])
        
        # Generate products per category (adaptive based on target)
        products_per_category = max(1, num_products_target // len(categories))
        num_products = random.randint(products_per_category - 2, products_per_category + 2)
        for _ in range(num_products):
            if product_id > num_products_target:
                break
            
            template = random.choice(product_templates)
            product_name = f"{template} {random.randint(1, 999)}"
            
            products.append({
                "product_id": product_id,
                "category_id": category_id,
                "name": product_name,
                "description": f"High-quality {template.lower()} for everyday use",
                "price": round(random.uniform(9.99, 999.99), 2),
                "stock_quantity": random.randint(0, 500),
                "created_at": (START_DATE + timedelta(days=random.randint(0, 200))).isoformat()
            })
            product_id += 1
    
    return products


def generate_customers(config):
    """Generate customers data."""
    customers = []
    num_customers = config["num_customers"]
    for i in range(1, num_customers + 1):
        first_name = random.choice(FIRST_NAMES)
        last_name = random.choice(LAST_NAMES)
        country = random.choice(COUNTRIES)
        city = random.choice(CITIES.get(country, ["Unknown"]))
        email = f"{first_name.lower()}.{last_name.lower()}{i}@example.com"
        
        customers.append({
            "customer_id": i,
            "email": email,
            "first_name": first_name,
            "last_name": last_name,
            "country": country,
            "city": city,
            "created_at": (START_DATE + timedelta(days=random.randint(0, 400))).isoformat()
        })
    return customers


def generate_orders(customers, config):
    """Generate orders data."""
    orders = []
    num_orders = config["num_orders"]
    for i in range(1, num_orders + 1):
        customer = random.choice(customers)
        order_date = START_DATE + timedelta(
            days=random.randint(0, (END_DATE - START_DATE).days),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        
        orders.append({
            "order_id": i,
            "customer_id": customer["customer_id"],
            "order_date": order_date.isoformat(),
            "total_amount": 0.0,  # Will be calculated from order_items
            "status": random.choice(ORDER_STATUSES),
            "shipping_country": customer["country"]
        })
    return orders


def generate_order_items(orders, products, config):
    """Generate order items data."""
    order_items = []
    item_id = 1
    min_items = config["min_items_per_order"]
    max_items = config["max_items_per_order"]
    
    # Group orders by order_id for efficient lookup
    orders_by_id = {order["order_id"]: order for order in orders}
    
    for order in orders:
        order_id = order["order_id"]
        num_items = random.randint(min_items, max_items)
        order_total = 0.0
        
        for _ in range(num_items):
            product = random.choice(products)
            quantity = random.randint(1, 5)
            unit_price = product["price"]
            subtotal = round(unit_price * quantity, 2)
            order_total += subtotal
            
            order_items.append({
                "order_item_id": item_id,
                "order_id": order_id,
                "product_id": product["product_id"],
                "quantity": quantity,
                "unit_price": unit_price,
                "subtotal": subtotal
            })
            item_id += 1
        
        # Update order total
        order["total_amount"] = round(order_total, 2)
    
    return order_items


def write_csv(filename, data, fieldnames):
    """Write data to CSV file."""
    filepath = OUTPUT_DIR / filename
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"Generated {filename}: {len(data)} records")


def main():
    """Main function to generate all data files."""
    parser = argparse.ArgumentParser(
        description="Generate synthetic data for SQL Query Optimization Benchmarking"
    )
    parser.add_argument(
        "--scale",
        type=str,
        default=DEFAULT_SCALE,
        choices=["small", "medium", "large"],
        help=f"Dataset scale (default: {DEFAULT_SCALE})"
    )
    
    args = parser.parse_args()
    scale = args.scale
    config = SCALE_CONFIGS[scale]
    
    print(f"Generating synthetic data (seed=42, scale={scale})...")
    print(f"Dataset size: {config['num_customers']} customers, {config['num_orders']} orders")
    print()
    
    # Generate data in dependency order
    print("Generating categories...")
    categories = generate_categories(config)
    write_csv("categories.csv", categories, ["category_id", "name", "description", "created_at"])
    
    print("Generating products...")
    products = generate_products(categories, config)
    write_csv("products.csv", products, ["product_id", "category_id", "name", "description", "price", "stock_quantity", "created_at"])
    
    print("Generating customers...")
    customers = generate_customers(config)
    write_csv("customers.csv", customers, ["customer_id", "email", "first_name", "last_name", "country", "city", "created_at"])
    
    print("Generating orders...")
    orders = generate_orders(customers, config)
    
    print("Generating order items...")
    order_items = generate_order_items(orders, products, config)
    write_csv("order_items.csv", order_items, ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "subtotal"])
    
    # Write orders after calculating totals
    write_csv("orders.csv", orders, ["order_id", "customer_id", "order_date", "total_amount", "status", "shipping_country"])
    
    print()
    print("Data generation complete!")
    print(f"Total records: {len(categories)} categories, {len(products)} products, "
          f"{len(customers)} customers, {len(orders)} orders, {len(order_items)} order items")


if __name__ == "__main__":
    main()

