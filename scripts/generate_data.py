import pandas as pd
import random
from faker import Faker
from datetime import datetime

faker = Faker()

# # # Generate Products Data
# products = [
#     {"product_id": i, "product_name": faker.word(), "category": faker.word(), "price": round(random.uniform(5, 100), 2)}
#     for i in range(1, 101)
# ]

# # Generate Customers Data
# customers = [
#     {"customer_id": i, "name": faker.name(), "region": faker.state(), "signup_date": faker.date_between("-2y", "today")}
#     for i in range(1, 201)
# ]

# Generate Sales Data
sales = [
    {
        "transaction_id": i,
        "product_id": random.randint(1, 400),
        "customer_id": random.randint(1, 800),
        "amount": round(random.uniform(10, 500), 2),
        "timestamp": faker.date_time_between(start_date=datetime(2019,1,1), end_date=datetime(2022,12,31)).strftime("%Y-%m-%d %H:%M:%S")  # Format the timestamp
    }
    for i in range(4000, 5001)
]


# Save to CSV
# pd.DataFrame(products).to_csv("data/product_data.csv", index=False)
# pd.DataFrame(customers).to_csv("data/customer_data.csv", index=False)
pd.DataFrame(sales).to_csv("data/sales_data.csv", index=False)

print("Mock data generated and saved in the 'data/' folder.")
