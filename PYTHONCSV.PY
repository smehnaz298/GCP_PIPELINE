import csv
import random
from faker import Faker

# Initialize Faker for realistic data generation
fake = Faker()

# Generate customer data
customers = []
for customer_id in range(1, 1001):
    customers.append([
        customer_id,
        fake.name(),
        fake.email(),
        fake.phone_number(),
        fake.city()
    ])

# Generate transaction data
transactions = []
for transaction_id in range(1001, 2001):
    transactions.append([
        transaction_id,
        random.randint(1, 1000),  # Random CustomerID
        round(random.uniform(10.00, 500.00), 2),  # Random transaction amount
        "GBP",
        fake.date_between(start_date="-1y", end_date="today").isoformat()
    ])

# Save customer data to CSV
with open("customer.csv", "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["CustomerID", "Name", "Email", "Phone", "City"])
    writer.writerows(customers)

# Save transaction data to CSV
with open("transactions.csv", "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["TransactionID", "CustomerID", "Amount", "Currency", "Date"])
    writer.writerows(transactions)

print("CSV files generated successfully!")

