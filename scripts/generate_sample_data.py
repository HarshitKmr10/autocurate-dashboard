#!/usr/bin/env python3
"""Generate sample CSV files for testing autocurate dashboard."""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

def generate_ecommerce_data():
    """Generate e-commerce sample data."""
    print("🛒 Generating e-commerce data...")
    
    # Generate dates for the last 12 months
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # Generate 1000 orders
    n_orders = 1000
    data = []
    
    for i in range(n_orders):
        order_date = random.choice(dates)
        customer_id = f"CUST_{random.randint(1000, 9999)}"
        product_category = random.choice(['Electronics', 'Clothing', 'Books', 'Home & Garden', 'Sports'])
        product_name = f"Product_{random.randint(1, 100)}"
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(10, 500), 2)
        total_amount = quantity * unit_price
        payment_method = random.choice(['Credit Card', 'PayPal', 'Bank Transfer'])
        shipping_country = random.choice(['USA', 'Canada', 'UK', 'Germany', 'France'])
        
        data.append({
            'order_id': f"ORD_{i+1:04d}",
            'order_date': order_date.strftime('%Y-%m-%d'),
            'customer_id': customer_id,
            'customer_name': f"Customer {customer_id}",
            'product_category': product_category,
            'product_name': product_name,
            'quantity': quantity,
            'unit_price': unit_price,
            'total_amount': total_amount,
            'payment_method': payment_method,
            'shipping_country': shipping_country,
            'order_status': random.choice(['Completed', 'Processing', 'Shipped', 'Cancelled'])
        })
    
    df = pd.DataFrame(data)
    df.to_csv('data/sample_ecommerce.csv', index=False)
    print(f"✅ Generated {len(df)} e-commerce records")
    return df

def generate_finance_data():
    """Generate finance/investment sample data."""
    print("💰 Generating finance data...")
    
    # Generate dates for the last 2 years
    end_date = datetime.now()
    start_date = end_date - timedelta(days=730)
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # Generate 500 transactions
    n_transactions = 500
    data = []
    
    for i in range(n_transactions):
        transaction_date = random.choice(dates)
        account_type = random.choice(['Savings', 'Checking', 'Investment', 'Credit'])
        transaction_type = random.choice(['Deposit', 'Withdrawal', 'Transfer', 'Investment', 'Interest'])
        amount = round(random.uniform(100, 10000), 2)
        if transaction_type in ['Withdrawal', 'Transfer']:
            amount = -amount
        
        data.append({
            'transaction_id': f"TXN_{i+1:04d}",
            'transaction_date': transaction_date.strftime('%Y-%m-%d'),
            'account_id': f"ACC_{random.randint(1000, 9999)}",
            'account_type': account_type,
            'transaction_type': transaction_type,
            'amount': amount,
            'balance': round(random.uniform(1000, 50000), 2),
            'category': random.choice(['Salary', 'Investment', 'Shopping', 'Bills', 'Entertainment']),
            'description': f"{transaction_type} transaction"
        })
    
    df = pd.DataFrame(data)
    df.to_csv('data/sample_finance.csv', index=False)
    print(f"✅ Generated {len(df)} finance records")
    return df

def generate_saas_data():
    """Generate SaaS/software usage data."""
    print("💻 Generating SaaS data...")
    
    # Generate dates for the last 6 months
    end_date = datetime.now()
    start_date = end_date - timedelta(days=180)
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # Generate 800 user activities
    n_activities = 800
    data = []
    
    for i in range(n_activities):
        activity_date = random.choice(dates)
        user_id = f"USER_{random.randint(1000, 9999)}"
        plan_type = random.choice(['Free', 'Basic', 'Pro', 'Enterprise'])
        feature_used = random.choice(['Dashboard', 'Analytics', 'Reports', 'API', 'Export'])
        
        data.append({
            'activity_id': f"ACT_{i+1:04d}",
            'activity_date': activity_date.strftime('%Y-%m-%d'),
            'user_id': user_id,
            'user_email': f"user{user_id}@example.com",
            'plan_type': plan_type,
            'feature_used': feature_used,
            'session_duration': random.randint(5, 120),
            'pages_viewed': random.randint(1, 20),
            'country': random.choice(['USA', 'Canada', 'UK', 'Germany', 'India', 'Australia']),
            'device_type': random.choice(['Desktop', 'Mobile', 'Tablet']),
            'subscription_status': random.choice(['Active', 'Trial', 'Cancelled', 'Expired'])
        })
    
    df = pd.DataFrame(data)
    df.to_csv('data/sample_saas.csv', index=False)
    print(f"✅ Generated {len(df)} SaaS records")
    return df

def generate_manufacturing_data():
    """Generate manufacturing/production data."""
    print("🏭 Generating manufacturing data...")
    
    # Generate dates for the last 3 months
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # Generate 600 production records
    n_records = 600
    data = []
    
    for i in range(n_records):
        production_date = random.choice(dates)
        product_line = random.choice(['Line A', 'Line B', 'Line C'])
        product_type = random.choice(['Widget A', 'Widget B', 'Widget C'])
        planned_quantity = random.randint(100, 1000)
        actual_quantity = int(planned_quantity * random.uniform(0.85, 1.15))
        defect_count = random.randint(0, int(actual_quantity * 0.05))
        
        data.append({
            'production_id': f"PROD_{i+1:04d}",
            'production_date': production_date.strftime('%Y-%m-%d'),
            'product_line': product_line,
            'product_type': product_type,
            'planned_quantity': planned_quantity,
            'actual_quantity': actual_quantity,
            'defect_count': defect_count,
            'quality_score': round(random.uniform(85, 99), 2),
            'operator_id': f"OP_{random.randint(1, 20)}",
            'shift': random.choice(['Morning', 'Afternoon', 'Night']),
            'machine_id': f"MACH_{random.randint(1, 10)}",
            'efficiency_rate': round(random.uniform(70, 95), 2)
        })
    
    df = pd.DataFrame(data)
    df.to_csv('data/sample_manufacturing.csv', index=False)
    print(f"✅ Generated {len(df)} manufacturing records")
    return df

def generate_generic_data():
    """Generate generic analytics data."""
    print("📊 Generating generic analytics data...")
    
    # Generate dates for the last year
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # Generate 400 records
    n_records = 400
    data = []
    
    for i in range(n_records):
        record_date = random.choice(dates)
        category = random.choice(['Category A', 'Category B', 'Category C', 'Category D'])
        region = random.choice(['North', 'South', 'East', 'West'])
        
        data.append({
            'record_id': f"REC_{i+1:04d}",
            'record_date': record_date.strftime('%Y-%m-%d'),
            'category': category,
            'region': region,
            'value_1': random.randint(10, 1000),
            'value_2': round(random.uniform(1.5, 50.5), 2),
            'value_3': random.randint(100, 10000),
            'status': random.choice(['Active', 'Inactive', 'Pending']),
            'priority': random.choice(['Low', 'Medium', 'High']),
            'department': random.choice(['Sales', 'Marketing', 'Operations', 'Finance'])
        })
    
    df = pd.DataFrame(data)
    df.to_csv('data/sample_generic.csv', index=False)
    print(f"✅ Generated {len(df)} generic records")
    return df

def main():
    """Generate all sample datasets."""
    print("🚀 Generating sample CSV files for autocurate dashboard testing...")
    
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    # Generate all datasets
    datasets = {
        'ecommerce': generate_ecommerce_data(),
        'finance': generate_finance_data(),
        'saas': generate_saas_data(),
        'manufacturing': generate_manufacturing_data(),
        'generic': generate_generic_data()
    }
    
    print("\n📋 Generated Files:")
    for name, df in datasets.items():
        filename = f"data/sample_{name}.csv"
        print(f"  📄 {filename} ({len(df)} rows, {len(df.columns)} columns)")
    
    print("\n🎯 You can now upload these files to test your autocurate dashboard!")
    print("   Each file represents a different business domain that the AI can detect.")

if __name__ == "__main__":
    main() 