import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import json
import os

# Initialize Faker
fake = Faker('en_IN')  

if not os.path.exists('data2'):
    os.makedirs('data2')

customer_df = pd.read_csv('data1/customer.csv')


def update_customer_data(customer_df):
    
    customer_df = customer_df.sample(100)
    
    food_preferences = ['Veg', 'Non-Veg', 'Vegan', 'Eggetarian']
    cuisine_types = ['North Indian', 'South Indian', 'Chinese', 'Italian', 'Continental', 
                     'Mediterranean', 'Mexican', 'Thai', 'Japanese', 'Street Food']
    
    for index, row in customer_df.iterrows():
        customer_id = row['CustomerID']  # Find the CustomerID
        
        # Randomly decide which columns to update
        columns_to_update = random.sample(
            ['Full_Name', 'Email', 'Mobile_no', 'Rating', 'Preferences'], 
            k=random.randint(1, 2)  # Update 1 or 2 columns
        )
        
        # Update columns based on CustomerID
        if 'Full_Name' in columns_to_update:
            if row['Gender'].lower() == 'male':
                customer_df.loc[customer_df['CustomerID'] == customer_id, 'Full_Name'] = fake.name_male()
            elif row['Gender'].lower() == 'female':
                customer_df.loc[customer_df['CustomerID'] == customer_id, 'Full_Name'] = fake.name_female()
            else:
                customer_df.loc[customer_df['CustomerID'] == customer_id, 'Full_Name'] = fake.name()  # Default if gender is unknown
        
        if 'Email' in columns_to_update:
            name = customer_df.loc[customer_df['CustomerID'] == customer_id, 'Full_Name'].values[0]
            email_domain = random.choice(['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com'])
            email = f"{name.lower().replace(' ', '')}{random.randint(1, 999)}@{email_domain}"
            customer_df.loc[customer_df['CustomerID'] == customer_id, 'Email'] = email
        
        if 'Mobile_no' in columns_to_update:
            customer_df.loc[customer_df['CustomerID'] == customer_id, 'Mobile_no'] = random.randint(7000000000, 9999999999)
        
        if 'Rating' in columns_to_update:
            customer_df.loc[customer_df['CustomerID'] == customer_id, 'Rating'] = round(random.uniform(3.0, 5.0), 1)
        
        if 'Preferences' in columns_to_update:
            food_pref = random.choice(food_preferences)
            num_cuisines = random.randint(1, 5)
            cuisine_pref = random.sample(cuisine_types, num_cuisines)
            preferences = {
            'FoodPreference': food_pref,
            'CuisineTypes': cuisine_pref
            }
            customer_df.loc[customer_df['CustomerID'] == customer_id, 'Preferences'] = json.dumps(preferences)
        
    return customer_df


customer_df_new = update_customer_data(customer_df)
customer_df_new.to_csv('data2/customer.csv', index=False)

