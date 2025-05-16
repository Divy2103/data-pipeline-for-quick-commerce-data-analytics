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
    
if not os.path.exists('data3'):
    os.makedirs('data3')

    
customer_df = pd.read_csv('data1/customer.csv')
delivery_agent_df = pd.read_json('data1/delivery_agent.json')
address_df = pd.read_csv('data1/customer_address.csv')
menu_df = pd.read_csv('data1/menu_items.csv')
restaurant_df = pd.read_csv('data1/restaurant.csv')


def update_customer_data(customer_df):
    
    customer_df = customer_df.sample(333)
    
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


def update_delivery_agent_data(delivery_agent_df):
    vehicle_types = ['Bike', 'Scooter']
    
    # Randomly sample 100 delivery agents for updates
    delivery_agent_df = delivery_agent_df.sample(263)
    
    for index, row in delivery_agent_df.iterrows():
        delivery_agent_id = row['DeliveryAgentID']  # Find the DeliveryAgentID
        
        # Randomly decide which columns to update
        columns_to_update = random.sample(
            ['email', 'VehicleType', 'Status', 'Mobile_no', 'Rating'], 
            k=random.randint(1, 2)  # Update 1 or 2 columns
        )
        
        # Update columns based on DeliveryAgentID
        if 'email' in columns_to_update:
            name = delivery_agent_df.loc[delivery_agent_df['DeliveryAgentID'] == delivery_agent_id, 'Full_Name'].values[0]
            email_domain = random.choice(['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com'])
            email = f"{name.lower().replace(' ', '')}{random.randint(1, 999)}@{email_domain}"
            delivery_agent_df.loc[delivery_agent_df['DeliveryAgentID'] == delivery_agent_id, 'email'] = email
        
        if 'VehicleType' in columns_to_update:
            delivery_agent_df.loc[delivery_agent_df['DeliveryAgentID'] == delivery_agent_id, 'VehicleType'] = random.choice(vehicle_types)
        
        if 'Status' in columns_to_update:
            delivery_agent_df.loc[delivery_agent_df['DeliveryAgentID'] == delivery_agent_id, 'Status'] = np.random.choice([True, False], p=[0.9, 0.1])
        
        if 'Mobile_no' in columns_to_update:
            delivery_agent_df.loc[delivery_agent_df['DeliveryAgentID'] == delivery_agent_id, 'Mobile_no'] = random.randint(6000000000, 9999999999)
        
        if 'Rating' in columns_to_update:
            delivery_agent_df.loc[delivery_agent_df['DeliveryAgentID'] == delivery_agent_id, 'Rating'] = round(random.uniform(3.0, 5.0), 1)
    
    return delivery_agent_df


def update_customer_address_data(address_df):
    address_types = ['Home', 'Work', 'Other']
    localities = [
        'Saket', 'Connaught Place', 'Dwarka', 'Vasant Kunj', 'South Extension',
        'Rohini', 'Karol Bagh', 'Pitampura', 'Janakpuri', 'Lajpat Nagar',
        'Malviya Nagar', 'Greater Kailash', 'Hauz Khas', 'Mayur Vihar', 'Rajouri Garden'
    ]
    
    # Randomly sample 100 addresses for updates
    address_df = address_df.sample(666)
    
    for index, row in address_df.iterrows():
        address_id = row['AddressID']  # Find the AddressID
        
        # Randomly decide which columns to update
        columns_to_update = random.sample(
            ['FlatNo/HouseNo', 'Floor', 'Building', 'Landmark', 'Locality', 'AddressType'], 
            k=random.randint(1, 2)  # Update 1 or 2 columns
        )
        
        # Update columns based on AddressID
        if 'FlatNo/HouseNo' in columns_to_update:
            address_df.loc[address_df['AddressID'] == address_id, 'FlatNo/HouseNo'] = random.randint(1, 50)
        
        if 'Floor' in columns_to_update:
            address_df.loc[address_df['AddressID'] == address_id, 'Floor'] = random.randint(1, 40) if random.random() > 0.3 else None
        
        if 'Building' in columns_to_update:
            address_df.loc[address_df['AddressID'] == address_id, 'Building'] = fake.company() + " " + random.choice(['Apartments', 'Residency', 'Heights', 'Towers', 'Complex'])
        
        if 'Landmark' in columns_to_update:
            address_df.loc[address_df['AddressID'] == address_id, 'Landmark'] = random.choice(["Near ", "Opp. ", "B/h. ", "Beside ", "Behind ", ""]) + fake.company()
        
        if 'Locality' in columns_to_update:
            address_df.loc[address_df['AddressID'] == address_id, 'Locality'] = random.choice(localities)
        
        if 'AddressType' in columns_to_update:
            address_df.loc[address_df['AddressID'] == address_id, 'AddressType'] = random.choice(address_types)
    
    return address_df


def update_menu_data(menu_df):
    # Randomly sample 100 menu items for updates
    menu_df = menu_df.sample(312)
    
    for index, row in menu_df.iterrows():
        menu_id = row['MenuItemID']  # Find the MenuItemID
        current_price = row['Price']  # Get the current price
        
        # Decide whether to increase or decrease the price
        if random.random() <= 0.9:  # 90% chance to increase the price
            percentage_change = random.uniform(0.05, 0.15)  # Increase by 5% to 15%
            new_price = current_price * (1 + percentage_change)
        else:  # 10% chance to decrease the price
            percentage_change = random.uniform(0.05, 0.15)  # Decrease by 5% to 15%
            new_price = current_price * (1 - percentage_change)
        
        # Update the price in the DataFrame
        menu_df.loc[menu_df['MenuItemID'] == menu_id, 'Price'] = round(new_price, 0)
    
    return menu_df


def update_restaurant_data(restaurant_df):
    cuisine_types = [
        "North Indian", "South Indian", "Chinese", "Italian", "Continental", 
        "Mediterranean", "Mexican", "Thai", "Japanese", "Lebanese", 
        "Mughlai", "Street Food", "Desserts", "Beverages", "Fast Food",
        "Cafe", "Bakery", "Ice Cream", "Pizza", "Burger"
    ]
    
    # Randomly sample 100 restaurants for updates
    restaurant_df = restaurant_df.sample(49)
    
    for index, row in restaurant_df.iterrows():
        restaurant_id = row['RestaurantID']  # Find the RestaurantID
        
        # Randomly decide which columns to update
        columns_to_update = random.sample(
            ['CuisineType', 'Pricing_for_2', 'Restaurant_Phone'], 
            k=random.randint(1, 2)  # Update 1 or 2 columns
        )
        
        # Update columns based on RestaurantID
        if 'CuisineType' in columns_to_update:
            num_cuisines = random.randint(1, 3)
            updated_cuisines = random.sample(cuisine_types, num_cuisines)
            restaurant_df.loc[restaurant_df['RestaurantID'] == restaurant_id, 'CuisineType'] = ", ".join(updated_cuisines)
        
        if 'Pricing_for_2' in columns_to_update:
            updated_pricing = random.randint(200, 2000)
            restaurant_df.loc[restaurant_df['RestaurantID'] == restaurant_id, 'Pricing_for_2'] = updated_pricing
        
        if 'Restaurant_Phone' in columns_to_update:
            updated_phone = random.randint(9100000000, 9999999999)
            restaurant_df.loc[restaurant_df['RestaurantID'] == restaurant_id, 'Restaurant_Phone'] = updated_phone
    
    return restaurant_df



customer_df_new = update_customer_data(customer_df)
customer_df_new.to_csv('data2/customer.csv', index=False)


delivery_agent_df = update_delivery_agent_data(delivery_agent_df)
delivery_agent_df.to_json('data2/delivery_agent.json',orient='records', lines=False, indent=4)


address_df_new = update_customer_address_data(address_df)
address_df_new.to_csv('data2/customer_address.csv', index=False)


# menu_df_new = update_menu_data(menu_df)
# menu_df_new.to_csv('data2/menu_items.csv', index=False)


# restaurant_df_new = update_restaurant_data(restaurant_df)
# restaurant_df_new.to_csv('data2/restaurant.csv', index=False)