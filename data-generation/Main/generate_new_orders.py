import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta, time
import json
import os

## First of all every time run th is file make sure to set start id of the all datasets to next id of the last generated data
## when generate last 6 months data then do some changes in order_df and also read customer_df from temp folder
## and if generating all time data then read customer_df from data1 folder

start_time = datetime.now()
print(f"Program started at: {start_time}")


# Initialize Faker
fake = Faker('en_IN')  # Using Indian locale


if not os.path.exists('data1'):
    os.makedirs('data1')
    
if not os.path.exists('data2'):
    os.makedirs('data2')

if not os.path.exists('data3'):
    os.makedirs('data3')

if not os.path.exists('data4'):
    os.makedirs('data4')

## ORDERS
NUM_ORDERS = 100  # Number of orders
ORDER_START_ID = 418750
ORDER_END_ID = ORDER_START_ID + NUM_ORDERS

## ORDERS_ITEMS
NUM_ORDERS_ITEMS = 100  # Number of orders
ORDER_ITEMS_START_ID = 749241
ORDER_ITEMS_END_ID = ORDER_ITEMS_START_ID + NUM_ORDERS_ITEMS

DELIVERY_START_ID = 370243


## CURRENT DATE FOR FILE SAVE 
CURRENT_DATE = datetime.now()



def generate_orders_data(order_start_id, order_end_id, customer_df, restaurant_df, address_df, location_df):
    order_statuses = ['Delivered', 'Canceled', 'Failed', 'Returned']
    payment_methods = ['Cash', 'UPI', 'CreditCard', 'DebitCard']
    
    data = []
    order_id = order_start_id
    
    # Add location information to address_df for easier joining
    address_with_location = pd.DataFrame()
    
    # For each customer, generate orders from restaurants in the same city as their address
    for customer_id in customer_df['CustomerID']:
        # Get all addresses for this customer
        customer_addresses = address_df[address_df['CustomerID'] == customer_id]
        
        if len(customer_addresses) == 0:
            continue
            
        # Generate 1-5 orders for every customer first time but here it is second time
        num_orders = random.randint(1, 3)
        
        # Track if this is the first order for this customer
        is_first_order = True
        
        for _ in range(num_orders):
            
            # Randomly select one of the customer's addresses
            address = customer_addresses.sample(1).iloc[0]
            # print(pd.to_datetime(address['CreatedDate']))
            address_id = address['AddressID']
            city = address['City']
        
        
            ## generate only last 6 months orders
            # six_months_ago = datetime.now() - timedelta(days=180)
            # start_date = max(six_months_ago, pd.to_datetime(address['CreatedDate']))
            # order_date = fake.date_time_between(start_date=start_date, end_date='now')
            
            ## Generate a random order date after the address creation date

            end_date = datetime(2021, 12, 31, 23, 59, 59)
            order_date = fake.date_time_between(start_date=pd.to_datetime(address['CreatedDate']), end_date=end_date)
            # order_date = fake.date_time_between(start_date=pd.to_datetime(address['CreatedDate']), end_date='now')

            # Created and modified dates
            created_date = order_date
            modified_date = fake.date_time_between(start_date=created_date, end_date='now')
            
            # Find restaurants in the same city
            city_location_ids = location_df[location_df['City'] == city]['LocationID'].tolist()
            city_restaurants = restaurant_df[restaurant_df['LocationID'].isin(city_location_ids)]

            # Skip if no restaurants found in this city
            if len(city_restaurants) == 0:
                continue
                
            # Select a random restaurant from the same city
            filtered_restaurants = city_restaurants[pd.to_datetime(city_restaurants['CreatedDate']) < order_date]
            
            if len(filtered_restaurants) == 0:
                continue
            
            restaurant = filtered_restaurants.sample(1).iloc[0]
            
            restaurant_id = restaurant['RestaurantID']
            
            
             # Parse operating hours from restaurant
            start_hr, end_hr = [
                int(time_part.split(":")[0]) for time_part in restaurant['OperatingHours'].replace(" ", "").split("-")
            ]

            # Construct available hours across midnight
            if end_hr > start_hr:
                available_hours = list(range(start_hr, end_hr))
            else:
                available_hours = list(range(start_hr, 24)) + list(range(0, end_hr))

            # Just pick a random hour — shift probability already handled upstream
            order_hour = random.choice(available_hours)

            # Final order datetime
            # order_date_date = pd.to_datetime(address['CreatedDate']) + timedelta(days=random.randint(1, 90))
            order_date = datetime.combine(order_date.date(), time(hour=order_hour, minute=random.randint(0, 59)))
            
            # Order amount (initially set to 0, will be updated later)
            total_amount = 0
            final_amount = 0
            
            # Status
            status = random.choices(
                    order_statuses, 
                    weights=[0.8, 0.1, 0.03, 0.07]
            )[0]
            
            # Payment method
            payment_method = np.random.choice(payment_methods,p=[0.2,0.7,0.05,0.05])
            
            # Created and modified dates
            created_date = order_date
            modified_date = fake.date_time_between(start_date=created_date, end_date='now')
            # Get restaurant coupons
            coupon_applied = False
            coupon_code = None
            discount_amount = 0
            delivery_charges = 0
            restaurant_coupons = None
            try:
                restaurant_coupons = json.loads(restaurant['Coupons'])
            except:
                restaurant_coupons = None
            
            # Randomly decide whether to keep the coupons or set them to None
            if np.random.rand() > 0.3:
                restaurant_coupons = restaurant_coupons
            else:
                restaurant_coupons = None
                   
            data.append({
                'OrderID': order_id,
                'CustomerID': customer_id,
                'RestaurantID': restaurant_id,
                'OrderDate': order_date,
                'TotalAmount': total_amount,  # Will be updated in order_items function
                'DiscountAmount': discount_amount,
                'DeliveryCharges' : delivery_charges,
                'FinalAmount' : final_amount,
                'Status': status,
                'PaymentMethod': payment_method,
                'IsFirstOrder': is_first_order,
                'RestaurantCoupons': restaurant['Coupons'] if restaurant_coupons != None else None,  # Store all restaurant coupons
                'CouponApplied': coupon_applied,
                'CouponCode': coupon_code,
                'CreatedDate': created_date,
                'ModifiedDate': modified_date,
                'AddressID': address_id
            })
            
            order_id += 1
            is_first_order = False  # Mark subsequent orders as not first orders
        #     if order_id >= 10:
        #         break
                
        # if order_id >= 10:
        #     break
    
    result_df = pd.DataFrame(data)
    
    return result_df


def generate_order_items_data(order_items_start_id,order_df, menu_df):
    data = []
    order_item_id = order_items_start_id
    
    # Get the mapping of restaurant to their menu items
    restaurant_menu_map = {}
    for _, menu_item in menu_df.iterrows():
        restaurant_id = menu_item['RestaurantID']
        menu_id = menu_item['MenuItemID']
        
        if restaurant_id not in restaurant_menu_map:
            restaurant_menu_map[restaurant_id] = []
            
        restaurant_menu_map[restaurant_id].append(menu_id)
    
    # For each order, generate 1-5 order items
    for idx, order in order_df.iterrows():
        order_id = order['OrderID']
        order_date = order['CreatedDate']
        restaurant_id = order['RestaurantID']
        payment_method = order['PaymentMethod']
        # print('Payment method' , payment_method)
        is_first_order = order['IsFirstOrder']
        
        
        # Get available menu items for this restaurant
        menu_ids = restaurant_menu_map.get(restaurant_id, [])
        
        if not menu_ids:
            continue
            
        filtered_menu_id = []
        for menu_id in menu_ids:
            if pd.to_datetime(menu_df[menu_df['MenuItemID'] == menu_id]['CreatedDate'].values[0]) < pd.to_datetime(order_date):
                filtered_menu_id.append(menu_id)
        
        if not filtered_menu_id:
            continue
        
        # Generate 1-5 random order items
        num_items = random.choices(
            population=[1, 2, 3, 4, 5],
            weights=[0.5, 0.3, 0.1, 0.06, 0.04],
            k=1
        )[0]
        # Select unique menu items
        selected_menu_ids = random.sample(filtered_menu_id, min(num_items, len(filtered_menu_id)))
        
        order_total = 0
        
        for menu_id in selected_menu_ids:
            # Get the menu item details
            menu_item = menu_df[menu_df['MenuItemID'] == menu_id].iloc[0]
            price = menu_item['Price']
            
            # Generate random quantity
            quantity = random.choices(
                population=[1, 2, 3],
                weights=[0.65, 0.25, 0.1],
                k=1
            )[0]
            
            # Calculate subtotal
            subtotal = price * quantity
            order_total += subtotal
            
            # Generate rating (if delivered)
            if order['Status'] == 'Delivered':
                ratings = np.random.choice([1, 2, 3, 4, 5], p=[0.1, 0.1, 0.1, 0.3, 0.4])
            else:
                ratings = None
            
            # Dates
            created_date = order['CreatedDate']
            modified_date = order['ModifiedDate']
            
            data.append({
                'OrderItemID': order_item_id,
                'OrderID': order_id,
                'MenuItemID': menu_id,
                'Quantity': quantity,
                'Price': price,
                'Subtotal': subtotal,
                'Ratings' : ratings,
                'CreatedDate': created_date,
                'ModifiedDate': modified_date
            })
            
            order_item_id += 1

    
        # Now that we know the total order amount, we can apply coupons
        final_amount = order_total  # Default to original amount
        coupon_applied = False
        coupon_code = None
        discount_amount = 0
        delivery_charges = 0

        if order_total < 199:
            delivery_charges = random.randint(80,100)
        elif order_total >= 200 and order_total < 399 :
            delivery_charges = random.randint(50,80)
        elif order_total >= 400 and order_total < 599 :
            delivery_charges = random.randint(30, 50)
        else :
            delivery_charges = 0

        # Check if there are available coupons
        if 'RestaurantCoupons' in order_df.columns and order['RestaurantCoupons'] is not None:
            try:
                restaurant_coupons = json.loads(order['RestaurantCoupons'])
                eligible_coupons = []
                
                # Find eligible coupons based on:
                # 1. Order amount meets minimum
                # 2. Payment method matches (if payment-specific)
                # 3. First order status (if first-order coupon)
                for coupon in restaurant_coupons:
                    # Skip if the order amount doesn't meet the minimum
                    if order_total < coupon['min_amount']:
                        continue
                    
                    # Skip if it's a payment-specific coupon and payment method doesn't match
                    if coupon['payment_method'] is not None and coupon['payment_method'] != payment_method:
                        continue
                    
                    # Skip if it's a first-order coupon but this isn't the first order
                    if 'FIRSTORDER' in coupon['code'] and not is_first_order:
                        continue
                    
                    # Add to eligible coupons
                    eligible_coupons.append(coupon)
                
                # Apply the best coupon (most discount)
                if eligible_coupons:
                    # Sort by discount amount, highest first
                    eligible_coupons.sort(key=lambda x: x['discount_amount'], reverse=True)
                    best_coupon = eligible_coupons[0]
                    
                    coupon_code = best_coupon['code']
                    discount_amount = best_coupon['discount_amount']
                    final_amount = max(0, order_total - discount_amount)
                    coupon_applied = True
                    
                    # Update the order with the coupon information
                    order_df.loc[order_df['OrderID'] == order_id, 'TotalAmount'] = order_total
                    order_df.loc[order_df['OrderID'] == order_id, 'CouponApplied'] = coupon_applied
                    order_df.loc[order_df['OrderID'] == order_id, 'CouponCode'] = coupon_code
                    order_df.loc[order_df['OrderID'] == order_id, 'DiscountAmount'] = discount_amount
                    
                    order_df.loc[order_df['OrderID'] == order_id,'DeliveryCharges'] = delivery_charges
                    order_df.loc[order_df['OrderID'] == order_id, 'FinalAmount'] = final_amount + delivery_charges

                else:
                    # No eligible coupons, update order with the original amount
                    # order_df.at[idx, 'TotalAmount'] = order_total
                    order_df.loc[order_df['OrderID'] == order_id, 'TotalAmount'] = order_total
                    order_df.loc[order_df['OrderID'] == order_id,'DeliveryCharges'] = delivery_charges
                    order_df.loc[order_df['OrderID'] == order_id, 'FinalAmount'] = final_amount + delivery_charges
            except Exception as e:
                # If there's an error applying coupons, just use the original amount
                order_df.loc[order_df['OrderID'] == order_id, 'TotalAmount'] = order_total
                order_df.loc[order_df['OrderID'] == order_id,'DeliveryCharges'] = delivery_charges
                order_df.loc[order_df['OrderID'] == order_id, 'FinalAmount'] = final_amount + delivery_charges
        else:
            # No coupons available, update with the original amount
           order_df.loc[order_df['OrderID'] == order_id, 'TotalAmount'] = order_total
           order_df.loc[order_df['OrderID'] == order_id,'DeliveryCharges'] = delivery_charges
           order_df.loc[order_df['OrderID'] == order_id, 'FinalAmount'] = final_amount + delivery_charges
    
    return pd.DataFrame(data)


def generate_delivery_data(order_df, delivery_agent_df, delivery_start_id,restaurant_df):
    # delivery_statuses = ['Delivered', 'In Transit', 'Assigned', 'Failed']
    
    data = []
    delivery_id = delivery_start_id
    
    # For each order, generate a delivery record
    for _, order in order_df.iterrows():
        order_id = order['OrderID']
        address_id = order['AddressID']
        order_date = order['CreatedDate']
        
        total_amount = order['TotalAmount']
        if total_amount == 0:
            continue
        # Only generate delivery for non-canceled orders
        if order['Status'] not in ['Canceled']:
            # Find delivery agents in the same city as the restaurant
            restaurant_id = order['RestaurantID']
            restaurant_location_id = restaurant_df[restaurant_df['RestaurantID'] == restaurant_id]['LocationID'].values[0]            
            available_agents = delivery_agent_df[delivery_agent_df['LocationID'] == restaurant_location_id]
            # Get delivery agents (we'll assume agents can deliver to any location)
            if len(available_agents) == 0:
                continue
            
            filtered_available_agents = available_agents[pd.to_datetime(available_agents['CreatedDate']) < order_date]
            
            if len(filtered_available_agents) == 0:
                continue
            
            # Select a random delivery agent
            agent = filtered_available_agents.sample(1).iloc[0]
            agent_id = agent['DeliveryAgentID']
            
            # Set status based on order status
            if order['Status'] == 'Failed':
                delivery_status = 'Failed'
            elif order['Status'] == 'Returned':
                delivery_status = 'Returned'
            else:
                delivery_status = 'Delivered'
            
            # Estimated time (10-60 minutes)
            estimated_time = random.randint(15, 55)

            if delivery_status in ['Delivered', 'Returned']:
                # Increase the chance of fast delivery
                delay_type = np.random.choice(['fast', 'normal', 'slow'], p=[0.4, 0.4, 0.2])

                if delay_type == 'fast':
                    delivered_time = random.randint(max(10, estimated_time - 10), estimated_time)
                elif delay_type == 'normal':
                    delivered_time = random.randint(estimated_time + 1, estimated_time + 10)
                else:  # slow
                    delivered_time = random.randint(estimated_time + 11, estimated_time + 30)
            else:
                delivered_time = None
            
            # Delivery date based on order date
            if delivery_status in ['Delivered','Returned']:
                # Add 30-90 minutes to order date
                minutes_to_add = random.randint(30, 90)
                delivery_date = pd.to_datetime(order['OrderDate']) + timedelta(minutes=int(delivered_time))
            else:
                delivery_date = None
            
            if delivered_time != None:
                delivered_time = int(delivered_time)
            
            # Dates
            created_date = order['CreatedDate']
            modified_date = order['ModifiedDate']
            
            data.append({
                'DeliveryID': delivery_id,
                'OrderID': order_id,
                'DeliveryAgentID': agent_id,
                'DeliveryStatus': delivery_status,
                'EstimatedTime': estimated_time,
                'DeliveredTime' : delivered_time,
                'AddressID': address_id,
                'DeliveryDate': delivery_date,
                'CreatedDate': created_date,
                'ModifiedDate': modified_date
            })
            
            delivery_id += 1

            # if(delivery_id >= 100):
            #     break
    
    return pd.DataFrame(data)


def update_menu_item_ratings(order_items_df,menu_df):
    for _,items in menu_df.iterrows():
        menu_id = items['MenuItemID']
        avg_of_menu_item_ratings = order_items_df[order_items_df['MenuItemID'] == menu_id]['Ratings'].agg('mean')
        menu_df.loc[menu_df['MenuItemID'] == menu_id,'Ratings'] = round(avg_of_menu_item_ratings,1)


def update_restaurant_ratings(menu_df,restaurant_df):
    for _,restaurant in restaurant_df.iterrows():
        restaurant_id = restaurant['RestaurantID']
        avg_of_restaurant_ratings = menu_df[menu_df['RestaurantID'] == restaurant_id]['Ratings'].agg('mean')
        restaurant_df.loc[restaurant_df['RestaurantID'] == restaurant_id, 'Ratings'] = round(avg_of_restaurant_ratings,1)



## Reading all files already generated
location_df = pd.read_csv('data1/location.csv')

restaurant_df = pd.read_csv('data1/restaurant.csv')
# restaurant_df = restaurant_df.sample(1000) # to update some restaunrants
restaurant_df['CreatedDate'] = pd.to_datetime(restaurant_df['CreatedDate'])
restaurant_df = restaurant_df[restaurant_df['CreatedDate'].dt.year.isin([2020, 2021])]
print('filtered customer data', restaurant_df)

menu_df = pd.read_csv('data1/menu_items.csv') ## menu_items.csv
menu_df = menu_df[menu_df['RestaurantID'].isin(restaurant_df['RestaurantID'])] # to update some menu items
menu_df['CreatedDate'] = pd.to_datetime(menu_df['CreatedDate'])
menu_df = menu_df[menu_df['CreatedDate'].dt.year.isin([2020, 2021])]
print('filtered customer data', menu_df)

# for all time customers
# customer_df = pd.read_csv('data1/customer.csv')
# customer_df = customer_df.sample(15000)

# for last 6 to 12 months
# customer_df = pd.read_csv('temp/customer.csv')

# for 2020 and 2021 
customer_df = pd.read_csv('temp/customer.csv')
customer_df['CreatedDate'] = pd.to_datetime(customer_df['CreatedDate'])
customer_df = customer_df[customer_df['CreatedDate'].dt.year.isin([2020, 2021])]
print('filtered customer data', customer_df)

address_df = pd.read_csv('data1/customer_address.csv')
address_df['CreatedDate'] = pd.to_datetime(address_df['CreatedDate'])
address_df = address_df[address_df['CreatedDate'].dt.year.isin([2020, 2021])]
print('filtered customer data', address_df)

login_audit_df = pd.read_csv('data1/login_audit.csv')

# delivery_agent_df = pd.read_csv('data1/delivery_agent.csv')
delivery_agent_df = pd.read_json('data1/delivery_agent.json')
delivery_agent_df['CreatedDate'] = pd.to_datetime(delivery_agent_df['CreatedDate'])
delivery_agent_df = delivery_agent_df[delivery_agent_df['CreatedDate'].dt.year.isin([2020, 2021])]
print('filtered customer data', delivery_agent_df)


order_df = generate_orders_data(ORDER_START_ID, ORDER_END_ID, customer_df, restaurant_df, address_df, location_df)
print('order data generated', datetime.now() , ' < total time > ', datetime.now() - start_time)


order_items_df = generate_order_items_data(ORDER_ITEMS_START_ID,order_df, menu_df)
print('order item data generated', datetime.now() , ' < total time > ', datetime.now() - start_time)

order_df = order_df[(order_df['TotalAmount'] != 0)]
print('order totalprice and all updated', datetime.now() , ' < total time > ', datetime.now() - start_time)


delivery_df = generate_delivery_data(order_df, delivery_agent_df, DELIVERY_START_ID,restaurant_df)
print('delivered', datetime.now() , ' < total time > ', datetime.now() - start_time)



    # Remove temporary columns
if 'RestaurantCoupons' in order_df.columns:
    order_df = order_df.drop(columns=['RestaurantCoupons'])
    
if 'AddressID' in order_df.columns:
    order_df = order_df.drop(columns=['AddressID'])

if 'CouponApplied' in order_df.columns:
    order_df = order_df.drop(columns=['CouponApplied'])

if 'LocationID' in address_df.columns:
    address_df =  address_df.drop(columns=['LocationID'])


# for adjusting ratings
order_items_df_old = pd.read_csv('data1/order_items.csv')
order_items_df_merged = pd.concat([order_items_df, order_items_df_old], ignore_index=True)


update_menu_item_ratings(order_items_df_merged,menu_df)
print('menu rattings updated', datetime.now() , ' < total time > ', datetime.now() - start_time)
update_restaurant_ratings(menu_df,restaurant_df)    
print('restaurant rattings updated', datetime.now() , ' < total time > ', datetime.now() - start_time)


# when generate last 6 months data then do some changes in order_df and also read customer_df from temp folder
# and if generating all time data then read customer_df from data1 folder

restaurant_df.to_csv('data4/restaurant.csv', index=False)
menu_df.to_csv('data4/menu_items.csv', index=False)
order_df.to_csv('data4/orders.csv', index=False)
order_items_df.to_csv('data4/order_items.csv', index=False)
delivery_df.to_csv('data4/delivery.csv', index=False)


end_time = datetime.now()
print(f"Program ended at: {end_time}")

# Total duration
duration = end_time - start_time
print(f"Total execution time: {duration}")