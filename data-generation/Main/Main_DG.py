import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta, time
import json
import os


start_time = datetime.now()
print(f"Program started at: {start_time}")

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

# Initialize Faker
fake = Faker('en_IN')  # Using Indian locale


if not os.path.exists('data1'):
    os.makedirs('data1')
    
if not os.path.exists('data2'):
    os.makedirs('data2')

if not os.path.exists('data3'):
    os.makedirs('data3')

## LOCATIONS
NUM_LOCATIONS = 50  # Number of locations
LOCATION_START_ID = 1
LOCATION_END_ID = LOCATION_START_ID + NUM_LOCATIONS

## RESTAURANTS
NUM_RESTAURANTS = 10  # Number of restaurants (increased to have restaurants in multiple cities)
RESTAURANT_START_ID = 1
RESTAURANT_END_ID = RESTAURANT_START_ID + NUM_RESTAURANTS

## MENU
NUM_MENU = 500  # Number of menu items
MENU_START_ID = 1
MENU_END_ID = MENU_START_ID + NUM_MENU

## CUSTOMERS
NUM_CUSTOMERS = 20000  # Number of customers
CUSTOMER_START_ID = 1
CUSTOMER_END_ID = CUSTOMER_START_ID + NUM_CUSTOMERS

## CUSTOMER ADDRESS - Multiple addresses per customer
NUM_CUSTOMER_ADDRESS = NUM_CUSTOMERS * 2
CUSTOMER_ADDRESS_START_ID = 1
CUSTOMER_ADDRESS_END_ID = CUSTOMER_ADDRESS_START_ID + NUM_CUSTOMER_ADDRESS

## CUSTOMER LOGIN AUDIT
NUM_CUSTOMER_LOGIN_AUDIT = NUM_CUSTOMERS * 3
CUSTOMER_LOGIN_AUDIT_START_ID = 1
CUSTOMER_LOGIN_AUDIT_END_ID = CUSTOMER_LOGIN_AUDIT_START_ID + NUM_CUSTOMER_LOGIN_AUDIT

## ORDERS
NUM_ORDERS = 100  # Number of orders
ORDER_START_ID = 1
ORDER_END_ID = ORDER_START_ID + NUM_ORDERS

## ORDERS
NUM_ORDERS_ITEMS = 100  # Number of orders
ORDER_ITEMS_START_ID = 1
ORDER_ITEMS_END_ID = ORDER_ITEMS_START_ID + NUM_ORDERS_ITEMS

DELIVERY_START_ID = 1

## DELIVERY AGENTS - Distributed across locations
NUM_DELIVERY_AGENT = 150
DELIVERY_AGENT_START_ID = 1
DELIVERY_AGENT_END_ID= DELIVERY_AGENT_START_ID + NUM_DELIVERY_AGENT

## CURRENT DATE FOR FILE SAVE 
CURRENT_DATE = datetime.now()


def generate_location_data(location_start_id, location_end_id):
    # Indian cities and states
    cities_states = [
        ('Delhi', 'Delhi'),
        ('Mumbai', 'Maharashtra'),
        ('Bangalore', 'Karnataka'),
        ('Hyderabad', 'Telangana'),
        ('Chennai', 'Tamil Nadu'),
        ('Kolkata', 'West Bengal'),
        ('Pune', 'Maharashtra'),
        ('Ahmedabad', 'Gujarat'),
        ('Jaipur', 'Rajasthan'),
        ('Lucknow', 'Uttar Pradesh'),
        ('Chandigarh', 'Punjab'),
        ('Bhopal', 'Madhya Pradesh'),
        ('Guwahati', 'Assam'),
        ('Kochi', 'Kerala'),
        ('Indore', 'Madhya Pradesh'),
        ('Surat', 'Gujarat'),
        ('Nagpur', 'Maharashtra'),
        ('Patna', 'Bihar'),
        ('Vadodara', 'Gujarat'),
        ('Thane', 'Maharashtra'),
        ('Agra', 'Uttar Pradesh'),
        ('Nashik', 'Maharashtra'),
        ('Faridabad', 'Haryana'),
        ('Meerut', 'Uttar Pradesh'),
        ('Rajkot', 'Gujarat'),
        ('Varanasi', 'Uttar Pradesh'),
        ('Srinagar', 'Jammu and Kashmir'),
        ('Aurangabad', 'Maharashtra'),
        ('Dhanbad', 'Jharkhand'),
        ('Amritsar', 'Punjab'),
        ('Morbi', 'Gujarat'),
        ('Anand', 'Gujarat'),
        ('Nadiad', 'Gujarat'),
        ('Gandhinagar', 'Gujarat'),
        ('Porbandar', 'Gujarat'),
        ('Himatnagar', 'Gujarat'),
        ('Kota', 'Rajasthan'),
        ('Vijayawada', 'Andhra Pradesh'),
        ('Hubli', 'Karnataka'),
        ('Mysore', 'Karnataka'),

    ]
    
    data = []
    location_id = location_start_id
    for city, state in cities_states:
        zipcode = f"{random.randint(110000, 999999)}"
        # active_flag = np.random.choice([True , False], p=[0.90, 0.1])
        active_flag = True
        created_date = fake.date_time_between(start_date='-5y', end_date='-6m')
        modified_date = fake.date_time_between(start_date=created_date, end_date='now') if random.random() > 0.3 else None
        
        data.append({
            'LocationID': location_id,
            'City': city,
            'State': state,
            'PinCode': zipcode,
            'ActiveFlag': active_flag,
            'CreatedDate': created_date,
            'ModifiedDate': modified_date
        })
        location_id += 1
        
        # if location_id >= location_end_id:
            # break
    
    return pd.DataFrame(data)


def generate_restaurant_data(restaurant_start_id, restaurant_end_id, location_df):
    # Cuisine types
    cuisine_types = [
        "North Indian", "South Indian", "Chinese", "Italian", "Continental", 
        "Mediterranean", "Mexican", "Thai", "Japanese", "Lebanese", 
        "Mughlai", "Street Food", "Desserts", "Beverages", "Fast Food",
        "Cafe", "Bakery", "Ice Cream", "Pizza", "Burger"
    ]

    # Restaurant names
    restaurant_names = [
        # Gujarati Restaurants
        # Gujarati Restaurants
    "Kathiyawadi Rasoi", "Surti Tadka", "Manek Chowk Bites", "Bhavnagari Zaika", "Rajkot Rasoi", "Gujarati Zaika",
    "Vadodara Bites", "Ahmedabadi Tandoor", "Kutchhi Rasoi", "Saurashtra Flavors", "Locho Junction",
    "Dhokla Delight", "Thepla House", "Fafda Jalebi Corner", "Undhiyu Bhavan", "Khichdi Khaas", "Gujarati Shaan",
    "Ghar ni Rasoi", "Rotlo Rajwadi", "Kathiyawadi Dhamaka", "Desi Gujju Tadka", "Swaminarayan Bhojan",
    "Saurashtra Kitchen", "Bhakarwadi Hub", "Farsan Street", "Ponk Treats", "Dal Dhokli Junction",
    "Handvo Delights", "Khaman House", "Gujarati Maharaj", "Rasoi ni Mahak", "Garam Rotlo", "Rajwadi Farsan",
    "Kathiyawadi Rasoi Express", "Surti Tadka Express", "Manek Chowk Bites Express", "Bhavnagari Zaika Express",
    "Rajkot Rasoi Express", "Gujarati Zaika Express", "Vadodara Bites Express", "Ahmedabadi Tandoor Express",
    "Kutchhi Rasoi Express", "Saurashtra Flavors Express", "Locho Junction Express", "Dhokla Delight Express",
    "Thepla House Express", "Fafda Jalebi Corner Express", "Undhiyu Bhavan Express", "Khichdi Khaas Express",
    "Gujarati Shaan Express", "Ghar ni Rasoi Express", "Rotlo Rajwadi Express", "Kathiyawadi Dhamaka Express",
    "Desi Gujju Tadka Express", "Swaminarayan Bhojan Express", "Saurashtra Kitchen Express", "Bhakarwadi Hub Express",
    "Farsan Street Express", "Ponk Treats Express", "Dal Dhokli Junction Express", "Handvo Delights Express",
    "Khaman House Express", "Gujarati Maharaj Express", "Rasoi ni Mahak Express", "Garam Rotlo Express",
    "Rajwadi Farsan Express", "Kathiyawadi Rasoi Bhavan", "Surti Tadka Zaika", "Manek Chowk Dhamaka",
    "Bhavnagari Rasoi", "Rajkot Tandoor", "Gujarati Tandoor", "Vadodara Kitchen", "Ahmedabadi Zaika",
    "Kutchhi Flavors", "Saurashtra Rasoi", "Locho Tadka", "Dhokla Tandoor", "Thepla Delight", "Fafda House",
    "Undhiyu Tadka", "Khichdi Junction", "Gujarati Delight", "Ghar ni Thali", "Rotlo Tadka", "Kathiyawadi Thali",
    "Desi Gujju Express", "Swaminarayan Kitchen", "Saurashtra Express", "Bhakarwadi Delight", "Farsan Express",
    "Ponk Zaika", "Dal Dhokli Bhavan", "Handvo Rasoi", "Khaman Delight", "Gujarati Maharaj Bhavan",
    "Rasoi ni Rasmalai", "Garam Thali", "Rajwadi Delight",
    
        # Maharashtrian Restaurants
  
        
        "Puneri Misal", "Kolhapuri Tadka", "Mumbai Pav Bhaji", "Shivneri Bhojanalay", "Solapuri Chaska",
        "Thalipeeth House", "Khandeshi Spice", "Malvani Coastal Kitchen", "Vada Pav Express", "Nagpuri Saoji Rasoi",
        "Peshwai Bhojan", "Puran Poli Junction", "Kokan King", "Shahi Misal", "Maharashtra Bhavan", "Bambaiya Zaika",
        "Wada Pav Junction", "Zunka Bhakar Thali", "Koliwada Seafood", "Aamras Thali", "Nagpuri Saoji",
        "Maharashtrian Feast", "Modak Magic", "Pitla Bhakri House", "Sabudana House", "Bombay Thali",
        "Bajri Bhakri Tadka", "Ganpati Bhojan", "Saswad Special Thali", "Shree Krishna Misal", "Bhaat Ghar",
        "Sahyadri Delights", "Deccan Spice", "Kokan Rasoi", "Shiv Bhojanam", "Kolhapuri Kitchen",
        "Puneri Misal Express", "Kolhapuri Tadka Junction", "Mumbai Pav Bhaji Bhavan", "Shivneri Bhojanalay Tadka",
        "Solapuri Chaska Rasoi", "Thalipeeth House House", "Khandeshi Spice Delight", "Malvani Coastal Kitchen Spot",
        "Vada Pav Express Treats", "Nagpuri Saoji Rasoi Café", "Peshwai Bhojan Express", "Puran Poli Junction Junction",
        "Kokan King Bhavan", "Shahi Misal Tadka", "Maharashtra Bhavan Rasoi", "Bambaiya Zaika House",
        "Wada Pav Junction Delight", "Zunka Bhakar Thali Spot", "Koliwada Seafood Treats", "Aamras Thali Café",
        "Nagpuri Saoji Express", "Maharashtrian Feast Junction", "Modak Magic Bhavan", "Pitla Bhakri House Tadka",
        "Sabudana House Rasoi", "Bombay Thali House", "Bajri Bhakri Tadka Delight", "Ganpati Bhojan Spot",
        "Saswad Special Thali Treats", "Shree Krishna Misal Café", "Bhaat Ghar Express", "Sahyadri Delights Junction",
        "Deccan Spice Bhavan", "Kokan Rasoi Tadka", "Shiv Bhojanam Rasoi", "Kolhapuri Kitchen House",
        "Puneri Misal Express Delight", "Kolhapuri Tadka Junction Treats", "Mumbai Pav Bhaji Bhavan Café",
        "Shivneri Bhojanalay Tadka Spot", "Solapuri Chaska Rasoi Treats", "Thalipeeth House House Delight",
        
        # South Indian Restaurants
        "Madras Tiffin", "Keralam Flavors", "Chettinad Curry House", "Andhra Spice", "Dosa Junction", "Idli Dosa Corner",
        "Malabar Magic", "Coconut Grove", "Udupi Sagar", "Filter Coffee House", "Rasam Rasoi", "Karnataka Bites",
        "Mysore Delights", "Tamil Nadu Kitchen", "Banana Leaf Bhojan", "Rava Dosa Delight", "Masala Dosa House",
        "Sambar Spice", "Appam & Stew", "Hyderabadi Biryani House", "Andhra Bhojan", "Pongal Place", "Vada Rasoi",
        "Ragi Roti House", "Chennai Café", "Kerala Sadhya", "Mysore Spice", "Bisi Bele Bhat Junction",
        "Payasam & More", "Pesarattu Palace", "Coconut Curry House", "Tamarind Treats", "Namma Ooru Kitchen",
        "Curry Leaf Café", "Udupi Bhavan", "Idli Express", "Steamy South", "Dakshin Zaika", "Thayir Saadham Spot",
        "Dosai Delight", "South Spice Hub", "Sambhar & Rice", "Kerala Express", "Idli & More", "South Thali Café",
        "Dosa Factory", "South Bites", "The Idli House", "Biryani Bhojanam", "Dravidian Delight", "Café Andhra",

        # North Indian Restaurants
        "Tandoori Junction", "Biryani Mahal", "Shahi Rasoi", "Dal Makhani Dhaba", "Mughlai Feast", "Butter Naan Bhavan",
        "Lajawab Tikka House", "Haveli Zaika", "Punjabi Rasoi", "Dilli Darbar", "Kebabs & Curries", "Rajputana Kitchen",
        "Royal Thali", "Tandoor-e-Zaika", "Lucknowi Biryani Point", "Chandni Chowk Chaat", "Amritsari Kulcha House",
        "Pind Da Swag", "Dilli Ka Tandoor", "Shahi Mughlai", "Zayka Darbar", "Grand Biryani Hub", "Rajwada Rasoi",
        "Paratha Junction", "North Indian Delights", "Bhature Chole Point", "Tandoor Mahal", "Peshawari Kitchen",
        "Biryani & Kebabs", "Pind Bhatura House", "Dum Biryani Express", "Makhni Curry House", "Royal Spice Kitchen",
        "Mughal Zaika", "Amritsar Thali", "Punjabi Thali House", "Zamindar Bhojan", "Delhi Belly Express",
        "Tandoori Tadka", "Awadhi Aroma", "Pind Curry House", "Naan & Tikka", "Rajput Royalty", "Thali & Tandoor",
        "Mughlai Bites", "Kebab-e-Khaas", "Zaika-e-Punjab", "North Biryani Kitchen", "Shahi Curry Corner",
        "Royal Haveli Rasoi", "Bhature Bhavan",

        # Chinese Restaurants (Indian Style)
        "Dragon Chilli House", "Hakka Street", "Schezwan Junction", "Manchurian Hub", "Red Pepper Bistro",
        "Chowmein Express", "Beijing Bites", "Wok & Roll", "Kung Pao Delights", "Desi Chinese Dhaba",
        "Spicy Noodle House", "Sichuan Spice", "Hakka Fusion", "Shanghai Zaika", "Noodle & Rice Corner",
        "Chilli Garlic Express", "Tandoori Momos", "Chopstick Bistro", "Hot Wok Kitchen", "Dim Sum Delight",
        "Schezwan Rasoi", "Hunan House", "Dumpling Junction", "The Great Wall Eatery", "Momo King",
        "Beijing Bowl", "Asian Zaika", "Stir Fry & More", "Chilli Wok", "Shanghai Garden", "Dragon Spice House",
        "Momo Street", "Chinese Wok Hub", "Spice Dragon", "Mandarin Masala", "Tangra Treats", "Wok On Fire",
        "Fusion China Express", "Spicy Bamboo Kitchen", "Red Dragon Bites", "Desi Wok & More", "Bamboo Bowl",
        "Hot Pot Zaika", "Ginger & Garlic Wok", "Wok & Noodles", "Zen Dragon", "Oriental Tadka", "Momo Express",
        "Chilli Chopsticks", "Wokland", "Dumpling Delight",

        # Italian Restaurants
        "Pizza Haveli", "Pasta Junction", "Spaghetti Zaika", "Cheesy Crust Kitchen", "Tuscany Treats",
        "Margherita Magic", "Risotto Delight", "Napoli Pizzeria", "Romeo’s Italian", "Fettuccine Feast",
        "Garlic Bread House", "Alfredo Delights", "Pasta Bites", "Neapolitan Bakes", "Mozzarella Masti",
        "Gnocchi Junction", "Lasagna Land", "Milan Pizza House", "Basil & Oregano", "Carbonara Kitchen",
        "Penne Pasta Express", "Trattoria Italia", "Tuscany Bistro", "Pizza Town", "Oven Fresh Pizzas",
        "Amore Pizzeria", "Cheesy Pizza House", "Mamma Mia Pasta", "Focaccia Delights", "Parmigiana Palace",
        "Woodfire Pizza Junction", "Olive & Tomato", "Italian Bistro", "Mediterranean Spice",
        "Vesuvio Kitchen", "The Italian Crust", "Pizza al Forno", "Pasta Mania", "Mozza & Co.",
        "Rustic Roma", "La Tavola", "Ciao Bella Kitchen", "Truffle & Thyme", "Azzurro Italia",
        "Pasta Fresca", "Basilico Express", "Dolce Vita", "Italiano Delights", "Pesto Place", "Toscano Café",

        # Fusion & Multi-Cuisine Restaurants
        "Global Bites", "Fusion Feast", "Taste of India", "World on a Plate", "Desi Continental Fusion",
        "Spice Route", "Food Carnival", "Bistro India", "The Grand Buffet", "Zaika Junction",
        "Tandoor & Wok", "Masala Fusion", "The Indian Table", "Spicy Twist", "Diverse Dishes",
        "Royal Banquet", "Indian Aroma", "Saffron & Spice", "Zesty Bites", "Curry & Beyond",
        "Urban Masala", "Foodies Junction", "Heritage Feast", "Spice & Grill", "Flavors of India",
        "Zaika Bazaar", "Tandoori Nights", "Foodgasm Express", "Desi Flavors Hub", "Epicurean Delights",
        "Global Tandoor", "Indo-Chinese Twist", "Flavorscape Express", "Tadka Treats", "Food Fusion Café",
        "Pan Asia Bites", "Zaika Mélange", "Continental Curry Hub", "Curry Carnival", "Global Grub House",
        "Zaika Tales", "Desi Delight Hub", "IndiMex Bites", "Urban Thali Kitchen", "Global Taste Bazaar",
        "Mix & Match Kitchen", "Fusion Thali", "Culinary Carousel", "Urban Feast", "Zaika Collective",
        
        # Minimalist & Elegant
        "Ember & Sage", "The Willow Table", "Solstice Kitchen", "Luna Bistro", "Ethereal Bites",
        "Velvet Fork", "Noir & Blanc", "The Golden Spoon", "Azure Plate", "Crisp & Co.",
        "White Marble", "Slate & Silver", "Ivory Ember", "Feather & Flame", "The Still Room",
        "Quill & Crust", "Silken Dish", "The Quiet Fork", "Alabaster Table", "Whispering Bites",

        # Nature-Inspired
        "Evergreen Eatery", "Wild Thyme", "Flora & Fawn", "Oak & Olive", "Rosewood Kitchen",
        "Meadow & Vine", "Pebble & Leaf", "Cedar & Sage", "Willow & Vine", "Harvest Moon Cafe",
        "Moss & Morel", "Sunbeam Grove", "Thorn & Blossom", "Pine & Petal", "Sagebrush Table",
        "Fern & Flame", "Dewdrop Kitchen", "Wisteria & Co.", "The Garden Table", "Briarwood Bistro",

        # Luxury & Fine Dining
        "Opal Dining", "Celeste", "The Gilded Fork", "Ambrosia", "Ivory & Gold",
        "Élan", "Veranda", "Noir Luxe", "Maison Blanc", "Astoria Kitchen",
        "Crimson Crown", "Orchid & Velvet", "Saffron Ember", "Regal Bloom", "Pearl & Caviar",
        "Aurum", "Silken Spoon", "The Monarch Table", "Chateau Lumière", "Luxe Reserve",

        # Trendy & Hipster
        "Urban Fork", "The Rusty Ladle", "Neon Bites", "Vibe & Dine", "The Graze House",
        "Wanderlust Café", "Nomad Bites", "Alchemy Kitchen", "Boho Bites", "Sage & Citrus",
        "Craft & Crumb", "Vinyl & Vanilla", "Grain Theory", "Wool & Whisk", "Brew & Barrel",
        "The Indie Dish", "Local Lore", "Brick & Brew", "The Vibe Spot", "Thread & Toast",

        # Futuristic & High-End
        "Astra", "NOVA", "Nebula Eats", "Lumen", "Prism Kitchen",
        "Gravity Bites", "Cosmo Eats", "Vertex Dine", "Eon", "Horizon Bistro",
        "Quantum Table", "Stellar Spoon", "Eclipse Dine", "Zero Point Kitchen", "Nebula Nosh",
        "CyberPlate", "Ion Flame", "Lunar Lounge", "Polaris Pantry", "Galactic Graze",

        # Fusion & Global
        "Nomad's Feast", "Palette", "Mélange", "Épicure", "Global Graze",
        "Zest & Zing", "Euphoric Bites", "Ambrosia Fusion", "Maison de Flavors", "Latitude Kitchen",
        "Crossroads Kitchen", "Spice Atlas", "Culture Spoon", "Nomadic Table", "Infusion Lane",
        "Babel Bites", "Continental Fork", "Taste Tapestry", "The World Palate", "Global Ember",

        # Chic & Casual
        "Toast & Tonic", "Basil & Bloom", "Hearth & Home", "Drizzle & Dash", "Sizzle & Swirl",
        "Amber Plate", "Dewdrop Café", "Cocoa & Chai", "Grove Kitchen", "Rustic Charm",
        "Mellow Morsel", "Sundown Spoon", "Pine & Pesto", "Whisk & Willow", "Casual Crumbs",
        "The Everyday Bite", "Mint & Mustard", "The Cozy Spoon", "Simple & Sage", "Mirth Kitchen",

        # Coastal & Tropical
        "Tide & Table", "Seabreeze Bites", "Horizon Cove", "Driftwood Dine", "Blue Lagoon Kitchen",
        "Salt & Sand", "Coral Café", "Sunset Grill", "Shoreline Eats", "Palm & Pineapple",
        "Coastal Ember", "The Coconut Bowl", "Marina Bites", "Wave & Whisk", "Beachside Bistro",
        "Tropicana Plate", "Ocean Grove", "SeaSalt Kitchen", "Aqua & Flame", "Palm Breeze Eats",

        # Asian-Inspired Modern
        "Zen Garden", "Umami House", "Hikari", "Ocha & Sake", "Satori Bistro",
        "Mizu", "Kaizen Kitchen", "Yume Sushi", "Chopsticks & Co.", "Koi & Co.",
        "Shibuya Bites", "Ramen & Rice", "Kimchi Flame", "Ichigo House", "Tea & Tempura",
        "Sakura Spoon", "Yaki & Co.", "Tokyo Ember", "Spice Kyoto", "Bento Theory",

        # Coffee & Dessert Bar Style
        "Mocha Muse", "Espresso Lane", "Vanilla Bean & Co.", "Sugar & Spice", "The Cocoa Loft",
        "Froth & Foam", "Lavender & Latte", "Sweet Serenade", "Velvet Crumb", "The Artisan Bean",
        "Crème & Cocoa", "Roast Ritual", "Bean & Butter", "Caffeine Bloom", "Sweet & Steep",
        "The Daily Drip", "Honey & Roast", "ChocoWhisk", "Barista’s Bite", "Vanilla Grove",

        # Experimental & Unique
        "The Gastronomy Lab", "Flavorscape", "Savant Bites", "The Test Kitchen", "Alchemy Bites",
        "InnovEat", "Taste Lab", "Avant-Garde Eats", "Culinary Canvas", "Bold & Butter",
        "Flavor Reactor", "Kitchen X", "Fork Theory", "Taste Architects", "The Flavor Foundry",
        "Experimental Dish", "The Edible Concept", "Modern Morsels", "Palate Shift", "Spoon Studio"

    ]

    
     # Coupon types with different discount structures
    coupon_types = [
        # Regular coupons - [coupon_code, min_amount, discount_amount, description, payment_method (None for any)]
        ["WELCOME", 500, 100, "₹100 off on orders above ₹500", None],
        ["SPECIAL", 800, 150, "₹150 off on orders above ₹800", None],
        ["WEEKEND", 1000, 200, "₹200 off on orders above ₹1000", None],
        ["FESTIVAL", 1500, 300, "₹300 off on orders above ₹1500", None],
        ["FIRSTORDER", 300, 75, "₹75 off on your first order above ₹300", None],
        
        # Payment-specific coupons
        ["CREDITCARD", 750, 150, "₹150 off on orders above ₹750 with Credit Card", "CreditCard"],
        ["DEBITCARD", 600, 100, "₹100 off on orders above ₹600 with Debit Card", "DebitCard"],
        ["UPISAVE", 400, 80, "₹80 off on orders above ₹400 with UPI", "UPI"],
        ["CASHBACK", 500, 75, "₹75 off on Cash payments above ₹500", "Cash"]
    ]
    
    data = []
    # Get only active locations
    active_locations = location_df[location_df['ActiveFlag'] == True]
    
    # Distribute restaurants across all active locations
    restaurant_id = restaurant_start_id
    
    # Ensure each location has at least 2-5 restaurants
    for _, location_row in active_locations.iterrows():
        location_id = location_row['LocationID']
        city_name = location_row['City']
        
        # Create 2-5 restaurants per location
        num_restaurants_for_location = random.randint(40, 100)
        location_created_date = location_df[location_df['LocationID'] == location_id]['CreatedDate'].values[0]
        
        for _ in range(num_restaurants_for_location):
            # Generate restaurant name
            name = random.choice(restaurant_names)
            
            # Generate cuisine types (1-3 random cuisine types)
            num_cuisines = random.randint(1, 3)
            restaurant_cuisines = random.sample(cuisine_types, num_cuisines)
            cuisine_type = ", ".join(restaurant_cuisines)
            
            # Other fields
            pricing_for_2 = random.randint(200, 2000)
            phone = f"9{random.randint(100000000, 999999999)}"
            
            # Opening and closing hours
            # opening_hour = random.randint(7, 12)
            # closing_hour = random.randint(17, 23)
            # operating_hours = f"{opening_hour}:00  - {closing_hour}:00 "
            
             # New fixed time slots with exact probabilities
            shifts = [
                (6, 11, 0.14),   # 06:00–11:00
                (11, 15, 0.12),  # 11:00–15:00
                (12, 16, 0.18),  # 12:00–16:00
                (16, 19, 0.11),  # 16:00–19:00
                (19, 23, 0.18),  # 19:00–23:00
                (20, 24, 0.18),  # 20:00–00:00
                (24, 30, 0.07)   # 00:00–06:00 (Next day)
            ]

            # Choose shift based on new probabilities
            chosen_shift = random.choices(shifts, weights=[s[2] for s in shifts])[0]
            start_raw, end_raw, shift_prob = chosen_shift

            # Convert to 24-hour time
            opening_hour = start_raw % 24
            closing_hour = end_raw % 24
            operating_hours = f"{opening_hour:02d}:00 - {closing_hour:02d}:00"
            
            active_flag = np.random.choice([True, False], p=[0.9, 0.1])
            open_status = 'Open' if active_flag == True else 'Closed'
    
            # Address and locality
            locality = f"{fake.street_name()}"
            city_pincode = location_row['PinCode']
            restaurant_address = f'{random.choice(["Ground Floor,","First Floor,","Second Floor,","Third Floor,", ""])} {locality}, {city_name} - {city_pincode}'
            
            # Coordinates (latitude and longitude for India)
            latitude = random.uniform(8.4, 37.6)
            longitude = random.uniform(68.7, 97.25)
            
            # Dates
            created_date = fake.date_time_between(start_date=pd.to_datetime(location_created_date), end_date='-4m')
            modified_date = fake.date_time_between(start_date=created_date, end_date='now')
            
            # Generate 3-6 coupons for this restaurant
            num_coupons = random.randint(3, 5)
            restaurant_coupons = random.sample(coupon_types, min(num_coupons, len(coupon_types)))
            
            # Create JSON structure for coupons
            coupons_json = []
            for coupon in restaurant_coupons:
                # Slightly vary the coupon parameters to make them unique
                min_amount_variation = random.uniform(0.9, 1.1)
                discount_variation = random.uniform(0.9, 1.1)
                
                coupon_data = {
                    "code": f"{coupon[0]}_{restaurant_id}",
                    "min_amount": int(coupon[1] * min_amount_variation),
                    "discount_amount": int(coupon[2] * discount_variation),
                    "description": coupon[3],
                    "payment_method": coupon[4]  # None or specific payment method
                }
                coupons_json.append(coupon_data)
            
            # Convert to JSON string
            coupons_str = json.dumps(coupons_json)
            
            
            data.append({
                'RestaurantID': restaurant_id,
                'Name': name,
                'CuisineType': cuisine_type,
                'Pricing_for_2': pricing_for_2,
                'Restaurant_Phone': phone,
                'OperatingHours': operating_hours,
                'LocationID': location_id,
                'ActiveFlag': active_flag,
                'OpenStatus': open_status,
                'Locality': locality,
                'Restaurant_Address': restaurant_address,
                'Ratings': 0,
                'Coupons': coupons_str,  # New field for coupons
                'Latitude': latitude,
                'Longitude': longitude,
                'CreatedDate': created_date,
                'ModifiedDate': modified_date
            })
            
            restaurant_id += 1
            # if restaurant_id >= restaurant_end_id:
            #     break
                
        # if restaurant_id >= restaurant_end_id:
        #     break
    
    return pd.DataFrame(data)


def generate_menu_data(menu_start_id, menu_end_id, restaurant_df):
    categories = ["Appetizers", "Main Course","South Indian","Chinese & Indo-Chinese","Italian & Continental", "Desserts", "Beverages", "Snacks & Street Food"]
    item_names = {
        "Appetizers": [
            "Samosa", "Paneer Tikka", "Chicken Tikka", "Aloo Tikki", "Fish Fry", "Spring Rolls",
            "Hara Bhara Kebab", "Seekh Kebab", "Chicken Wings", "Prawn Skewers", "Dahi Puri",
            "Masala Papad", "Cheese Balls", "Chilli Paneer", "Stuffed Mushrooms", "Tandoori Broccoli",
            "Tandoori Prawns", "Cheese Corn Nuggets", "Honey Chilli Potatoes", "Gobi Manchurian"
        ],
    
        "Main Course": [
            "Butter Chicken", "Paneer Butter Masala", "Dal Makhani", "Chole Bhature", "Biryani",
            "Rogan Josh", "Palak Paneer", "Malai Kofta", "Mutton Curry", "Fish Curry",
            "Hyderabadi Biryani", "Chettinad Chicken", "Vegetable Korma", "Baingan Bharta",
            "Methi Malai Murg", "Mushroom Masala", "Dum Aloo", "Kadai Paneer", "Shahi Paneer",
            "Goan Prawn Curry", "Sarson Ka Saag & Makki Roti", "Kosha Mangsho", "Egg Curry",
            "Mutton Handi", "Chicken Rezala", "Malabar Fish Curry", "Sindhi Kadhi",
            "Paneer Lababdar", "Dal Tadka", "Litti Chokha", "Chicken Chettinad"
        ],
    
        "South Indian": [
            "Masala Dosa", "Plain Dosa", "Rava Dosa", "Set Dosa", "Neer Dosa",
            "Mysore Masala Dosa", "Onion Uttapam", "Tomato Uttapam", "Pesarattu", "Idli",
            "Medu Vada", "Rasam Rice", "Sambar Rice", "Curd Rice", "Lemon Rice",
            "Bisibele Bath", "Pongal", "Chettinad Chicken Curry", "Vegetable Stew",
            "Kerala Parotta with Egg Curry", "Appam with Stew", "Ghee Roast Dosa"
        ],
    
        "Chinese & Indo-Chinese": [
            "Hakka Noodles", "Chilli Chicken", "Gobi Manchurian", "Chicken Manchurian",
            "Schezwan Noodles", "Spring Rolls", "Momos", "Hot & Sour Soup",
            "Sweet Corn Soup", "Chicken 65", "Chilli Paneer", "Fried Rice",
            "Schezwan Fried Rice", "Paneer Chilli Dry", "Triple Schezwan Rice",
            "Manchow Soup", "Crispy Honey Chilli Potatoes"
        ],
    
        "Italian & Continental": [
            "Margherita Pizza", "Farmhouse Pizza", "Pepperoni Pizza", "Four Cheese Pasta",
            "Penne Arrabbiata", "Spaghetti Aglio e Olio", "Lasagna", "Mushroom Risotto",
            "Garlic Bread", "Caesar Salad", "Bruschetta", "Grilled Chicken Steak",
            "Lemon Herb Fish", "Stuffed Bell Peppers", "Tacos", "Quesadillas", "Burritos"
        ],
    
        "Desserts": [
            "Gulab Jamun", "Rasgulla", "Kheer", "Jalebi", "Kulfi", "Ras Malai",
            "Gajar Halwa", "Mysore Pak", "Peda", "Sandesh", "Basundi", "Modak",
            "Chhena Poda", "Kaju Katli", "Phirni", "Shahi Tukda", "Mango Shrikhand",
            "Chocolate Brownie", "Fruit Custard", "Falooda", "Ice Cream Sundae",
            "Tiramisu", "Cheesecake", "Chocolate Mousse", "Apple Pie"
        ],
    
        "Beverages": [
            "Masala Chai", "Lassi", "Nimbu Pani", "Cold Coffee", "Fruit Juice",
            "Coconut Water", "Aam Panna", "Buttermilk", "Thandai", "Falooda",
            "Filter Coffee", "Badam Milk", "Rose Sharbat", "Sugarcane Juice",
            "Virgin Mojito", "Mango Lassi", "Iced Tea", "Cold Brew Coffee"
        ],
    
        "Snacks & Street Food": [
            "Pav Bhaji", "Bhel Puri", "Pani Puri", "Vada Pav", "Pakora", "Dhokla",
            "Kachori", "Sev Puri", "Dabeli", "Aloo Chaat", "Egg Roll", "Frankie",
            "Chana Chaat", "Keema Pav", "Mirchi Bajji", "Chowmein", "Corn on the Cob",
            "Kathi Roll", "Tandoori Momos", "Samosa Chaat", "Batata Vada",
            "Masala Puri", "Veg Frankie", "Misal Pav"
        ]
    }
    
    descriptions = ["Delicious and authentic {}.", "A popular Indian dish.", "Traditional Indian {} with rich flavors.", "A must-try {} from India.", "Classic {} with a twist."]
    
    data = []
    menu_id = menu_start_id
    
    # Ensure each restaurant has at least a few menu items
    for restaurant_id in restaurant_df['RestaurantID']:
        # Create 3-10 menu items per restaurant
        num_items = random.randint(10, 15)
        
        # Create a set to track what items we've already added to this restaurant
        restaurant_items = set()
        restaurant_created_date = restaurant_df[restaurant_df['RestaurantID'] == restaurant_id]['CreatedDate'].values[0]
        
        for _ in range(num_items):
            category = random.choice(categories)
            
            # Try to find an item name not already in use for this restaurant
            attempts = 0
            while attempts < 20:  # Prevent infinite loop
                item_name = random.choice(item_names[category])
                if item_name not in restaurant_items:
                    restaurant_items.add(item_name)
                    break
                attempts += 1
            
            if attempts >= 20:
                # Just pick any item if we can't find a unique one
                item_name = random.choice(item_names[category])
            
            # Set item type based on item name
            if item_name in ["Chicken Tikka", "Fish Fry", "Seekh Kebab", "Chicken Wings", "Prawn Skewers",
                             "Tandoori Prawns", "Chicken 65", "Egg Roll", "Keema Pav", "Tandoori Momos",
                            "Butter Chicken", "Rogan Josh", "Mutton Curry", "Fish Curry", "Hyderabadi Biryani",
                            "Chettinad Chicken", "Egg Curry", "Chicken Rezala", "Goan Prawn Curry",
                            "Methi Malai Murg", "Malabar Fish Curry", "Mutton Handi", "Chicken Chettinad",
                            "Kerala Parotta with Egg Curry", "Chettinad Chicken Curry",
                            "Chilli Chicken", "Chicken Manchurian", "Chicken 65", "Hot & Sour Soup (Chicken)",
                            "Manchow Soup (Chicken)", "Schezwan Chicken Fried Rice",
                            "Pepperoni Pizza", "Grilled Chicken Steak", "Lemon Herb Fish",
                            "Egg Roll", "Keema Pav", "Kathi Roll",
                            "Chicken Biryani", "Mutton Biryani", "Prawns Biryani"]:
                item_type = "Non-Veg"
            else:
                item_type = "Veg"
            
            description = random.choice(descriptions).format(item_name)
            # price = random.randint(50, 500)
            price_tier = random.choices(
                population=[
                    (50, 99),
                    (100, 199),
                    (200, 299),
                    (300, 399),
                    (400, 500)
                ],
                weights=[0.2, 0.30, 0.35, 0.1, 0.05],
                k=1
            )[0]

            price = random.randint(price_tier[0], price_tier[1])
            created_date = fake.date_time_between(start_date=pd.to_datetime(restaurant_created_date), end_date=pd.to_datetime(restaurant_created_date) + pd.DateOffset(months=2))
            modified_date = fake.date_time_between(start_date=created_date, end_date='now')

            data.append({
                "MenuItemID": menu_id,
                "RestaurantID": restaurant_id,
                "ItemName": item_name,
                "Description": description,
                "Price": price,
                "Category": category,
                "Availability": True,
                "ItemType": item_type,
                "Ratings" : 0,
                "CreatedDate": created_date,
                "ModifiedDate": modified_date
            })
            
            menu_id += 1
            # if menu_id >= menu_end_id:
            #     break
                
        # if menu_id >= menu_end_id:
        #     break
    
    return pd.DataFrame(data)


def generate_customer_data(customer_start_id, customer_end_id):
    login_methods = ['GMail_Account', 'Apple_ID', 'Other_EMail']
    genders = ['Male', 'Female', 'Other']
    
    food_preferences = ['Veg', 'Non-Veg', 'Vegan', 'Eggetarian']
    cuisine_types = ['North Indian', 'South Indian', 'Chinese', 'Italian', 'Continental', 
                      'Mediterranean', 'Mexican', 'Thai', 'Japanese', 'Street Food']
    
    data = []
    for i in range(customer_start_id, customer_end_id):
        # Basic info
        gender = np.random.choice(genders , p=[0.49,0.48,0.03])
        if gender == 'Male':
            name = fake.name_male()
        elif gender == 'Female':
            name = fake.name_female()
        else:
            name = fake.name()
            
        mobile = f"{random.randint(7000000000, 9999999999)}"
        email_domain = random.choice(['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com'])
        email = f"{name.lower().replace(' ', '')}{random.randint(1, 999)}@{email_domain}"
        login_using = random.choice(login_methods)
        
        # Generate random birth date (18-60 years old)
        dob = fake.date_of_birth(minimum_age=15, maximum_age=75)
        
        # Generate anniversary date (0-30 years after birth date)
        years_after_dob = random.randint(18, 30)  # Most people marry after 21
        anniversary = dob + timedelta(days=365 * years_after_dob)
        
        # Check if anniversary is in the future
        anniversary = anniversary if anniversary < datetime.date(datetime.now()) else None
        anniversary = np.random.choice([anniversary, None], p=[0.7, 0.3])

        # Generate food preferences
        food_pref = random.choice(food_preferences)
        num_cuisines = random.randint(1, 5)
        cuisine_pref = random.sample(cuisine_types, num_cuisines)
        
        preferences = {
            'FoodPreference': food_pref,
            'CuisineTypes': cuisine_pref
        }
        # Customer reting
        rating = round(random.uniform(3.0, 5.0), 1)
        # Dates
        created_date = fake.date_time_between(start_date='-5y', end_date='-1m')
        modified_date = fake.date_time_between(start_date=created_date, end_date='now')
        
        data.append({
            'CustomerID': i,
            'Full_Name': name,
            'Email': email,
            'Mobile_no': mobile,
            'LoginByUsing': login_using,
            'Gender': gender,
            'DOB': dob,
            'Anniversary': anniversary,
            'Rating': rating,
            'Preferences': json.dumps(preferences),
            'CreatedDate': created_date,
            'ModifiedDate': modified_date
        })
    
    return pd.DataFrame(data)


def generate_customer_address_data(customer_address_start_id, customer_address_end_id, customer_df, location_df):
    address_types = ['Home', 'Work', 'Other']
    
    data = []
    address_id = customer_address_start_id
    
    # Get active locations for address assignment
    active_locations = location_df[location_df['ActiveFlag'] == True]
    
    # Generate 1-4 addresses per customer in different cities
    for customer_id in customer_df['CustomerID']:
        # Number of addresses for this customer
        num_addresses = random.randint(1, 3)
        
        # Randomly select locations for this customer
        customer_locations = active_locations.sample(min(num_addresses, len(active_locations)))
        
        for idx, location_row in enumerate(customer_locations.iterrows()):
            location_data = location_row[1]  # Get the Series from the tuple
            location_id = location_data['LocationID']
            city = location_data['City']
            state = location_data['State']
            pincode = location_data['PinCode']
            
            flat_no = str(random.randint(1, 50))
            # house_no = str(random.randint(1, 10)) if random.random() > 0.5 else ""
            floor = str(random.randint(1, 40)) if random.random() > 0.3 else ""
            building = fake.company() + " " + random.choice(['Apartments', 'Residency', 'Heights', 'Towers', 'Complex'])
            landmark = random.choice(["Near ","Opp. ","B/h. ","Beside ","Behind ",""]) + fake.company()
            
            # Locality
            locality =  locality = random.choice([
                'Saket', 'Connaught Place', 'Dwarka', 'Vasant Kunj', 'South Extension',
                'Rohini', 'Karol Bagh', 'Pitampura', 'Janakpuri', 'Lajpat Nagar',
                'Malviya Nagar', 'Greater Kailash', 'Hauz Khas', 'Mayur Vihar', 'Rajouri Garden'
            ])
            
            # Coordinates
            latitude = random.uniform(8.4, 37.6)
            longitude = random.uniform(68.7, 97.25)
            coordinates = f"{latitude},{longitude}"
            
            # Primary flag (only one primary address per customer)
            primary_flag = True if idx == 0 else False
            address_type = random.choice(address_types)
            
            # Dates
            customer_created_date = customer_df[customer_df['CustomerID'] == customer_id]['CreatedDate'].values[0]
            created_date = fake.date_time_between(start_date=pd.to_datetime(customer_created_date), end_date='now')
            modified_date = fake.date_time_between(start_date=created_date, end_date='now') if random.random() > 0.3 else None

            data.append({
                'AddressID': address_id,
                'CustomerID': customer_id,
                'FlatNo/HouseNo': flat_no,
                'Floor': floor,
                'Building': building,
                'Landmark': landmark,
                'Locality': locality,
                'City': city,
                'State': state,
                'PinCode': pincode,
                'Coordinates': coordinates,
                'PrimaryFlag': primary_flag,
                'AddressType': address_type,
                'CreatedDate': created_date,
                'ModifiedDate': modified_date,
                'LocationID': location_id  # Adding this temporarily to link addresses to locations
            })
            
            address_id += 1
            
    
    result_df = pd.DataFrame(data)
    
    return result_df


def generate_login_audit_data(customer_login_audit_start_id, customer_login_audit_end_id, customer_df):
    login_types = ['App', 'Web']
    device_interfaces = ['Android', 'iOS']
    mobile_devices = ['Samsung Galaxy', 'OnePlus', 'Xiaomi', 'Oppo', 'Vivo', 'Realme']
    web_interfaces = ['Chrome', 'Firefox', 'Safari', 'Edge', 'Opera']
    
    data = []
    for i in range(customer_login_audit_start_id, customer_login_audit_end_id):
        # Random customer
        customer_id = random.choice(customer_df['CustomerID'])
        customer_created_date = customer_df[customer_df['CustomerID'] == customer_id]['CreatedDate'].values[0]
        # Login details
        login_type = np.random.choice(login_types, p=[0.9, 0.1])
        
        # Device details depend on login type
        if login_type == 'App':
            device_interface = np.random.choice(device_interfaces, p=[0.7, 0.3])
            mobile_device_name = 'iPhone' if device_interface=='iOS' else random.choice(mobile_devices)
            web_interface = None
        else:  # Web or Desktop
            device_interface = None
            mobile_device_name = None
            web_interface = random.choice(web_interfaces)
        
        # Login timestamp
        last_login = fake.date_time_between(start_date=pd.to_datetime(customer_created_date), end_date='now')
        
        data.append({
            'LoginID': i,
            'CustomerID': customer_id,
            'LoginType': login_type,
            'DeviceInterface': device_interface,
            'MobileDeviceName': mobile_device_name,
            'WebInterface': web_interface,
            'LastLogin': last_login
        })
    
    return pd.DataFrame(data)


def generate_delivery_agent_data(delivery_agents_start_id, delivery_agent_end_id, location_df):
    vehicle_types = ['Bike', 'Scooter']
    
    data = []
    
    # Ensure each active location has at least 2-5 delivery agents
    active_locations = location_df[location_df['ActiveFlag'] == True]
    delivery_agent_id = delivery_agents_start_id
    
    for _, location_row in active_locations.iterrows():
        location_id = location_row['LocationID']
        location_created_date = location_df[location_df['LocationID'] == location_id]['CreatedDate'].values[0]
        # Number of agents for this location
        num_agents = random.randint(100, 150)
        
        for _ in range(num_agents):
            # Basic info
            gender = np.random.choice(['Male', 'Female', 'Other'], p=[0.9, 0.09, 0.01])
            
            if gender == 'Male':
                name = fake.name_male()
            elif gender == 'Female':
                name = fake.name_female()
            else:
                name = fake.name()
                
            phone = f"{random.randint(6000000000, 9999999999)}"
            vehicle_type = random.choice(vehicle_types)
            status = np.random.choice([True, False], p=[0.9, 0.1]) # True = Active , False = Inactive Delivery
            
            # Rating (1.0 to 5.0)
            # rating = round(random.uniform(3.0, 5.0), 1)
            ranges = [(1.0, 2.0), (2.0, 3.0), (3.0, 4.0), (4.0, 5.0)]
            probabilities = [0.05, 0.15, 0.20, 0.60]
            chosen_range = random.choices(ranges, weights=probabilities, k=1)[0]
            rating = round(random.uniform(*chosen_range), 1)
            
            email_domain = random.choice(['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com'])
            email = f"{name.lower().replace(' ', '')}{random.randint(1, 999)}@{email_domain}"
            
            # Dates
            created_date = fake.date_time_between(start_date=pd.to_datetime(location_created_date), end_date='-3m')
            modified_date = fake.date_time_between(start_date=created_date, end_date='now')
            
            data.append({
                'DeliveryAgentID': delivery_agent_id,
                'Full_Name': name,
                'email': email,
                'Mobile_no': phone,
                'VehicleType': vehicle_type,
                'LocationID': location_id,
                'Status': status,
                'Gender': gender,
                'Rating': rating,
                'CreatedDate': created_date,
                'ModifiedDate': modified_date
            })
            
            delivery_agent_id += 1
        #     if delivery_agent_id >= delivery_agent_end_id:
        #         break
                
        # if delivery_agent_id >= delivery_agent_end_id:
        #     break
    
    return pd.DataFrame(data)


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
            
        # Generate 1-5 orders for this customer
        num_orders = random.randint(1, 8)
        
        # Track if this is the first order for this customer
        is_first_order = True
        
        for _ in range(num_orders):
            
            # Randomly select one of the customer's addresses
            address = customer_addresses.sample(1).iloc[0]
            # print(pd.to_datetime(address['CreatedDate']))
            address_id = address['AddressID']
            city = address['City']
            
            
            # Order date (last 90 days)
            order_date = fake.date_time_between(start_date=pd.to_datetime(address['CreatedDate']), end_date='now')
            # print(order_date)
            # Created and modified dates
            created_date = order_date
            modified_date = fake.date_time_between(start_date=created_date, end_date='now')
            
            # Find restaurants in the same city
            city_location_ids = location_df[location_df['City'] == city]['LocationID'].tolist()
            city_restaurants = restaurant_df[restaurant_df['LocationID'].isin(city_location_ids)]
            # print(city_restaurants)
            # Skip if no restaurants found in this city
            if len(city_restaurants) == 0:
                continue
                
            # Select a random restaurant from the same city
            filtered_restaurants = city_restaurants[pd.to_datetime(city_restaurants['CreatedDate']) < order_date]
            # print(filtered_restaurants)
            # print(len(filtered_restaurants))
            if len(filtered_restaurants) == 0:
                continue
            # print('hello')
            restaurant = filtered_restaurants.sample(1).iloc[0]
            # print(restaurant)
            restaurant_id = restaurant['RestaurantID']
            # print(restaurant_id)
            
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
            # days_since_order = (datetime.now() - order_date).days
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
        # print('menu ids',menu_ids)
        if not menu_ids:
            continue
            
        filtered_menu_id = []
        for menu_id in menu_ids:
            if pd.to_datetime(menu_df[menu_df['MenuItemID'] == menu_id]['CreatedDate'].values[0]) < pd.to_datetime(order_date):
                filtered_menu_id.append(menu_id)
        
        if not filtered_menu_id:
            continue
        # print('filtered menu ids',filtered_menu_id)
        # Generate 1-5 random order items
        # num_items = random.randint(1, 5)
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
            # quantity = random.randint(1, 3)
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





location_df = generate_location_data(LOCATION_START_ID, LOCATION_END_ID)
print('Location Data Generated')


restaurant_df = generate_restaurant_data(RESTAURANT_START_ID, RESTAURANT_END_ID, location_df)
print('Restaurant Data Generated')


menu_df = generate_menu_data(MENU_START_ID, MENU_END_ID, restaurant_df)
print('Menu Data Generated')


customer_df = generate_customer_data(CUSTOMER_START_ID, CUSTOMER_END_ID)
print('Customer Data Generated')


address_df = generate_customer_address_data(CUSTOMER_ADDRESS_START_ID, CUSTOMER_ADDRESS_END_ID, customer_df, location_df)
print('Customer Address Data Generated')


login_audit_df = generate_login_audit_data(CUSTOMER_LOGIN_AUDIT_START_ID, CUSTOMER_LOGIN_AUDIT_END_ID, customer_df)
print('Login Audit Data Generated')


delivery_agent_df = generate_delivery_agent_data(DELIVERY_AGENT_START_ID, DELIVERY_AGENT_END_ID, location_df)
print('Delivery Agent Data Generated')


order_df = generate_orders_data(ORDER_START_ID, ORDER_END_ID, customer_df, restaurant_df, address_df, location_df)
print('order data generated')


order_items_df = generate_order_items_data(ORDER_ITEMS_START_ID,order_df, menu_df)
print('order item data generated')
order_df = order_df[(order_df['TotalAmount'] != 0)]
print('order totalprice and all updated')


delivery_df = generate_delivery_data(order_df, delivery_agent_df, DELIVERY_START_ID,restaurant_df)
print('delivered')



    # Remove temporary columns
if 'RestaurantCoupons' in order_df.columns:
    order_df = order_df.drop(columns=['RestaurantCoupons'])
    
if 'AddressID' in order_df.columns:
    order_df = order_df.drop(columns=['AddressID'])

if 'CouponApplied' in order_df.columns:
    order_df = order_df.drop(columns=['CouponApplied'])

if 'LocationID' in address_df.columns:
    address_df =  address_df.drop(columns=['LocationID'])


update_menu_item_ratings(order_items_df,menu_df)
print('menu rattings updated')
update_restaurant_ratings(menu_df,restaurant_df)    
print('restaurant rattings updated')

location_df.to_csv('data1/location.csv',index=False)
customer_df.to_csv('data1/customer.csv', index=False)
address_df.to_csv('data1/customer_address.csv', index=False)
delivery_agent_df.to_csv('data1/delivery_agent.csv', index=False)

delivery_agent_df['CreatedDate'] = delivery_agent_df['CreatedDate'].dt.strftime('%m/%d/%Y %H:%M')
delivery_agent_df['ModifiedDate'] = delivery_agent_df['ModifiedDate'].dt.strftime('%m/%d/%Y %H:%M')
delivery_agent_df.to_json('data1/delivery_agent.json',orient='records', lines=False, indent=4)

restaurant_df.to_csv('data1/restaurant.csv', index=False)
menu_df.to_csv('data1/menu_items.csv', index=False)
login_audit_df.to_csv('data1/login_audit.csv', index=False)
order_df.to_csv('data1/orders.csv', index=False)
order_items_df.to_csv('data1/order_items.csv', index=False)
delivery_df.to_csv('data1/delivery.csv', index=False)


end_time = datetime.now()
print(f"Program ended at: {end_time}")

# Total duration
duration = end_time - start_time
print(f"Total execution time: {duration}")