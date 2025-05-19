import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta, time
import json
import os


# Load both CSVs
df1 = pd.read_csv('data1/order_items.csv')
df2 = pd.read_csv('data2/order_items.csv')

# Combine them
combined = pd.concat([df1, df2], ignore_index=True)

# Optional: remove duplicates if needed
combined = combined.drop_duplicates()

# Save to a new CSV
combined.to_csv('temp/combined.csv', index=False)