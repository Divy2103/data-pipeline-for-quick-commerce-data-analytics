Internship Project: Real-Time Data Pipeline for Restaurant Analytics 🍽️📊

📌 Overview:
This project showcases the implementation of a real-time data engineering pipeline using modern tools and technologies. The aim is to collect, store, process, and visualize data for restaurant delivery and performance analytics using cloud-native services.

🚀 Problem Statement:
The rapid growth of food delivery platforms has created a massive influx of unstructured and semi-structured data. Restaurants often lack proper infrastructure to analyze performance metrics like top-selling items, delivery efficiency, and operational effectiveness.

Our goal is to:

1). Store data in S3 with proper folder structures.
2). Load into Snowflake using SCD1, SCD2, and CDC strategies.
3). Visualize insights in Power BI.

🛠️ Tech Stack
| Tool/Tech  | Role                                               |
| ---------- | -------------------------------------------------- |
| Python     | For Data Generation     
| SQL        | For transformations and data modeling              |
| Snowflake  | Cloud data warehouse for storing and querying data |
| AWS S3     | Data lake to store raw and processed files         |
| Power BI   | Dashboard for insights and KPIs                    |
| Git/GitHub | Version control and collaboration                  |

 📁 Folder Structure  
 Internship_project/
├── data_generation/ 
│   ├── main
     │   ├── Data Generation - https://github.com/Falsi3007/Internship_project/blob/main/data-generation/Main/Main_DG.py
     │   ├── Uploading data into s3 -https://github.com/Falsi3007/Internship_project/blob/main/data-generation/Main/s3_upload.py
     |   ├── AWS authentication credentials -https://github.com/Falsi3007/Internship_project/blob/main/data-generation/Main/.env.local
├──snowflake-code/
   ├──customer table code-https://github.com/Falsi3007/Internship_project/blob/main/snowflake-code/customer.txt
   ├──customer_address code logic-https://github.com/Falsi3007/Internship_project/blob/main/snowflake-code/customer_address.txt
   ├──delivery table code-https://github.com/Falsi3007/Internship_project/blob/main/snowflake-code/delivery.txt
   ├──delivery agent code logic-https://github.com/Falsi3007/Internship_project/blob/main/snowflake-code/delivery_agent.txt
   ├──location table code-https://github.com/Falsi3007/Internship_project/blob/main/snowflake-code/location.txt
   ├──menu table code-https://github.com/Falsi3007/Internship_project/blob/main/snowflake-code/menu.txt
   ├──orders table code -https://github.com/Falsi3007/Internship_project/blob/main/snowflake-code/order.txt
   ├──order_items table code-https://github.com/Falsi3007/Internship_project/blob/main/snowflake-code/order_item.txt
   ├──restaurant table code-https://github.com/Falsi3007/Internship_project/blob/main/snowflake-code/restaurant.txt
├──snowflake-code-v2/
   ├──sql_Scripts - creation of database,schema,file_format -https://github.com/Falsi3007/Internship_project/blob/main/snowflake-code-v2/sql_scripts/create_db_schema.sql
├──snowflake-code-v3/
   ├──procedure_Scripts/
      ├──customer_address_proc-procedure of customer_address code
      ├──customer_proc-procedure of customer table code
      ├──delivery_agent_proc-procedure of delivery_agent code
      ├──delivery_proc-procedure of delivery table code
      ├──location_proc-procedure of location table code
      ├──login_audit_proc-procedure of login_audit table proc
      ├──menu_proc-procedure of menu table code
      ├──order_item_proc-procedure of order_item code
      ├──order_proc-procedure of orders table code
      ├──restaurant_proc-procedure of restaurant table code
      ├──final_proc-procedure to call all the procedures created
├──Swiggy_Report_2025-contains the final dashboard.

 🔄 Project Workflow 
 ✅ Step 1: Generate Synthetic Data bash python data_generation/Main_DG  and store it in form of CSV format.
 🧹 Step 2: Dump the data into the s3 using the data_generation/main/s3_upload.py and authenticating using the data-generation/Main/.env.local.
 📤 Step 3: Load Data into Snowflake external stage(AWS managed) using the s3 credentials and create the database, schema, file format using snowflake-code-v2/sql_scripts/create_db_schema.sql.
 📤 Step 4: Create the respective tables and procedures using snowflake-code-v3/procedure_scripts/.
 📊 Step 4: Connect your power BI desktop with snowflake using the credentials  
 
 🔍 Features -
 🔧 Modular design for generation, transformation, and loading 
 🏗 Snowflake integration with reusable utility functions 
 📈 Ready-to-analyze datasets with realistic food delivery platform structure 
 📊 Interactive data exploration in notebooks --- ## ✅ Use Cases - Simulate large-scale data pipelines for food delivery systems - Build BI dashboards using Snowflake data - Practice advanced SQL and data modeling - Apply Slowly Changing Dimensions (SCD) or Change Data Capture (CDC) 

📊 Power BI report : https://github.com/Falsi3007/Internship_project/blob/main/Swiggy_Report_2025.pbix
Tables and Column Names 🍲

| Table           |     Column Names                                   
| ----------      | -------------------------------------------------- 
| customers       | CustomerID,Full_Name,Email,Mobile_no,LoginByUsing,Gender,DOB,Anniversary,Rating,Preferences,CreatedDate,ModifiedDate     
| Customer_Address| AddressID,CustomerID,FlatNo/HouseNo,Floor,Building,Landmark,Locality,City,State,PinCode,Coordinates,PrimaryFlag,AddressType,CreatedDate,ModifiedDate            
| Delivery        | DeliveryID,OrderID,DeliveryAgentID,DeliveryStatus,EstimatedTime,DeliveredTime,AddressID,DeliveryDate,CreatedDate,ModifiedDate
| Delivery_Agent  | DeliveryAgentID,Full_Name,email,Mobile_no,VehicleType,LocationID,Status,Gender,Rating,CreatedDate,ModifiedDate        
| Location        | LocationID,City,State,PinCode,ActiveFlag,CreatedDate,ModifiedDate                   
| Login_Audit     | LoginID,CustomerID,LoginType,DeviceInterface,MobileDeviceName,WebInterface,LastLogin
| Menu_Items      | MenuItemID,RestaurantID,ItemName,Description,Price,Category,Availability,ItemType,Ratings,CreatedDate,ModifiedDate
| Order_items     | OrderItemID,OrderID,MenuItemID,Quantity,Price,Subtotal,Ratings,CreatedDate,ModifiedDate
| Orders          | OrderID,CustomerID,RestaurantID,OrderDate,TotalAmount,DiscountAmount,DeliveryCharges,FinalAmount,Status,PaymentMethod,IsFirstOrder,CouponCode,CreatedDate,ModifiedDate
| Restaurant      | RestaurantID,Name,CuisineType,Pricing_for_2,Restaurant_Phone,OperatingHours,LocationID,ActiveFlag,OpenStatus,Locality,Restaurant_Address

KPIs:
1) Total Revenue generated- Gives total generated revenue till date.
2) AOV - Gives Average Order Value
3) Total Customers- Gives total number of customers
4) Total Cities-Gives the number of cities where swiggy is operating.
5) Top Performing City- Gives the City from where most number of order arrives.
6) Total Orders - Gives total number of orders till date
7) No. of Delivery Agents -Gives the number of delivery agent in system
8) Total Restaurant- gives the Total number of restaurant operating under swiggy
9) Avg. Restaurant Rating- gives the avg rating per restaurant.
10) Returned Amount Rate-Rate of Amount Returned till now.
11) Returned Deliveries-Total number of deliveries returned
12) Churn Rate-gives the rate of customers churned in the period of 3 months.
13) Retention Rate-gives the rate of customers retained
14) Different Payment Method Rate-Gives Payment Method rate for orders
15) Most Valuable Customer-gives name of customer who ordered the most
16) Revenue Growth(%)-gives the % growth rate with respect to each financial year
17) Order Cancellation Rate - gives the rate of orders cancelled
18) Revenue per restaurant - gives total revenue generated per different restaurant
19) Revenue per state: gives the revenue generated per different state
20) Total Revenue per Order Item - gives the revenue generated per different order item
21) Avg. No. of successful deliveries by agent - gives the avg number of deliveries which are successful
22) Avg Delivery Time - gives the average of delivery time taken
23) Average customer waiting time - gives the average of customer waiting time
24) Avg delivery Partner rating - guives the average rating of a delivery partner
25) No. Of deliveries per different hours of a day: gives no. of deliveries done during different hours of a day
26) Delivery status rate - shows the portion of deliveries successful, returned and failed to deliver

   
   


