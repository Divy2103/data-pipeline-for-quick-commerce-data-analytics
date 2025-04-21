create or replace procedure swiggy_db.common.FINAL_PROCEDURE(stage_name string)
returns string
LANGUAGE SQL
as
$$
DECLARE 
    location_csv string;
    restaurant_csv string;
    menu_items_csv string;
    orders_csv string;
    order_items_csv string;
    delivery_csv string;
    delivery_agent_csv string;
    customer_csv string;
    customer_address_csv string;
    login_audit_csv string;
BEGIN

    location_csv := stage_name || 'location.csv';
    restaurant_csv := stage_name || 'restaurant.csv';
    menu_items_csv := stage_name || 'menu_items.csv';
    orders_csv := stage_name || 'orders.csv';
    order_items_csv := stage_name || 'order_items.csv';
    delivery_csv := stage_name || 'delivery.csv';
    delivery_agent_csv := stage_name || 'delivery_agent.csv';
    customer_csv := stage_name || 'customer.csv';
    customer_address_csv := stage_name || 'customer_address.csv';
    login_audit_csv := stage_name || 'login_audit.csv';
    
    EXECUTE IMMEDIATE 'CALL SWIGGY_DB.COMMON.LOCATION_MAIN_PROCEDURE('|| location_csv ||')';
    EXECUTE IMMEDIATE 'CALL SWIGGY_DB.COMMON.RESTAURANT_MAIN_PROCEDURE('|| restaurant_csv ||')';
    EXECUTE IMMEDIATE 'CALL SWIGGY_DB.COMMON.MENU_MAIN_PROCEDURE('|| menu_items_csv ||')';
    EXECUTE IMMEDIATE 'CALL SWIGGY_DB.COMMON.ORDERS_MAIN_PROCEDURE('|| orders_csv ||')';
    EXECUTE IMMEDIATE 'CALL SWIGGY_DB.COMMON.ORDER_ITEM_MAIN_PROCEDURE('|| order_items_csv ||')';
    EXECUTE IMMEDIATE 'CALL SWIGGY_DB.COMMON.DELIVERY_MAIN_PROCEDURE('|| delivery_csv ||')';
    EXECUTE IMMEDIATE 'CALL SWIGGY_DB.COMMON.DELIVERY_AGENT_MAIN_PROCEDURE('|| delivery_agent_csv ||')';
    EXECUTE IMMEDIATE 'CALL SWIGGY_DB.COMMON.CUSTOMER_MAIN_PROCEDURE('|| customer_csv ||')';
    EXECUTE IMMEDIATE 'CALL SWIGGY_DB.COMMON.CUSTOMER_ADDRESS_MAIN_PROCEDURE('|| customer_address_csv ||')';
    EXECUTE IMMEDIATE 'CALL SWIGGY_DB.COMMON.LOGIN_AUDIT_MAIN_PROCEDURE('|| login_audit_csv ||')';

    RETURN 'ALL PROCEDURES EXECUTED';
END;
$$;

CALL SWIGGY_DB.COMMON.FINAL_PROCEDURE('@STAGE_SCH.AWS_S3_STAGE/2025/4/17/');


select * from consumption_sch.customer_dim;
select * from consumption_sch.customer_address_dim;
select * from consumption_sch.location_dim;
select * from consumption_sch.delivery_dim;
select * from consumption_sch.menu_dim;
select * from consumption_sch.order_item_fact;
select * from consumption_sch.orders_fact;
select * from consumption_sch.restaurant_dim;
select * from consumption_sch.delivery_agent_dim;
select * from consumption_sch.login_audit_fact;