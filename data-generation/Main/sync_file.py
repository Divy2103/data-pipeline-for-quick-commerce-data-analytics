import pandas as pd

menu_file_1 = 'data1/menu_items.csv'
menu_file_2 = 'data2/menu_items.csv'


def sync_updated_menu_prices(main_csv_path, updated_csv_path):

    main_menu_df = pd.read_csv(main_csv_path)
    updated_menu_df = pd.read_csv(updated_csv_path)
    
    main_menu_df.set_index('MenuItemID', inplace=True)
    updated_menu_df.set_index('MenuItemID', inplace=True)
    
    main_menu_df.update(updated_menu_df[['Price']])
    
    main_menu_df.reset_index(inplace=True)
    
    main_menu_df.to_csv(main_csv_path, index=False)
    print(f"Prices and other updates synced from {updated_csv_path} to {main_csv_path}.")

sync_updated_menu_prices(menu_file_1, menu_file_2)