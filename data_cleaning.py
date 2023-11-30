import pandas as pd
#from dateutil.parser import parse


class DataCleaning:
    def __init__(self) -> None:
        pass

    @staticmethod
    def clean_user_data(df, index_col=None):
        def clean_dates(df):
            for column in df.columns:
                if 'date' in column:
                    df[column] = pd.to_datetime(df[column], errors='coerce', format='mixed')
                    df.dropna(subset=column, inplace=True)

        def clean_unkown_string(df):
            mask = r'^[A-Z0-9]{10}$'
            for column in df.columns:
                df[column] = df[column][~df[column].astype(str).str.contains(mask, na=False)]

        def clean_address(df):
            def clean_single_address(address):
                return address.replace('\n', ', ') if '\n' in address else address
            df['address'] = df['address'].apply(clean_single_address) 
        
        def reset_index_col(df, index_col):
            if index_col != None:
                df.set_index(index_col, inplace=True, drop=True)
                df.reset_index(drop=True, inplace=True)
                df.index = df.index + 1
            else:
                return df   
        
        clean_dates(df)
        clean_unkown_string(df)
        df.dropna(subset='address', inplace=True)
        clean_address(df)
        reset_index_col(df, index_col)
    
    @staticmethod
    def clean_card_data(df):
        df['expiry_date'] = pd.to_datetime(df['expiry_date'], errors='coerce', format='%m/%y')
        df['expiry_date'] = df['expiry_date'].dt.strftime('%m/%y')
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')
        df.dropna(inplace=True)
        


        



