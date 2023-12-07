import pandas as pd


class DataCleaning:
    """
    A class that provides methods for cleaning and transforming data in a pandas DataFrame.

    Methods:
    - clean_unknown_string(df): Cleans unknown strings in a DataFrame by removing rows that do not match a specified pattern.
    - clean_dates(df): Cleans dates in a DataFrame by converting date columns to datetime format and dropping rows with missing values.
    - clean_address(df): Cleans addresses in a DataFrame by replacing newline characters with commas.
    - reset_index_col(df, index_col): Resets the index column of a DataFrame and optionally sets a new index column.
    - clean_user_data(df, index_col='index'): Cleans user data in a DataFrame by applying the `clean_dates`, `clean_unknown_string`, `clean_address`, and `reset_index_col` methods.
    - clean_card_data(df): Cleans card data in a DataFrame by converting the 'expiry_date' column to datetime format and dropping rows with missing values.
    - clean_store_data(df, index_col='index'): Cleans store data in a DataFrame by applying the `clean_unknown_string`, `clean_address`, `clean_dates`, and `reset_index_col` methods, and reordering the columns.
    """

    def __init__(self) -> None:
        pass

    def clean_unknown_string(self, df):

        mask = r"^[A-Z0-9]{10}$"
        for column in df.columns:
            df[column] = df[column][~df[column].astype(str).str.contains(mask, na=False)]
    
    def clean_dates(self, df):

        for column in df.columns:
            if "date" in column:
                df[column] = pd.to_datetime(df[column], errors="coerce", format="mixed")
                df.dropna(subset=column, inplace=True)

    def clean_address(self, df):

        df.dropna(subset="address", inplace=True)

        def clean_single_address(address):
            return address.replace("\n", ", ") if "\n" in address else address

        df["address"] = df["address"].apply(clean_single_address)

    def reset_index_col(self, df, index_col):

        if index_col != None:
            df.set_index(index_col, inplace=True, drop=True)
            df.reset_index(drop=True, inplace=True)
            df.index = df.index + 1
        else:
            return df

    def clean_user_data(self, df, index_col='index'):
      
        self.clean_dates(df)
        self.clean_unknown_string(df)
        self.clean_address(df)
        self.reset_index_col(df, index_col=index_col)

    @staticmethod
    def clean_card_data(df):
      
        df['expiry_date'] = pd.to_datetime(df['expiry_date'], errors='coerce', format='%m/%y')
        df['expiry_date'] = df['expiry_date'].dt.strftime('%m/%y')
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')
        df.dropna(inplace=True)

    def clean_store_data(self, df, index_col='index'):
       
        self.clean_unknown_string(df)
        self.clean_address(df)
        self.clean_dates(df)
        self.reset_index_col(df, index_col=index_col)
        self.df = df.reindex(
            columns=[
                "index",
                "store_code",
                "store_type",
                "staff_numbers",
                "address",
                "longitude",
                "latitude",
                "locality",
                "country_code",
                "continent",
                "opening_date",
            ]
        )

    def convert_product_weights(self, df):
        pass