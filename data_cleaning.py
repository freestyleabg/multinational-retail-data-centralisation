from ast import literal_eval
from datetime import datetime

import numpy as np
import pandas as pd


class DataCleaning:
    """
    This class provides methods for cleaning various types of data in pandas DataFrames.
    It includes functions to clean strings, dates, addresses, and more, ensuring data integrity
    and consistency across different datasets.
    """

    def __init__(self) -> None:
        """
        Initializes an instance of the DataCleaning class.
        """
        pass

    def clean_unknown_string(self, df):
        """
        Removes rows in the DataFrame where any column matches a specific regex pattern.

        Parameters:
        df (DataFrame): The DataFrame to be cleaned.
        """
        mask = r"^[A-Z0-9]{10}$"
        for column in df.columns:
            df[column] = df[column][
                ~df[column].astype(str).str.contains(mask, na=False)
            ]

    def clean_dates(self, df):
        """
        Converts string dates to datetime objects and drops rows with invalid dates.

        Parameters:
        df (DataFrame): The DataFrame to be cleaned.
        """
        for column in df.columns:
            if "date" in column:
                df[column] = pd.to_datetime(df[column], errors="coerce", format="mixed")
                df.dropna(subset=[column], inplace=True)

    def clean_address(self, df):
        """
        Cleans and formats address data in the DataFrame.

        Parameters:
        df (DataFrame): The DataFrame to be cleaned.
        """
        df.dropna(subset=["address"], inplace=True)

        def clean_single_address(address):
            return address.replace("\n", ", ") if "\n" in address else address

        df["address"] = df["address"].apply(clean_single_address)

    def reset_index_col(self, df, index_col):
        """
        Resets the index of the DataFrame and sets a new index column if specified.

        Parameters:
        df (DataFrame): The DataFrame whose index is to be reset.
        index_col (str): The column to set as the new index.
        """
        if index_col is not None:
            df.set_index(index_col, inplace=True, drop=True)
            df.reset_index(drop=True, inplace=True)
            df.index = df.index + 1
        else:
            return df

    def clean_user_data(self, df, index_col="index"):
        """
        Cleans user data by applying various cleaning functions.

        Parameters:
        df (DataFrame): The DataFrame containing user data to be cleaned.
        index_col (str): The column to set as the new index.
        """
        self.clean_dates(df)
        self.clean_unknown_string(df)
        self.clean_address(df)
        self.reset_index_col(df, index_col=index_col)
        df.loc[df["country_code"] == "GGB", "country_code"] = "GB"

    def clean_card_data(self, df):
        """
        Cleans credit card data by removing NaN values and applying string cleaning.

        Parameters:
        df (DataFrame): The DataFrame containing card data to be cleaned.
        """
        df.dropna(inplace=True)
        self.clean_unknown_string(df)
        for column in df.columns:
            mask = df[column] == column
            df.loc[mask, column] = np.nan
        df.dropna(inplace=True)
        df.loc[:, "card_number"] = df.loc[:, "card_number"].str.replace(
            r"[\?]+", "", regex=True
        )

    def clean_store_data(self, df, index_col="index"):
        """
        Cleans store data, adjusting fields specific to different types of stores.

        Parameters:
        df (DataFrame): The DataFrame containing store data to be cleaned.
        index_col (str): The column to set as the new index.
        """
        self.clean_unknown_string(df)
        self.clean_address(df)
        self.clean_dates(df)
        mask = df["store_type"] == "Web Portal"
        df.loc[mask, "continent"] = "N/A"
        df.loc[mask, "longitude"] = np.nan
        df.loc[mask, "latitude"] = np.nan
        df.loc[mask, "address"] = "N/A"
        df.loc[:, "staff_numbers"] = df["staff_numbers"].str.replace(
            r"[A-Za-z]+", "", regex=True
        )
        df.loc[:, "continent"] = df["continent"].str.replace(r"^[a-z]+", "", regex=True)
        self.reset_index_col(df, index_col=index_col)

    def convert_product_weights(self, df):
        """
        Converts product weight to a uniform unit of measurement.

        Parameters:
        df (DataFrame): The DataFrame containing product weight data to be converted.
        """
        unit_factors = {"g": 0.001, "ml": 0.001, "oz": 0.028349523125, "kg": 1}
        df.dropna(subset=["weight"], inplace=True)
        df["weight"] = df["weight"].str.replace(" ", "")
        df["weight"] = df["weight"].str.replace("x", "*")
        df["weight_unit"] = df["weight"].str.extract(r"([a-zA-Z]+)")
        df["weight"] = df["weight"].str.extract(
            r"([0-9]+\.[0-9]+|[0-9]+\*[0-9]+|[0-9]+)"
        )
        mask = df["weight"].str.contains(r"\*")
        df.loc[mask, "weight"] = df.loc[mask, "weight"].apply(lambda x: literal_eval(x))
        df["weight"] = df["weight"].astype(float)
        df["weight"] = df["weight_unit"].map(unit_factors) * df["weight"]
        df.drop(columns=["weight_unit"], inplace=True)

    def clean_products_data(self, df):
        """
        Cleans products data, including categories and pricing.

        Parameters:
        df (DataFrame): The DataFrame containing products data to be cleaned.
        """
        self.clean_dates(df)
        df["removed"] = df["removed"] == "Removed"
        df["category"] = df["category"].astype("category")
        df["weight"] = df["weight"].round(3)
        df["product_price"] = df["product_price"].str.replace("£", "")

    def clean_orders_data(self, df):
        """
        Cleans orders data by dropping unnecessary columns.

        Parameters:
        df (DataFrame): The DataFrame containing orders data to be cleaned.
        """
        df.drop(columns=["first_name", "last_name", "1", "level_0"], inplace=True)

    def clean_date_data(self, df):
        """
        Cleans and standardizes date and time data in the DataFrame.

        Parameters:
        df (DataFrame): The DataFrame containing date and time data to be cleaned.
        """
        self.clean_unknown_string(df)
        df.replace("NULL", np.nan, inplace=True)
        df.dropna(inplace=True)
        df["datetime"] = df.apply(
            lambda row: datetime.strptime(
                f"{row['year']}-{row['month']}-{row['day']} {row['timestamp']}",
                "%Y-%m-%d %H:%M:%S",
            ),
            axis=1,
        )
        df.drop(columns=["timestamp"], inplace=True)
        df["time_period"] = df["time_period"].astype("category")
