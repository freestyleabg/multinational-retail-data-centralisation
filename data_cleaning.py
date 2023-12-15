import pandas as pd
from datetime import datetime
import numpy as np


class DataCleaning:
    def __init__(self) -> None:
        pass

    @staticmethod
    def clean_unknown_string(df):
        mask = r"^[A-Z0-9]{10}$"
        for column in df.columns:
            df[column] = df[column][
                ~df[column].astype(str).str.contains(mask, na=False)
            ]

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
        if index_col is not None:
            df.set_index(index_col, inplace=True, drop=True)
            df.reset_index(drop=True, inplace=True)
            df.index = df.index + 1
        else:
            return df

    def clean_user_data(self, df, index_col="index"):
        self.clean_dates(df)
        self.clean_unknown_string(df)
        self.clean_address(df)
        self.reset_index_col(df, index_col=index_col)
        df.loc[df["country_code"] == "GGB"] = "GB"

    def clean_card_data(self, df):
        self.clean_unknown_string(df)
        for column in df.columns:
           mask = df[column] == column
           df.loc[mask, column] = np.nan
        df.dropna(inplace=True)
        # df["expiry_date"] = pd.to_datetime(
        #     df["expiry_date"], errors="raise", format="%m/%y"
        # )
        # df["expiry_date"] = df["expiry_date"].dt.strftime("%m/%y")
        # df["date_payment_confirmed"] = pd.to_datetime(
        #     df["date_payment_confirmed"], errors="raise"
        # )

    def clean_store_data(self, df, index_col="index"):
        self.clean_unknown_string(df)
        self.clean_address(df)
        self.clean_dates(df)
        mask = df["store_type"] == "Web Portal"
        df.loc[mask, "longitude"] = np.nan
        df.loc[mask, "latitude"] = np.nan
        df.loc[mask, "locality"] = np.nan
        df.loc[mask, "address"] = "N/A"
        df.loc[mask, "locality"] = "N/A"
        df["staff_numbers"] = df["staff_numbers"].str.extract(r"([0-9]{1,2})")
        self.reset_index_col(df, index_col=index_col)

    def convert_product_weights(self, df):
        unit_factors = {"g": 0.001, "ml": 0.001, "oz": 0.028349523125, "kg": 1}
        df.dropna(subset="weight", inplace=True)
        df["weight"] = df["weight"].str.replace(" ", "")
        df["weight"] = df["weight"].str.replace("x", "*")
        df["weight_unit"] = df["weight"].str.extract(r"([a-zA-Z]+)")
        df["weight"] = df["weight"].str.extract(
            r"([0-9]+\.[0-9]+|[0-9]+\*[0-9]+|[0-9]+)"
        )
        mask = df["weight"].str.contains("\*")
        df.loc[mask, "weight"] = df.loc[mask, "weight"].apply(lambda x: eval(x))
        df["weight"] = df["weight"].astype(float)
        df["weight"] = df["weight_unit"].map(unit_factors) * df["weight"]
        df.drop(columns="weight_unit", inplace=True)

    def clean_products_data(self, df):
        self.clean_dates(df)
        df["removed"] = df["removed"] == "Removed"
        df["category"] = df["category"].astype("category")
        df["weight"] = df["weight"].round(3)
        df["product_price"] = df["product_price"].str.replace("£", "")

    def clean_orders_data(self, df):
        df.drop(columns=["first_name", "last_name", "1", "level_0"], inplace=True)

    def clean_date_data(self, df):
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
        # df.drop(columns=["year", "month", "day", "timestamp"], inplace=True)
        df.drop(columns=["timestamp"], inplace=True)
        df["time_period"] = df["time_period"].astype("category")
