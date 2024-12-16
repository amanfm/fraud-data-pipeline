import pandas as pd

class DataTransformer:
    def __init__(self):
        pass

    @staticmethod
    def concat_without_dups(keep: pd.DataFrame, drop: pd.DataFrame) -> pd.DataFrame:
        return pd.concat([keep, drop[~drop["order_id"].isin(keep["order_id"])]])
    
    @staticmethod
    def drop_null_columns(df: pd.DataFrame) -> pd.DataFrame:
        nulls = df.isna().all(axis="index")
        return df.drop(nulls[nulls].index, axis="columns")
    
    @staticmethod
    def drop_time_columns(df: pd.DataFrame, keep_cols=None) -> pd.DataFrame:
        return df.drop(
            df.columns[
                ~df.columns.isin(keep_cols or ["time_zone", "order_local_time"]) &
                df.columns.str.contains(r"time|_ts|_dt")
            ],
            axis="columns",
        )
    
    @staticmethod
    def drop_id_columns(df: pd.DataFrame, keep_cols=None) -> pd.DataFrame:
        return df.drop(
            df.columns[
                ~df.columns.isin(keep_cols or ["order_id", "device_order_id"]) &
                df.columns.str.contains(r"_u{,2}id")
            ],
            axis="columns",
        )
    
    @staticmethod
    def process_order_details(order_details: pd.DataFrame) -> pd.DataFrame:
        # Remove "1" prefix from `order_id` and `original_order_id`
        order_details["order_id"] = order_details["order_id"].str.removeprefix("1")
        order_details["original_order_id"] = order_details["original_order_id"].str.removeprefix("1")

        # Remove "01" suffix from `order_id`s with length 10
        cond = order_details["order_id"].str.len().eq(10)
        ten_len_ords = order_details[cond]
        ten_len_ords["order_id"] = ten_len_ords["order_id"].str.removesuffix("01")

        # Concatenate without duplicates
        order_details = DataTransformer.concat_without_dups(order_details[~cond], ten_len_ords)

        # Remove "14" prefix from `cust_prof_id`
        order_details["cust_prof_id"] = order_details["cust_prof_id"].str.removeprefix("14")

        return order_details
    
    @staticmethod
    def process_payment_details(payment_details: pd.DataFrame) -> pd.DataFrame:
        # Example transformation for payment details
        payment_details["order_id"] = payment_details["order_id"].str.removeprefix("1")
        cond = payment_details["order_id"].str.len().eq(10)
        ten_len_ords = payment_details[cond]
        ten_len_ords["order_id"] = ten_len_ords["order_id"].str.removesuffix("01")
        return DataTransformer.concat_without_dups(payment_details[~cond], ten_len_ords)

    @staticmethod
    def process_acm_details(acm: pd.DataFrame, reduction_cds: dict, card_types: dict) -> pd.DataFrame:
        # Replace values in the acm dataframe
        acm["reduction_cd"] = acm["reduction_cd"].replace(to_replace=reduction_cds)
        acm["card_type"] = acm["card_type"].replace(to_replace=card_types)
        return acm


