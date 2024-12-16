dtypes_order_details =  {
    'reduction_cd': 'object',
    'order_id': 'object',
    'original_order_id': 'object',
    'scheduled_local_dtm': 'object',
    'united_account_id': 'object',
    'employer_id': 'object',
    'employer_name': 'object',
    'identity_provider_id': 'object',
    'cust_prof_key': 'object',
    'cust_reduction_cd': 'object',
    'employee_role_name': 'object',
    'cust_active_fl': 'object',
    'cust_prof_id': 'object',
    'cust_emply_verified_fl': 'object',
    'cust_modified_time': 'object',
    'active_fl': 'object',
    'company_id': 'object',
    'default_email_address': 'object',
    'fee_amt': 'float64',
    'order_status_cd': 'object',
    'cust_create_time': 'object',
    'cust_first_name': 'object',
    'cust_last_name': 'object',
    'activity_status_code': 'object'
    }

dtypes_all = {
    # 'bus_date':'datetime64[ns]',
    'pos_venue_id':'object',
    'order_id':'object',
    'transaction_id':'string',
    'device_order_id':'object',
    'time_zone':'object',
    # 'order_local_time':'datetime64[ns]',
    'sales_hr':'Int64',
    # 'ord_beg_time':'datetime64[ns]',
    # 'ord_close_time':'datetime64[ns]',
    'item_count':'Int64',
    'gross_total':'Float64',
    'net_total':'Float64',
    'taxes':'Float64',
    'exclusive_tax':'Int8',
    'inclusive_tax':'Int8',
    'reduction_amt':'Float64',
    'cust_prof_id':'string',
    'mobile_ord_fl':'Int64',
    'original_order_id':'string',
    'vendor_loc_id':'string',
    'fee_amt':'Float64',
    'tip_amount':'Float64',
    'sync_status':'string',
    'src_sys_id':'object',
    'vendor_id':'object',
    'menu_vendor_id':'object',
    'refund_vendor_id':'string',
    'refund_venue_id':'string',
    'pos_terminal_id':'string',
    'reduction_cd':'string',
    'active_fl':'object',
    'order_tab_id':'string',
    'order_status_cd':'string',
    'united_account_id':'string',
    'cust_first_name':'string',
    'cust_last_name':'string',
    # 'birth_date':'datetime64[ns]',
    'activity_status_code':'string',
    'employee_role_name':'string',
    'cust_emply_verified_fl':'string',
    'cust_emply_active_fl':'Int8',
    'employer_id':'string',
    'employer_name':'string',
    'employer_active_fl':'Int8',
    'default_email_address':'string',
    'default_phone_number':'string',
    'company_id':'string',
    'primary_email_address':'string',
    'primary_email_opt_in_fl':'Int8',
    'primary_email_active_fl':'Int8',
    'primary_email_verified_fl':'Int8',
    'extra_ph_number':'Float64',
    'card_type':'string',
    'payment_type':'string',
    'transaction_seq_nu':'Int64',
    'cash_recycler_tiny_code':'string',
    'payment_amount':'Float64',
    'account_id':'string',
    'payment_amt_rewards_points':'Float64',
    'voucher_number':'string',
    'emp_email':'string',
    'active_fl':'Int8'
}
    
date_cols = [
    "bus_date",
    "order_local_time",
    "ord_beg_time",
    "ord_close_time",
    "birth_date"
]


dtypes_payment_details = {
    "cash_recycler_tiny_code": "object",
    "emp_email": "object",
    "voucher_number": "object",
    "country_id": "object",
    "pos_venue_id": "object",
    "order_id": "object",
    "src_sys_id": "object",
    "transaction_id": "object",
    "vendor_id": "object",
    "account_id": "object",
}


reduction_cds = {
    "AIRPORT_DISCOUNT": "INT_AIRPORT_DISCOUNT",
    "Airport": "INT_AIRPORT_DISCOUNT",
    "AMERICAN_DISCOUNT": "INT_AMERICAN_DISCOUNT",
    "Chase": "INT_UNITED_CHASE",
    "CREW_DISCOUNT": "INT_CREW_DISCOUNT",
    "Crew": "INT_CREW_DISCOUNT",
    "DELTA_DISCOUNT": "INT_DELTA_DISCOUNT",
    "Delta": "INT_DELTA_DISCOUNT",
    "JETBLUE_DISCOUNT": "INT_JETBLUE_DISCOUNT",
    "MANAGER_DISCOUNT": "INT_MANAGER_DISCOUNT",
    "Manager Meal": "INT_MANAGER_DISCOUNT",
    "SOUTHWEST_DISCOUNT": "INT_SOUTHWEST_DISCOUNT",
    "SPIRIT_DISCOUNT": "INT_SPIRIT_DISCOUNT",
    "UNITED_CHASE": "INT_UNITED_CHASE",
    "UNITED_DISCOUNT": "INT_UNITED_DISCOUNT",
}


card_types = {
    "MC": "MASTERCARD",
    "AMEX": "AMERICAN EXPRESS",
    "DISC": "DISCOVER",
    "DISCOVER (DC)": "DISCOVER",
    "Invoice": "Invoice/ Military",
    "Military": "Invoice/ Military",
}


data_cols = ['bus_date', 'pos_venue_id', 'order_id', 'device_order_id', 'time_zone',
       'order_local_time', 'sales_hr', 'ord_beg_time', 'ord_close_time',
       'item_count', 'gross_total', 'net_total', 'taxes', 'exclusive_tax',
       'inclusive_tax', 'reduction_amt', 'cust_prof_id', 'mobile_ord_fl',
       'original_order_id', 'vendor_loc_id', 'fee_amt', 'tip_amount',
       'sync_status', 'src_sys_id', 'vendor_id', 'menu_vendor_id',
       'refund_vendor_id', 'refund_venue_id', 'pos_terminal_id',
       'reduction_cd', 'active_fl', 'order_tab_id', 'order_status_cd',
       'united_account_id', 'card_type', 'payment_type', 'transaction_id',
       'transaction_seq_nu', 'cash_recycler_tiny_code', 'payment_amount',
       'account_id', 'payment_amt_rewards_points', 'voucher_number',
       'emp_email', 'cust_first_name', 'cust_last_name', 'birth_date',
       'activity_status_code', 'employee_role_id', 'employee_role_name',
       'cust_emply_verified_fl', 'cust_emply_active_fl', 'employer_id',
       'employer_name', 'employer_active_fl', 'default_email_address',
       'default_phone_number', 'company_id', 'primary_email_address',
       'primary_email_opt_in_fl', 'primary_email_active_fl',
       'primary_email_verified_fl', 'extra_ph_number',
       'is_fraud']
