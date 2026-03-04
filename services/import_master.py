import pandas as pd
import sqlalchemy
from .db import get_engine, get_raw_connection

MASTER_CONFIGS = {
    "customer": {
        "sheet": "Customer master",
        "table": "customer_master",
        "columns": {
            "Internal ID": "internal_id",
            "Code": "code",
            "External ID": "external_id",
            "Old Code": "old_code",
            "Customer Name": "customer_name",
        },
        "pk": "internal_id",
        "dedup": None,
    },
    "item": {
        "sheet": "item master",
        "table": "item_master",
        "columns": {
            "Internal ID": "internal_id",
            "Item Code": "item_code",
            "Item Name": "item_name",
            "Primary Units Type": "primary_units_type",
            "Subsidiary": "subsidiary",
            "Item Group": "item_group",
            "Old Item Code": "old_item_code",
        },
        "pk": "internal_id",
        "dedup": "internal_id",
    },
    "location": {
        "sheet": "location master",
        "table": "location_master",
        "columns": {
            "Plant": "plant",
            "Location ID": "location_id",
            "Location name": "location_name",
        },
        "pk": "location_id",
        "dedup": "location_id",
    },
    "payment_term": {
        "sheet": "payment term master",
        "table": "payment_term_master",
        "columns": {
            "Internal ID": "internal_id",
            "Name": "name",
            "Description": "description",
            "Days Till Net Due": "days_till_net_due",
            "Validate Credit Limit Usage": "validate_credit_limit_usage",
            "Plan % Advance/Deposit": "plan_percent_advance_deposit",
            "Domestic/Export": "domestic_export",
            "Payment Term Code": "payment_term_code",
        },
        "pk": "internal_id",
        "dedup": "internal_id",
    },
    "sales_rep": {
        "sheet": "Sales Rep master",
        "table": "sales_rep_master",
        "columns": {
            "Internal ID": "internal_id",
            "External ID": "external_id",
            "Employee ID": "employee_id",
            "Image": "image",
            "Name": "name",
            "First Name (TH)": "first_name_th",
            "Last Name (TH)": "last_name_th",
        },
        "pk": "internal_id",
        "dedup": "internal_id",
    },
    "shipping_label": {
        "sheet": "shipping label master",
        "table": "shipping_label_master",
        "columns": {
            "Address Internal ID": "address_internal_id",
            "Default Shipping Address": "default_shipping_address",
            "Default Billing Address": "default_billing_address",
            "Address Label": "address_label",
            "Ship to Code": "ship_to_code",
            "Ship to Name": "ship_to_name",
        },
        "pk": "address_internal_id",
        "dedup": "address_internal_id",
    },
    "sales_dist": {
        "sheet": "Sale&Dist master",
        "table": "sales_dist_master",
        "columns": {
            "Internal ID": "internal_id",
            "Distribution Channel": "distribution_channel",
            "Domestic/Export": "domestic_export",
            'filter by "Subsidiary"': "subsidiary",
            "Sale channel ID": "sale_channel_id",
            'filter by "Sale Channel"': "sale_channel",
            "Tax Branch": "tax_branch",
            "Tax Code": "tax_code",
        },
        "pk": "internal_id",
        "dedup": "internal_id",
    },
}


def import_master_data(excel_path: str, master_key: str) -> dict:
    """Import master data from an Excel sheet into MySQL.

    Returns a dict with 'status', 'message', and 'rows' count.
    """
    if master_key not in MASTER_CONFIGS:
        return {"status": "error", "message": f"Unknown master type: {master_key}"}

    cfg = MASTER_CONFIGS[master_key]
    engine = get_engine()

    df = pd.read_excel(excel_path, sheet_name=cfg["sheet"])
    df = df.rename(columns=cfg["columns"])

    if cfg["dedup"]:
        before = len(df)
        df = df.drop_duplicates(subset=[cfg["dedup"]])
        dropped = before - len(df)
    else:
        dropped = 0

    # Ensure database exists
    conn = get_raw_connection()
    cursor = conn.cursor()
    db_name = engine.url.database
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    conn.close()

    with engine.connect() as con:
        table_exists = engine.dialect.has_table(con, cfg["table"])
        if table_exists:
            con.execute(sqlalchemy.text(f"TRUNCATE TABLE {cfg['table']}"))
            con.commit()

    if_exists_action = "append" if table_exists else "replace"
    df.to_sql(name=cfg["table"], con=engine, if_exists=if_exists_action, index=False)

    if not table_exists:
        with engine.connect() as con:
            con.execute(sqlalchemy.text(
                f"ALTER TABLE {cfg['table']} ADD PRIMARY KEY ({cfg['pk']});"
            ))
            con.commit()

    msg = f"Imported {len(df)} rows into '{cfg['table']}'"
    if dropped:
        msg += f" ({dropped} duplicates removed)"
    return {"status": "success", "message": msg, "rows": len(df)}
