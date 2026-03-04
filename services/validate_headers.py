import os
import re
import json
import pandas as pd
import sqlalchemy
from openai import OpenAI
from .db import get_engine, get_raw_connection

EXPECTED_COLUMNS = [
    "Sales Order No",               # 0
    "Item No",                       # 1
    "Material No",                   # 2
    "Material Name",                 # 3
    "Sales org code",                # 4
    "Distribution channel code",     # 5
    "Distribution channel name",     # 6
    "payment term code",             # 7
    "payment term name",             # 8
    "sold to no",                    # 9
    "sold to name",                  # 10
    "ship to no",                    # 11
    "ship to name",                  # 12
    "Shipping label",                # 13
    "Sales employee no",             # 14
    "Sales employee name",           # 15
    "PO number",                     # 16
    "Plant",                         # 17
    "Sloc",                          # 18
    "SO created date",               # 19
    "SO date",                       # 20
    "PO date",                       # 21
    "request delivery date",         # 22
    "Order Quantity",                # 23
    "Pending Quantity",              # 24
    "Sales Unit",                    # 25
    "Conv.of Unit(EA/PC)",           # 26
    "Conv.of Unit(PAC)",             # 27
    "Conv.of Unit(KG)",              # 28
    "Conv.of Unit(RM)",              # 29
    "Conv.of Unit(BOX)",             # 30
    "Conv.of Unit(TO)",              # 31
    "Conv.of Unit(PAL)",             # 32
    "Condition PR00",                # 33
    "Cond Type Desc",                # 34
    "Amount",                        # 35
    "Rate Unit(Curr,SU,%)",          # 36
    "Per",                           # 37
    "UoM",                           # 38
    "Condition Value",               # 39
    "Currency",                      # 40
]

DISCOUNT_COLUMN_WIDTH = 8
DISCOUNT_START_INDEX = 41
DISCOUNT_PATTERN = re.compile(r"^(.+?)\s*\(by\s+(item|order)\)$", re.IGNORECASE)


def _call_openai_validate(expected: list[str], actual: list[str]) -> dict:
    """Use GPT-5-mini to compare expected vs actual column names."""
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    pairs = []
    for i, (exp, act) in enumerate(zip(expected, actual)):
        pairs.append({"index": i, "expected": exp, "actual": str(act) if act else ""})

    response = client.chat.completions.create(
        model="gpt-5-mini",
        response_format={"type": "json_object"},
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a data column validator. Compare SAP Excel column headers. "
                    "For each pair, determine if the actual name is similar enough to the "
                    "expected name (minor differences in casing, spacing, abbreviation, or "
                    "wording are acceptable). Return JSON with key 'results' as an array of "
                    "objects: {index, expected, actual, match (bool), reason (string, only if not match)}."
                ),
            },
            {
                "role": "user",
                "content": json.dumps({"columns": pairs}),
            },
        ],
        temperature=1,
    )

    return json.loads(response.choices[0].message.content)


def _detect_discounts(headers: list) -> list[dict]:
    """Detect discount columns starting from index 41, every 8 columns."""
    discounts = []
    idx = DISCOUNT_START_INDEX
    while idx < len(headers):
        header_val = headers[idx]
        if header_val is None:
            break
        m = DISCOUNT_PATTERN.match(str(header_val).strip())
        if not m:
            break
        discounts.append({
            "sap_discount": m.group(1).strip(),
            "discount_type": f"by {m.group(2).lower()}",
            "column_index": idx,
        })
        idx += DISCOUNT_COLUMN_WIDTH
    return discounts


def _save_discounts_to_db(discounts: list[dict]):
    """Save detected discounts to discount_master table."""
    if not discounts:
        return

    engine = get_engine()

    conn = get_raw_connection()
    cursor = conn.cursor()
    db_name = engine.url.database
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    conn.close()

    with engine.connect() as con:
        table_exists = engine.dialect.has_table(con, "discount_master")
        if table_exists:
            con.execute(sqlalchemy.text("TRUNCATE TABLE discount_master"))
            con.commit()

    df = pd.DataFrame([
        {
            "sap_discount": d["sap_discount"],
            "internal_id": "",
            "discount_name": "",
            "discount_type": d["discount_type"],
            "column_index": d["column_index"],
        }
        for d in discounts
    ])

    if_exists = "append" if table_exists else "replace"
    df.to_sql(name="discount_master", con=engine, if_exists=if_exists, index=False)

    if not table_exists:
        with engine.connect() as con:
            con.execute(sqlalchemy.text(
                "ALTER TABLE discount_master "
                "MODIFY sap_discount VARCHAR(10), "
                "MODIFY internal_id VARCHAR(10), "
                "MODIFY discount_name VARCHAR(30), "
                "MODIFY discount_type VARCHAR(30), "
                "MODIFY column_index INT, "
                "ADD PRIMARY KEY (sap_discount);"
            ))
            con.commit()


def validate_sap_headers(excel_path: str) -> dict:
    """Validate SAP report headers and detect discount columns.

    Returns dict with status, columns_valid, column_errors, discounts.
    """
    try:
        sap_df = pd.read_excel(excel_path, sheet_name="SAP report", nrows=0)
    except Exception as e:
        return {
            "status": "error",
            "message": f"Cannot read 'SAP report' sheet: {e}",
            "columns_valid": False,
            "column_errors": [],
            "discounts": [],
        }

    headers = list(sap_df.columns)

    if len(headers) < len(EXPECTED_COLUMNS):
        return {
            "status": "error",
            "message": f"Expected at least {len(EXPECTED_COLUMNS)} columns (A-AO), found {len(headers)}.",
            "columns_valid": False,
            "column_errors": [{"index": i, "expected": EXPECTED_COLUMNS[i], "actual": ""} for i in range(len(headers), len(EXPECTED_COLUMNS))],
            "discounts": [],
        }

    actual_headers = [str(h) if h is not None else "" for h in headers[:len(EXPECTED_COLUMNS)]]

    try:
        openai_result = _call_openai_validate(EXPECTED_COLUMNS, actual_headers)
    except Exception as e:
        return {
            "status": "error",
            "message": f"OpenAI validation failed: {e}",
            "columns_valid": False,
            "column_errors": [],
            "discounts": [],
        }

    column_errors = []
    results = openai_result.get("results", [])
    for r in results:
        if not r.get("match", True):
            column_errors.append({
                "index": r["index"],
                "expected": r["expected"],
                "actual": r["actual"],
                "reason": r.get("reason", ""),
            })

    columns_valid = len(column_errors) == 0

    discounts = _detect_discounts(headers)

    try:
        _save_discounts_to_db(discounts)
    except Exception as e:
        return {
            "status": "error",
            "message": f"Failed to save discounts: {e}",
            "columns_valid": columns_valid,
            "column_errors": column_errors,
            "discounts": discounts,
        }

    return {
        "status": "success" if columns_valid else "error",
        "message": "All columns validated successfully." if columns_valid else "Some columns do not match expected names.",
        "columns_valid": columns_valid,
        "column_errors": column_errors,
        "discounts": discounts,
    }
