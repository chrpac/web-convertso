import pandas as pd
import numpy as np
from rapidfuzz import fuzz, process
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
from .db import get_engine


COLUMN_WIDTHS = {
    'A': 8,      # Internal Running No
    'B': 20,     # Sales Order No
    'C': 20,     # External ID
	'D': 18,
	'E': 30,
	'F': 18,
	'G': 30,
	'H': 18,
	'I': 30,
    'BA': 25,
    'BC': 20
}


def parse_sap_number(val):
    """Parse SAP number format e.g. '  0.560000000-' -> -0.56"""
    if pd.isna(val):
        return np.nan
    s = str(val).strip()
    if s == '':
        return np.nan
    if s.endswith('-'):
        s = '-' + s[:-1]
    try:
        return float(s)
    except ValueError:
        return np.nan


OUTPUT_COLUMNS = [
    'Product',                                          # A
    'Internal Running No',                              # B
    'External ID of Sales Order',                       # C
    'Internal ID of Customer',                          # D
    'Customer',                                         # E
    'Internal ID of Ship to Customer',                  # F
    'Ship to Customer',                                 # G
    'Internal ID of Shipping Label New',                # H
    'Shipping Label',                                   # I
    'Date',                                             # J
    'Domestic/Export',                                   # K
    'Internal ID of Subsidiary',                        # L
    'Subsidiary',                                       # M
    'Internal ID Payment Terms',                        # N
    'Payment Terms',                                    # O
    'Internal ID IncotermsTerms',                       # P
    'IncotermsTerms',                                   # Q
    'Internal ID Ship from Country',                    # R
    'Ship from Country',                                # S
    'Internal ID Ship to Country',                      # T
    'Ship to Country',                                  # U
    'Internal ID Port',                                 # V
    'Port',                                             # W
    'ETD Date',                                         # X
    'Currency',                                         # Y
    'Exchange Rate',                                    # Z
    'Memo',                                             # AA
    'PO Number',                                        # AB
    'PO Date',                                          # AC
    'Internal ID of Sales Ref.',                        # AD
    'Sales Ref.',                                       # AE
    'Internal ID of Sales Channel',                     # AF
    'Sales Channel',                                    # AG
    'Internal ID of Distribution Channel',              # AH
    'Distribution Channel',                             # AI
    'Internal ID of Customer Country',                  # AJ
    'Customer Country',                                 # AK
    'Internal ID of Continent',                         # AL
    'Continent',                                        # AM
    'Internal ID of Customer Region',                   # AN
    'Customer Region',                                  # AO
    'Internal ID of Department',                        # AP
    'Department',                                       # AQ
    'Internal ID of Location (Branch)',                 # AR
    'Location (Branch)',                                # AS
    'Internal ID Cluster CEO',                          # AT
    'Cluster CEO',                                      # AU
    'Remark to Warehouse',                              # AV
    'Remark to Transport',                              # AW
    'Remark to Account',                                # AX
    'Place to',                                         # AY
    'Internal ID of Item',                              # AZ
    'Item',                                             # BA
    'Internal ID of Location (Line)',                   # BB
    'Location (Line)',                                  # BC
    'Quantity',                                         # BD
    'Unit',                                             # BE
    'Internal ID ofSPS Sale Discount Reference',        # BF
    'SPS Sale Discount Reference',                      # BG
    'SPS STD Sales Unit Price',                         # BH
    'SPS STD Sales Total',                              # BI
    'Unit Price after Item Discount (Exc.VAT)',         # BJ
    'Amount',                                           # BK
    'Tax Code 1',                                       # BL
    'Tax Amt',                                          # BM
    'Gross Amount',                                     # BN
    'Amount/TON',                                       # BO
    'Conv.of Unit (EA/PC)',                             # BP
    'Conv. of Unit (Pack)',                             # BQ
    'Conv. of Unit (KG)',                               # BR
    'Conv.of Unit (Ream)',                              # BS
    'Conv.of Unit (Box)',                               # BT
    'Conv.of Unit (TON)',                               # BU
    'Conv.of Unit (PAL)',                               # BV
    'Net Weight',                                       # BW
    'Gross Weight',                                     # BX
    'SPS Sales Discount Total',                         # BY
    'ID of Discount/Markup 1',                          # BZ
    'Code of Discount/Markup 1',                        # CA
    'SPS Net Discount/Markup 1',                        # CB
    'ID of Discount/Markup 2',                          # CC
    'Code of Discount/Markup 2',                        # CD
    'SPS Net Discount/Markup 2',                        # CE
    'ID of Discount/Markup 3',                          # CF
    'Code of Discount/Markup 3',                        # CG
    'SPS Net Discount/Markup 3',                        # CH
    'ID of Discount/Markup 4',                          # CI
    'Code of Discount/Markup 4',                        # CJ
    'SPS Net Discount/Markup 4',                        # CK
    'ID of Discount/Markup 5',                          # CL
    'Code of Discount/Markup 5',                        # CM
    'SPS Net Discount/Markup 5',                        # CN
    'ID of Discount/Markup 6',                          # CO
    'Code of Discount/Markup 6',                        # CP
    'SPS Net Discount/Markup 6',                        # CQ
    'Internal ID of Promotion Order Code1',             # CR
    'Promotion Order Code1',                            # CS
    'Promotion Order Amount 1',                         # CT
    'Internal ID of Promotion Order Code2',             # CU
    'Promotion Order Code2',                            # CV
    'Promotion Order Amount 2',                         # CW
    'Internal ID of Promotion Order Code3',             # CX
    'Promotion Order Code3',                            # CY
    'Promotion Order Amount 3',                         # CZ
    'Promotion Order Amount Total',                     # DA
    'Internal ID of Promotion Code (FOC)',              # DB
    'Promotion Code (FOC)',                             # DC
    'Blank 2',                                          # DD
    'Tax Code 2',                                       # DE
]

# SAP report column indices
SAP_COL_SALES_ORDER_NO       = 0
SAP_COL_MATERIAL_NO          = 2
SAP_COL_PAYMENT_TERM_CODE    = 7
SAP_COL_SOLD_TO_NO           = 9
SAP_COL_SHIP_TO_NO           = 11
SAP_COL_SHIPPING_LABEL       = 13
SAP_COL_SALES_EMPLOYEE_NAME  = 15
SAP_COL_PO_NUMBER            = 16
SAP_COL_PLANT                = 17
SAP_COL_SO_DATE              = 20
SAP_COL_PO_DATE              = 21
SAP_COL_REQUEST_DELIVERY_DATE = 22
SAP_COL_ORDER_QUANTITY       = 23
SAP_COL_PENDING_QUANTITY     = 24
SAP_COL_SALES_UNIT           = 25
SAP_COL_AMOUNT               = 35
SAP_COL_ZP01                 = 41
SAP_COL_ZP01_AMOUNT          = 43
SAP_COL_ZP01_VALUE           = 47
SAP_COL_ZC01                 = 49
SAP_COL_ZC01_AMOUNT          = 51
SAP_COL_ZC01_VALUE           = 55
SAP_COL_ZD13                 = 57
SAP_COL_ZD13_VALUE           = 63


def _recalc_running_no(output_df):
    running_nos = []
    current_so = None
    counter = 0
    for ext_id in output_df['External ID of Sales Order'].values:
        if ext_id == current_so:
            counter += 1
        else:
            current_so = ext_id
            counter = 1
        running_nos.append(counter)
    output_df['Internal Running No'] = (
        output_df['External ID of Sales Order'] + '_' + [str(n) for n in running_nos]
    )


def generate_opening_so(excel_path: str, output_path: str) -> dict:
    """Generate Opening SO Excel from SAP report.

    Returns a dict with 'status', 'message', 'rows' count.
    """
    engine = get_engine()

    # ===== 1. Read SAP report =====
    sap_df = pd.read_excel(excel_path, sheet_name='SAP report')
    col_name_so = sap_df.columns[SAP_COL_SALES_ORDER_NO]
    sap_df = sap_df.dropna(subset=[col_name_so])
    sap_df[col_name_so] = sap_df[col_name_so].astype(int).astype(str)
    sap_df = sap_df.sort_values(by=col_name_so).reset_index(drop=True)

    # ===== 2. Load lookup tables =====
    customer_df = pd.read_sql(
        "SELECT internal_id, old_code, code, customer_name FROM customer_master", con=engine
    )
    valid_customers = customer_df.dropna(subset=['old_code'])
    valid_customers = valid_customers[valid_customers['old_code'].astype(str).str.strip() != '']
    old_code_to_internal_id = dict(zip(valid_customers['old_code'].astype(str), valid_customers['internal_id']))
    old_code_to_code = dict(zip(valid_customers['old_code'].astype(str), valid_customers['code']))
    internal_id_to_customer_name = dict(zip(customer_df['internal_id'], customer_df['customer_name']))

    shipping_label_df = pd.read_sql(
        "SELECT address_internal_id, ship_to_code, address_label, ship_to_name FROM shipping_label_master", con=engine
    )
    valid_shipping = shipping_label_df.dropna(subset=['address_label'])
    valid_shipping = valid_shipping[valid_shipping['address_label'].astype(str).str.strip() != '']
    shipping_label_choices = valid_shipping['address_label'].astype(str).str.strip().tolist()
    shipping_label_records = valid_shipping[['address_internal_id']].to_dict('records')

    payment_term_df = pd.read_sql(
        "SELECT internal_id, payment_term_code, name FROM payment_term_master", con=engine
    )
    valid_payment = payment_term_df.dropna(subset=['payment_term_code'])
    valid_payment = valid_payment[valid_payment['payment_term_code'].astype(str).str.strip() != '']
    payment_code_to_internal_id = dict(zip(valid_payment['payment_term_code'].astype(str), valid_payment['internal_id']))
    payment_code_to_name = dict(zip(valid_payment['payment_term_code'].astype(str), valid_payment['name']))

    sales_rep_df = pd.read_sql(
        "SELECT internal_id, employee_id, name, first_name_th, last_name_th FROM sales_rep_master", con=engine
    )
    sales_rep_df['full_name_th'] = (
        sales_rep_df['first_name_th'].fillna('') + sales_rep_df['last_name_th'].fillna('')
    ).str.strip()
    valid_sales_rep = sales_rep_df[sales_rep_df['full_name_th'] != ''].copy()
    sales_rep_choices = valid_sales_rep['full_name_th'].tolist()
    sales_rep_records = valid_sales_rep[['internal_id', 'employee_id', 'name']].to_dict('records')

    item_df = pd.read_sql(
        "SELECT internal_id, item_code, item_name, old_item_code FROM item_master", con=engine
    )
    valid_items = item_df.dropna(subset=['old_item_code'])
    valid_items = valid_items[valid_items['old_item_code'].astype(str).str.strip() != '']
    old_item_code_to_internal_id = dict(zip(valid_items['old_item_code'].astype(str), valid_items['internal_id']))
    old_item_code_to_item_code = dict(zip(valid_items['old_item_code'].astype(str), valid_items['item_code']))

    location_df = pd.read_sql("SELECT plant, location_id, location_name FROM location_master", con=engine)
    plant_to_location_id = dict(zip(location_df['plant'].astype(str), location_df['location_id']))
    plant_to_location_name = dict(zip(location_df['plant'].astype(str), location_df['location_name']))

    # ===== 3. Build output DataFrame =====
    output_df = pd.DataFrame(columns=OUTPUT_COLUMNS)

    # (C) External ID of Sales Order
    output_df['External ID of Sales Order'] = 'SO_DA_' + sap_df.iloc[:, SAP_COL_SALES_ORDER_NO].values

    # (A) Product
    output_df['Product'] = 'CZ'

    # (B) Internal Running No
    _recalc_running_no(output_df)

    # (D)(E) Customer
    sold_to_nos = sap_df.iloc[:, SAP_COL_SOLD_TO_NO].values
    ids_cust, names_cust = [], []
    for sold_to in sold_to_nos:
        if pd.isna(sold_to) or str(sold_to).strip() == '':
            ids_cust.append(None); names_cust.append(None)
        else:
            s = str(int(sold_to)) if isinstance(sold_to, float) else str(sold_to)
            iid = old_code_to_internal_id.get(s)
            ids_cust.append(iid)
            names_cust.append(internal_id_to_customer_name.get(iid) if iid else None)
    output_df['Internal ID of Customer'] = ids_cust
    output_df['Customer'] = names_cust

    # (F)(G) Ship to Customer
    ship_to_nos = sap_df.iloc[:, SAP_COL_SHIP_TO_NO].values
    ids_ship, names_ship = [], []
    for ship_to in ship_to_nos:
        if pd.isna(ship_to) or str(ship_to).strip() == '':
            ids_ship.append(None); names_ship.append(None)
        else:
            s = str(int(ship_to)) if isinstance(ship_to, float) else str(ship_to)
            iid = old_code_to_internal_id.get(s)
            ids_ship.append(iid)
            names_ship.append(internal_id_to_customer_name.get(iid) if iid else None)
    output_df['Internal ID of Ship to Customer'] = ids_ship
    output_df['Ship to Customer'] = names_ship

    # (H)(I) Shipping Label
    sap_shipping_labels = sap_df.iloc[:, SAP_COL_SHIPPING_LABEL].values
    label_ids = []
    for label in sap_shipping_labels:
        if pd.isna(label) or str(label).strip() == '':
            label_ids.append(None)
        else:
            result = process.extractOne(str(label).strip(), shipping_label_choices, scorer=fuzz.ratio)
            if result and result[1] >= 80:
                idx = shipping_label_choices.index(result[0])
                label_ids.append(shipping_label_records[idx]['address_internal_id'])
            else:
                label_ids.append(None)
    output_df['Internal ID of Shipping Label New'] = label_ids
    output_df['Shipping Label'] = sap_shipping_labels

    # (J) Date
    so_dates = pd.to_datetime(sap_df.iloc[:, SAP_COL_SO_DATE], errors='coerce')
    output_df['Date'] = so_dates.dt.strftime('%d/%m/%y').values

    # (K)-(M) Constants
    output_df['Domestic/Export'] = 'Domestic'
    output_df['Internal ID of Subsidiary'] = 2
    output_df['Subsidiary'] = 'Double A (1991) PLC'

    # (N)(O) Payment Terms
    pt_codes = sap_df.iloc[:, SAP_COL_PAYMENT_TERM_CODE].values
    pt_ids, pt_names = [], []
    for pt in pt_codes:
        if pd.isna(pt) or str(pt).strip() == '':
            pt_ids.append(None); pt_names.append(None)
        else:
            s = str(pt).strip()
            pt_ids.append(payment_code_to_internal_id.get(s))
            pt_names.append(payment_code_to_name.get(s))
    output_df['Internal ID Payment Terms'] = pt_ids
    output_df['Payment Terms'] = pt_names

    # (P)(Q) Incoterms
    output_df['Internal ID IncotermsTerms'] = 5
    output_df['IncotermsTerms'] = 'DAP'

    # (S) Ship from Country
    output_df['Ship from Country'] = 'Thailand'

    # (X) ETD Date
    etd_dates = pd.to_datetime(sap_df.iloc[:, SAP_COL_REQUEST_DELIVERY_DATE], errors='coerce')
    output_df['ETD Date'] = etd_dates.dt.strftime('%d/%m/%y').values

    # (Y)(Z) Currency
    output_df['Currency'] = 'THB'
    output_df['Exchange Rate'] = 1

    # (AB)(AC) PO
    output_df['PO Number'] = sap_df.iloc[:, SAP_COL_PO_NUMBER].values
    po_dates = pd.to_datetime(sap_df.iloc[:, SAP_COL_PO_DATE], errors='coerce')
    output_df['PO Date'] = po_dates.dt.strftime('%d/%m/%y').values

    # (AD)(AE) Sales Ref - fuzzy match
    emp_names = sap_df.iloc[:, SAP_COL_SALES_EMPLOYEE_NAME].values
    sr_ids, sr_names = [], []
    for emp in emp_names:
        if pd.isna(emp) or str(emp).strip() == '':
            sr_ids.append(None); sr_names.append(None)
        else:
            result = process.extractOne(str(emp).strip(), sales_rep_choices, scorer=fuzz.ratio)
            if result and result[1] >= 80:
                idx = sales_rep_choices.index(result[0])
                rec = sales_rep_records[idx]
                sr_ids.append(rec['internal_id'])
                sr_names.append(f"{rec['employee_id']} {rec['name']}")
            else:
                sr_ids.append(None); sr_names.append(None)
    output_df['Internal ID of Sales Ref.'] = sr_ids
    output_df['Sales Ref.'] = sr_names

    # (AJ)-(AM) Country / Continent constants
    output_df['Internal ID of Customer Country'] = 324
    output_df['Customer Country'] = 'Thailand'
    output_df['Internal ID of Continent'] = 2
    output_df['Continent'] = 'Asia'

    # (AZ)(BA) Item
    mat_nos = sap_df.iloc[:, SAP_COL_MATERIAL_NO].values
    item_ids, item_codes = [], []
    for m in mat_nos:
        if pd.isna(m) or str(m).strip() == '':
            item_ids.append(None); item_codes.append(None)
        else:
            s = str(int(m)) if isinstance(m, float) else str(m)
            item_ids.append(old_item_code_to_internal_id.get(s))
            item_codes.append(old_item_code_to_item_code.get(s))
    output_df['Internal ID of Item'] = item_ids
    output_df['Item'] = item_codes

    # (BB)(BC) Location
    plants = sap_df.iloc[:, SAP_COL_PLANT].values
    loc_ids, loc_names = [], []
    for p in plants:
        if pd.isna(p) or str(p).strip() == '':
            loc_ids.append(None); loc_names.append(None)
        else:
            s = str(int(p)) if isinstance(p, float) else str(p)
            loc_ids.append(plant_to_location_id.get(s))
            loc_names.append(plant_to_location_name.get(s))
    output_df['Internal ID of Location (Line)'] = loc_ids
    output_df['Location (Line)'] = loc_names

    # (BD)(BE) Quantity / Unit
    output_df['Quantity'] = sap_df.iloc[:, SAP_COL_PENDING_QUANTITY].values
    output_df['Unit'] = sap_df.iloc[:, SAP_COL_SALES_UNIT].values

    # (BH) SPS STD Sales Unit Price
    output_df['SPS STD Sales Unit Price'] = sap_df.iloc[:, SAP_COL_AMOUNT].apply(parse_sap_number)

    # (BI) SPS STD Sales Total
    output_df['SPS STD Sales Total'] = output_df['Quantity'] * output_df['SPS STD Sales Unit Price']

    # (BJ) Unit Price after Item Discount
    aq = sap_df.iloc[:, SAP_COL_ZP01_AMOUNT].apply(parse_sap_number).fillna(0)
    ay = sap_df.iloc[:, SAP_COL_ZC01_AMOUNT].apply(parse_sap_number).fillna(0)
    output_df['Unit Price after Item Discount (Exc.VAT)'] = output_df['SPS STD Sales Unit Price'] + (aq + ay)

    # (BK) Amount
    output_df['Amount'] = output_df['Unit Price after Item Discount (Exc.VAT)'] * output_df['Quantity']

    # (BL)(BM)(BN) Tax
    output_df['Tax Code 1'] = 'VAT_TH:VAT 7%'
    output_df['Tax Amt'] = output_df['Amount'] * 0.07
    output_df['Gross Amount'] = output_df['Amount'] + output_df['Tax Amt']

    # (CA)(CB) ZP01 Discount
    sap_zp01 = sap_df.iloc[:, SAP_COL_ZP01].astype(str).str.strip().str.upper()
    has_zp01 = sap_zp01.str.contains('ZP01', na=False)
    output_df.loc[has_zp01, 'Code of Discount/Markup 1'] = 'ZP01'
    output_df.loc[has_zp01, 'SPS Net Discount/Markup 1'] = sap_df.iloc[:, SAP_COL_ZP01_VALUE].apply(parse_sap_number)[has_zp01]

    # (CD)(CE) ZC01 Discount
    sap_zc01 = sap_df.iloc[:, SAP_COL_ZC01].astype(str).str.strip().str.upper()
    has_zc01 = sap_zc01.str.contains('ZC01', na=False)
    output_df.loc[has_zc01, 'Code of Discount/Markup 2'] = 'ZC01'
    output_df.loc[has_zc01, 'SPS Net Discount/Markup 2'] = sap_df.iloc[:, SAP_COL_ZC01_VALUE].apply(parse_sap_number)[has_zc01]

    # (BY) SPS Sales Discount Total
    discount_cols = [
        'SPS Net Discount/Markup 1', 'SPS Net Discount/Markup 2',
        'SPS Net Discount/Markup 3', 'SPS Net Discount/Markup 4',
        'SPS Net Discount/Markup 5', 'SPS Net Discount/Markup 6',
    ]
    output_df['SPS Sales Discount Total'] = output_df[discount_cols].fillna(0).sum(axis=1)

    # (CT) Promotion Order Amount 1
    sap_zd13_value = sap_df.iloc[:, SAP_COL_ZD13_VALUE].apply(parse_sap_number)
    output_df['Promotion Order Amount 1'] = sap_zd13_value

    # (DA) Promotion Order Amount Total
    promo_cols = ['Promotion Order Amount 1', 'Promotion Order Amount 2', 'Promotion Order Amount 3']
    output_df['Promotion Order Amount Total'] = output_df[promo_cols].fillna(0).sum(axis=1)

    # ===== 3.5 Insert subtotal rows =====
    has_promo = output_df[promo_cols].notna().any(axis=1) & (output_df[promo_cols].fillna(0) != 0).any(axis=1)
    copy_cols = (
        [OUTPUT_COLUMNS[0]]
        + OUTPUT_COLUMNS[2:7]
        + OUTPUT_COLUMNS[8:51]
        + OUTPUT_COLUMNS[53:55]
    )
    sum_cols = [
        'SPS Sales Discount Total', 'SPS Net Discount/Markup 1', 'SPS Net Discount/Markup 2',
        'SPS Net Discount/Markup 3', 'SPS Net Discount/Markup 4', 'SPS Net Discount/Markup 5',
        'SPS Net Discount/Markup 6', 'Promotion Order Amount 1',
    ]

    inserted_row_indices = []
    promotion_row_indices = []

    if has_promo.any():
        so_with_promo = set(output_df.loc[has_promo, 'External ID of Sales Order'].unique())
        so_sums = output_df[output_df['External ID of Sales Order'].isin(so_with_promo)] \
            .groupby('External ID of Sales Order')[sum_cols].sum()

        new_rows = []
        is_inserted = []
        prev_so = None

        def _build_subtotal(last_row, so_ext_id):
            dup = {col: None for col in OUTPUT_COLUMNS}
            for col in copy_cols:
                dup[col] = last_row[col]
            dup['Internal ID of Item'] = -2
            dup['Item'] = 'Subtotal'
            for col in sum_cols:
                dup[col] = so_sums.loc[so_ext_id, col]
            return dup

        for idx in range(len(output_df)):
            current_so = output_df.iloc[idx]['External ID of Sales Order']
            if prev_so is not None and current_so != prev_so and prev_so in so_with_promo:
                new_rows.append(_build_subtotal(new_rows[-1], prev_so))
                is_inserted.append(True)
            new_rows.append(output_df.iloc[idx].to_dict())
            is_inserted.append(False)
            prev_so = current_so

        if prev_so in so_with_promo:
            new_rows.append(_build_subtotal(new_rows[-1], prev_so))
            is_inserted.append(True)

        output_df = pd.DataFrame(new_rows, columns=OUTPUT_COLUMNS).reset_index(drop=True)
        _recalc_running_no(output_df)
        inserted_row_indices = [i for i, f in enumerate(is_inserted) if f]

        # ===== 3.6 Insert promotion rows =====
        subtotal_set = set(inserted_row_indices)
        subtotal_with_ct = [
            i for i in inserted_row_indices
            if pd.notna(output_df.at[i, 'Promotion Order Amount 1'])
            and output_df.at[i, 'Promotion Order Amount 1'] != 0
        ]

        if subtotal_with_ct:
            subtotal_with_ct_set = set(subtotal_with_ct)
            new_rows2 = []
            row_types = []

            for idx in range(len(output_df)):
                new_rows2.append(output_df.iloc[idx].to_dict())
                row_types.append('subtotal' if idx in subtotal_set else 'normal')

                if idx in subtotal_with_ct_set:
                    promo_row = output_df.iloc[idx].to_dict()
                    ct_val = promo_row.get('Promotion Order Amount 1') or 0
                    promo_row['Internal ID of Item'] = 72795
                    promo_row['Item'] = 'LDCD-IR-001'
                    promo_row['SPS STD Sales Total'] = ct_val
                    promo_row['Amount'] = ct_val
                    promo_row['Tax Code 1'] = 'VAT_TH:VAT 7%'
                    promo_row['Tax Amt'] = ct_val * 0.07
                    promo_row['Gross Amount'] = ct_val + ct_val * 0.07
                    new_rows2.append(promo_row)
                    row_types.append('promotion')

            output_df = pd.DataFrame(new_rows2, columns=OUTPUT_COLUMNS).reset_index(drop=True)
            _recalc_running_no(output_df)
            inserted_row_indices = [i for i, t in enumerate(row_types) if t == 'subtotal']
            promotion_row_indices = [i for i, t in enumerate(row_types) if t == 'promotion']

    # ===== 4. Write to Excel =====
    output_df.to_excel(output_path, index=False, sheet_name='Opening SO')

    wb = load_workbook(output_path)
    ws = wb['Opening SO']

    for col_letter, width in COLUMN_WIDTHS.items():
        ws.column_dimensions[col_letter].width = width

    if inserted_row_indices or promotion_row_indices:
        green_fill = PatternFill(start_color='C6EFCE', end_color='C6EFCE', fill_type='solid')
        pink_fill = PatternFill(start_color='FCE4EC', end_color='FCE4EC', fill_type='solid')
        num_cols = len(OUTPUT_COLUMNS)
        for row_idx in inserted_row_indices:
            for col in range(1, num_cols + 1):
                ws.cell(row=row_idx + 2, column=col).fill = green_fill
        for row_idx in promotion_row_indices:
            for col in range(1, num_cols + 1):
                ws.cell(row=row_idx + 2, column=col).fill = pink_fill

    wb.save(output_path)

    return {
        "status": "success",
        "message": f"Generated {len(output_df)} rows",
        "rows": len(output_df),
        "subtotal_rows": len(inserted_row_indices),
        "promotion_rows": len(promotion_row_indices),
    }
