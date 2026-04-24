"""
SAP MRP ENGINE — Full L1 to L4 with Phantom Handling
Streamlit Cloud deployment (GitHub-ready)

Order type rules (from export file):
  DM00 + EM00  →  FG production orders  →  USE as FG demand/requirement
  LA           →  Planned orders         →  NOT used as requirement
  SF01         →  Component prod orders  →  Open_Qty + Confirmed_Qty per component

Output: date-wise shortage columns (one column per unique Basic finish date in DM00/EM00)
"""

import io
import re
import pandas as pd
import streamlit as st

# ═══════════════════════════════════════════════════════════════
# PAGE CONFIG
# ═══════════════════════════════════════════════════════════════
st.set_page_config(page_title="SAP MRP Engine", page_icon="⚙️", layout="wide")

# ═══════════════════════════════════════════════════════════════
# LOGIN WALL  — credentials stored in Streamlit secrets
# secrets.toml:  admin_id = "admin"   admin_password = "admin@2040"
# ═══════════════════════════════════════════════════════════════
def check_login():
    if st.session_state.get("authenticated"):
        return True

    st.title("⚙️ SAP MRP Engine")
    st.subheader("🔐 Login")

    with st.form("login_form"):
        user_id  = st.text_input("User ID")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Login", use_container_width=True)

    if submitted:
        valid_id  = st.secrets.get("admin_id",       "admin")
        valid_pwd = st.secrets.get("admin_password",  "admin@2040")
        if user_id.strip() == valid_id and password == valid_pwd:
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("Invalid User ID or Password.")

    return False

if not check_login():
    st.stop()

st.title("⚙️ SAP MRP Engine — L1 to L4")
st.caption("Phantom handling · Alt-aware · NET propagation · Date-wise shortage")

# ═══════════════════════════════════════════════════════════════
# SIDEBAR  — file uploads only, no config clutter
# ═══════════════════════════════════════════════════════════════
with st.sidebar:
    st.header("Upload files")
    export_file  = st.file_uploader(
        "Export file (.xlsx) ✱  — DM00/EM00 demand + SF01 component orders",
        type=["xlsx","xls"], key="export")
    stock_file   = st.file_uploader(
        "Stock file (.xlsx) ✱  — Stock sheet",
        type=["xlsx","xls"], key="stock")
    bom_file     = st.file_uploader(
        "BOM file (.xlsx) ✱",
        type=["xlsx","xls"], key="bom")
    receipt_file = st.file_uploader(
        "Receipt Quantities (.xlsx) — optional",
        type=["xlsx","xls"], key="receipt",
        help="GR/receipt quantities added to stock before MRP run")
    run_btn = st.button("▶ Run MRP", type="primary", use_container_width=True)


# ═══════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════
PHANTOM             = "50"                     # Special procurement = phantom
FG_ORDER_TYPES      = {"DM00", "EM00"}         # FG production orders  → demand
COMP_ORDER_TYPE     = "SF01"                   # Component prod orders → open/confirmed qty


# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════
def is_phantom(val):
    return str(val).strip() == PHANTOM


def safe_series(df, col):
    """Return Series even if duplicate column names give a DataFrame."""
    result = df[col]
    return result.iloc[:, 0] if isinstance(result, pd.DataFrame) else result


def empty_prod_summary():
    return pd.DataFrame(columns=["Component", "Confirmed_Qty",
                                  "Open_Production_Qty", "Plan_Order_Qty"])


def fmt_date_col(dt):
    """Format a date as '01-Apr-26' for column headers."""
    if pd.isna(dt):
        return None
    return pd.Timestamp(dt).strftime("%d-%b-%y")


# ═══════════════════════════════════════════════════════════════
# EXPORT FILE PARSER
# ═══════════════════════════════════════════════════════════════
def parse_export_file(export_file):
    """
    Parse the SAP export file into:
      fg_demand    — DataFrame[BOM_Header, Alt, Date_col, FG_Demand]
                     One row per (FG, Alt, date) combination.
                     Source: DM00 + EM00 orders with FA-prefix Production Version.
      prod_summary — DataFrame[Component, Confirmed_Qty, Open_Production_Qty, Plan_Order_Qty]
                     Source: SF01 for Open/Confirmed; LA (non-FA) for Plan_Order_Qty.
      date_cols    — sorted list of date-label strings ('01-Apr-26', ...)
    """
    df = pd.read_excel(export_file)
    df.columns = df.columns.str.strip()

    # ── Detect key columns ────────────────────────────────────
    def find_col(df, *keywords, exclude=None):
        for c in df.columns:
            cl = c.lower()
            if all(k in cl for k in keywords):
                if exclude is None or not any(e in cl for e in exclude):
                    return c
        return None

    order_col   = find_col(df, "order") or "Order"
    mat_col     = find_col(df, "material") or "Material Number"
    pv_col      = find_col(df, "production", "version") or "Production Version"
    otype_col   = find_col(df, "order", "type") or "Order Type"
    ord_qty_col = find_col(df, "order", "quantity") or "Order quantity (GMEIN)"
    del_qty_col = find_col(df, "delivered") or find_col(df, "quantity", "deliver") or "Quantity Delivered (GMEIN)"
    conf_col    = find_col(df, "confirmed", "quantity") or "Confirmed quantity (GMEIN)"
    date_col    = find_col(df, "finish", "date") or find_col(df, "basic", "finish") or "Basic finish date"
    status_col  = find_col(df, "system", "status") or find_col(df, "status")

    # ── Coerce types ─────────────────────────────────────────
    for c in [ord_qty_col, del_qty_col, conf_col]:
        if c and c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    df[mat_col]   = df[mat_col].astype(str).str.strip()
    df[pv_col]    = df[pv_col].astype(str).str.strip()
    df[otype_col] = df[otype_col].astype(str).str.strip()
    if date_col and date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    # ── Remove TECO ───────────────────────────────────────────
    if status_col and status_col in df.columns:
        df = df[~df[status_col].astype(str)
                .str.contains("TECO", case=False, na=False)].copy()

    # ── FG demand: DM00 + EM00, FA-prefix production version ─
    fg_mask = (
        df[otype_col].isin(FG_ORDER_TYPES) &
        df[pv_col].str.upper().str.startswith("FA")
    )
    fg_rows = df[fg_mask].copy()

    # Alt BOM = strip 'FA' from production version  (FA10→'10', FA20→'20')
    fg_rows["_alt"] = (
        fg_rows[pv_col].str.upper().str.replace("FA", "", regex=False).str.strip()
    )
    fg_rows["_alt"] = (
        pd.to_numeric(fg_rows["_alt"], errors="coerce")
        .fillna(0).astype(int).astype(str)
    )

    # Date label for column headers
    if date_col and date_col in fg_rows.columns:
        fg_rows["_date_col"] = fg_rows[date_col].apply(fmt_date_col)
    else:
        fg_rows["_date_col"] = "Unknown"

    fg_rows = fg_rows[fg_rows["_date_col"].notna()]

    # Aggregate: same FG + Alt + date can have multiple orders on the same day
    fg_demand = (
        fg_rows.groupby([mat_col, "_alt", "_date_col"], as_index=False)[ord_qty_col]
        .sum()
        .rename(columns={mat_col:"BOM_Header", "_alt":"Alt",
                         "_date_col":"Date_col", ord_qty_col:"FG_Demand"})
    )
    fg_demand = fg_demand[fg_demand["FG_Demand"] > 0]

    # ── Sort date columns chronologically ─────────────────────
    def date_sort_key(s):
        try:
            return pd.to_datetime(s, format="%d-%b-%y")
        except Exception:
            return pd.Timestamp.max

    date_cols = sorted(fg_demand["Date_col"].unique(), key=date_sort_key)

    # ── Component production orders (SF01): open + confirmed ──
    sf_rows = df[df[otype_col] == COMP_ORDER_TYPE].copy()
    if not sf_rows.empty and ord_qty_col in sf_rows and del_qty_col in sf_rows:
        sf_rows["_open"] = (sf_rows[ord_qty_col] - sf_rows[del_qty_col]).clip(lower=0)
        prod_agg = (
            sf_rows.groupby(mat_col, as_index=False)
                   .agg(Confirmed_Qty=(conf_col, "sum") if conf_col else ("_open","sum"),
                        Open_Production_Qty=("_open", "sum"))
            .rename(columns={mat_col: "Component"})
        )
    else:
        prod_agg = pd.DataFrame(columns=["Component","Confirmed_Qty","Open_Production_Qty"])

    # ── LA (non-FA): component plan orders ────────────────────
    plan_mask = (
        (df[otype_col] == "LA") &
        (~df[pv_col].str.upper().str.startswith("FA"))
    )
    plan_rows = df[plan_mask].copy()
    if not plan_rows.empty and ord_qty_col in plan_rows:
        plan_agg = (
            plan_rows.groupby(mat_col, as_index=False)[ord_qty_col]
                     .sum()
                     .rename(columns={mat_col:"Component", ord_qty_col:"Plan_Order_Qty"})
        )
    else:
        plan_agg = pd.DataFrame(columns=["Component","Plan_Order_Qty"])

    # ── Merge into prod_summary ───────────────────────────────
    prod_summary = prod_agg.merge(plan_agg, on="Component", how="outer")
    for col in ["Confirmed_Qty","Open_Production_Qty","Plan_Order_Qty"]:
        prod_summary[col] = prod_summary.get(col, pd.Series(dtype=float)).fillna(0)
    prod_summary["Component"] = prod_summary["Component"].astype(str).str.strip()

    return fg_demand, prod_summary, date_cols


# ═══════════════════════════════════════════════════════════════
# RECEIPT QUANTITY LOADER
# ═══════════════════════════════════════════════════════════════
def load_receipt_qty(receipt_file):
    if receipt_file is None:
        return pd.Series(dtype=float)
    try:
        df = pd.read_excel(receipt_file)
        df.columns = df.columns.str.strip()
        mat_kw = ["material","component","part number","part","mat"]
        qty_kw = ["gr qty","gr quantity","receipt qty","receipt quantity",
                  "received qty","quantity","qty"]
        mat_col = next((c for c in df.columns if any(k in c.lower() for k in mat_kw)),
                       df.columns[0])
        qty_col = next((c for c in df.columns if any(k in c.lower() for k in qty_kw)
                        and c != mat_col), None)
        if qty_col is None:
            st.warning("Receipt file: quantity column not detected — skipped.")
            return pd.Series(dtype=float)
        df[mat_col] = df[mat_col].astype(str).str.strip()
        df[qty_col] = pd.to_numeric(
            df[qty_col].astype(str).str.replace(",","",regex=False).str.strip(),
            errors="coerce").fillna(0)
        result = df.groupby(mat_col)[qty_col].sum()
        st.sidebar.success(f"Receipt file: {len(result):,} components loaded.")
        return result
    except Exception as e:
        st.warning(f"Receipt file error ({e}) — skipped.")
        return pd.Series(dtype=float)


# ═══════════════════════════════════════════════════════════════
# SEARCH + ANCESTRY TREE HELPERS
# ═══════════════════════════════════════════════════════════════
def get_ancestry_paths(component, bom):
    comp_rows = bom[bom["Component"] == component][
        ["BOM Header","Alt","Level","Parent","Component",
         "Required Qty","Component descriptio","Special procurement"]
    ].drop_duplicates()
    paths = []
    for _, row in comp_rows.iterrows():
        path_comps = [row["Component"]]
        path_descs = [row["Component descriptio"]]
        path_qtys  = [float(row["Required Qty"])]
        path_sp    = [str(row["Special procurement"]).strip()]
        current    = row["Parent"]
        fg         = row["BOM Header"]
        alt        = row["Alt"]
        for _ in range(4):
            if current == fg:
                break
            pr_rows = bom[
                (bom["BOM Header"]==fg) & (bom["Alt"]==alt) & (bom["Component"]==current)
            ]
            if pr_rows.empty:
                break
            pr = pr_rows.iloc[0]
            path_comps.insert(0, pr["Component"])
            path_descs.insert(0, pr["Component descriptio"])
            path_qtys.insert(0, float(pr["Required Qty"]))
            path_sp.insert(0, str(pr["Special procurement"]).strip())
            current = pr["Parent"]
        paths.append({"fg":fg, "alt":str(alt), "level":int(row["Level"]),
                       "path_comps":path_comps, "path_descs":path_descs,
                       "path_qtys":path_qtys, "path_sp":path_sp})
    return paths


def build_dot_tree(component, paths, fg_demand_df, date_cols, stock, prod_summary):
    # Total demand per FG+Alt
    fg_demand_map = {}
    for p in paths:
        rows = fg_demand_df[
            (fg_demand_df["BOM_Header"]==p["fg"]) & (fg_demand_df["Alt"]==p["alt"])
        ]
        fg_demand_map[(p["fg"],p["alt"])] = float(rows["FG_Demand"].sum())

    r = st.session_state.get("mrp_results", {})
    gross_map, shortage_map = {}, {}
    for key in ["result_l1","result_l2","result_l3","result_l4"]:
        df = r.get(key)
        if df is None or df.empty:
            continue
        agg = df.groupby("Component")[["Gross_Requirement","Shortage"]].sum()
        for comp2, row in agg.iterrows():
            gross_map[comp2]    = gross_map.get(comp2, 0) + row["Gross_Requirement"]
            shortage_map[comp2] = shortage_map.get(comp2, 0) + row["Shortage"]

    def trunc(s, n=20):
        return (str(s)[:n]+"…") if len(str(s))>n else str(s)

    node_attrs, edges, seen_edges = {}, [], set()

    for path in paths:
        fg, alt = path["fg"], path["alt"]
        demand  = fg_demand_map.get((fg,alt), 0)
        fg_id   = f"FG_{fg}_A{alt}".replace("-","_").replace(".","_")
        node_attrs[fg_id] = (
            f'label="FG: {fg}\\nAlt: {alt}\\nTotal demand: {demand:,.0f}"'
            f' shape=box style="filled,rounded" fillcolor="#2e86c1"'
            f' fontcolor=white fontsize=11'
        )
        prev_id = fg_id

        for comp2, desc, qty, sp in zip(
            path["path_comps"], path["path_descs"],
            path["path_qtys"],  path["path_sp"]
        ):
            is_tgt = (comp2 == component)
            is_ph  = (sp == PHANTOM)
            nid    = (f"N_{comp2}_FG_{fg}_A{alt}"
                      .replace("-","_").replace(".","_").replace("+","p"))
            gross    = gross_map.get(comp2, 0)
            shortage = shortage_map.get(comp2, 0)
            stk      = float(stock.get(comp2, 0))

            if is_tgt:
                prow = prod_summary[prod_summary["Component"]==comp2]
                conf = float(prow["Confirmed_Qty"].iloc[0])       if not prow.empty else 0
                oprd = float(prow["Open_Production_Qty"].iloc[0]) if not prow.empty else 0
                plan = float(prow["Plan_Order_Qty"].iloc[0])      if not prow.empty else 0
                label = (f"{trunc(comp2)}\\n{trunc(desc)}\\n"
                         f"Stock: {stk:,.0f}\\nConf: {conf:,.0f} | Open: {oprd:,.0f}\\n"
                         f"Plan: {plan:,.0f}\\n"
                         f"Gross: {gross:,.0f} | Short: {shortage:,.0f}")
                node_attrs[nid] = (
                    f'label="{label}" shape=box style="filled,rounded"'
                    f' fillcolor="#1e8449" fontcolor=white fontsize=10 penwidth=2.5'
                )
            elif is_ph:
                label = f"PHANTOM\\n{trunc(comp2)}\\n{trunc(desc)}\\nqty={qty:g}"
                node_attrs[nid] = (
                    f'label="{label}" shape=box style="filled,dashed"'
                    f' fillcolor="#f39c12" fontcolor="#333" fontsize=10'
                )
            else:
                label = (f"{trunc(comp2)}\\n{trunc(desc)}\\n"
                         f"Qty: {qty:g} | Stock: {stk:,.0f}\\n"
                         f"Gross: {gross:,.0f} | Short: {shortage:,.0f}")
                node_attrs[nid] = (
                    f'label="{label}" shape=box style="filled,rounded"'
                    f' fillcolor="#f9e79f" fontcolor="#333" fontsize=10'
                )

            ek = (prev_id, nid)
            if ek not in seen_edges:
                edges.append((prev_id, nid, f"×{qty:g}"))
                seen_edges.add(ek)
            prev_id = nid

    lines = [
        "digraph MRP {", "  rankdir=TB;",
        '  node [fontname="Arial"];', '  edge [fontname="Arial" fontsize=10];',
        "  graph [splines=ortho nodesep=0.6 ranksep=0.8];",
    ]
    for nid, attrs in node_attrs.items():
        lines.append(f'  "{nid}" [{attrs}];')
    for src, dst, lbl in edges:
        lines.append(f'  "{src}" -> "{dst}" [label="{lbl}"];')
    lines.append("}")
    return "\n".join(lines)


def show_search_section(bom, fg_demand_df, date_cols, stock, prod_summary):
    st.divider()
    st.subheader("🔍 Component Search")
    st.caption("Enter any component code to see date-wise demand, shortage and BOM ancestry tree.")

    scol, _ = st.columns([2,3])
    with scol:
        comp = st.text_input("Component code", placeholder="e.g. 0010748458",
                             label_visibility="collapsed").strip()
    if not comp:
        return

    r = st.session_state.get("mrp_results", {})
    found_in = {}
    for lbl in ["result_l1","result_l2","result_l3","result_l4"]:
        df2 = r.get(lbl)
        if df2 is not None and not df2.empty and comp in df2["Component"].values:
            found_in[lbl] = df2[df2["Component"]==comp].copy()

    bom_in = bom[bom["Component"]==comp]
    if bom_in.empty and not found_in:
        st.warning(f"`{comp}` not found in BOM or MRP results.")
        return

    desc  = bom_in["Component descriptio"].iloc[0] if not bom_in.empty else "—"
    ptype = bom_in["Procurement type"].iloc[0]     if not bom_in.empty else "—"
    sp    = bom_in["Special procurement"].iloc[0]  if not bom_in.empty else "—"
    stk   = float(stock.get(comp, 0))
    prow  = prod_summary[prod_summary["Component"]==comp]
    conf_qty = float(prow["Confirmed_Qty"].iloc[0])       if not prow.empty else 0
    open_qty = float(prow["Open_Production_Qty"].iloc[0]) if not prow.empty else 0
    plan_qty = float(prow["Plan_Order_Qty"].iloc[0])      if not prow.empty else 0

    ph_badge = " 🔶 PHANTOM" if str(sp).strip()==PHANTOM else ""
    st.markdown(f"### `{comp}` — {desc}{ph_badge}")

    c1,c2,c3,c4,c5,c6 = st.columns(6)
    c1.metric("Stock on hand",      f"{stk:,.3f}")
    c2.metric("Confirmed prod qty", f"{conf_qty:,.0f}")
    c3.metric("Open prod qty",      f"{open_qty:,.0f}")
    c4.metric("Plan order qty",     f"{plan_qty:,.0f}")
    c5.metric("Procurement type",   ptype)
    c6.metric("Sp. procurement",    sp if sp not in ("","nan") else "—")

    if found_in:
        st.markdown("#### Date-wise demand & shortage")
        all_rows = pd.concat(found_in.values(), ignore_index=True)

        def date_sort_key(s):
            try:
                return pd.to_datetime(s, format="%d-%b-%y")
            except Exception:
                return pd.Timestamp.max

        dc_order = {m:i for i,m in enumerate(date_cols)}
        daily = (all_rows.groupby("Month", as_index=False)
                 .agg(Gross_Requirement=("Gross_Requirement","sum"),
                      Stock_Used=("Stock_Used","sum"),
                      Shortage=("Shortage","sum"),
                      Stock_Remaining=("Stock_Remaining","last")))
        daily["_ord"] = daily["Month"].map(dc_order)
        daily = daily.sort_values("_ord").drop(columns="_ord")
        daily["Cumul. Shortage / Excess"] = daily["Shortage"].cumsum()
        daily = daily.rename(columns={
            "Month":"Date",
            "Shortage":"Shortage(+) / Excess(-)"
        })

        def hl(row):
            val = row["Shortage(+) / Excess(-)"]
            if val > 0:
                return ["background-color:#ffe0e0"]*len(row)   # red  = shortage
            elif val < 0:
                return ["background-color:#e0f7e0"]*len(row)   # green = excess
            return [""]*len(row)

        st.dataframe(
            daily.style.apply(hl, axis=1).format({
                "Gross_Requirement":"{:,.2f}","Stock_Used":"{:,.2f}",
                "Shortage(+) / Excess(-)":"{:,.2f}","Stock_Remaining":"{:,.2f}",
                "Cumul. Shortage / Excess":"{:,.2f}"}),
            use_container_width=True, hide_index=True)

        s1,s2,s3,s4 = st.columns(4)
        s1.metric("Total gross req",    f"{daily['Gross_Requirement'].sum():,.2f}")
        s2.metric("Stock consumed",     f"{daily['Stock_Used'].sum():,.2f}")
        total_short = daily[daily["Shortage(+) / Excess(-)"]>0]["Shortage(+) / Excess(-)"].sum()
        total_excess = abs(daily[daily["Shortage(+) / Excess(-)"]<0]["Shortage(+) / Excess(-)"].sum())
        s3.metric("Total shortage",     f"{total_short:,.2f}")
        s4.metric("Total excess stock", f"{total_excess:,.2f}")
    else:
        st.info("Component in BOM but not in MRP results (phantom or no demand).")

    st.markdown("#### BOM ancestry tree")
    st.caption("🔵 FG   🟡 Intermediate   🟠 Phantom   🟢 Searched component")
    paths = get_ancestry_paths(comp, bom)
    if not paths:
        st.info("No ancestry paths found.")
        return

    fg_rows = []
    for p in paths:
        rows = fg_demand_df[
            (fg_demand_df["BOM_Header"]==p["fg"]) & (fg_demand_df["Alt"]==p["alt"])
        ]
        total = float(rows["FG_Demand"].sum())
        fg_rows.append({"FG code":p["fg"], "Alt":p["alt"], "BOM level":p["level"],
                        "Total demand":f"{total:,.0f}"})

    fg_df = pd.DataFrame(fg_rows).drop_duplicates(subset=["FG code","Alt"])
    st.dataframe(fg_df, use_container_width=True, hide_index=True)

    MAX_PATHS = 12
    display_paths = paths[:MAX_PATHS]
    if len(paths) > MAX_PATHS:
        st.caption(f"⚠️ Showing {MAX_PATHS} of {len(paths)} ancestry paths.")

    dot = build_dot_tree(comp, display_paths, fg_demand_df, date_cols, stock, prod_summary)
    try:
        st.graphviz_chart(dot, use_container_width=True)
    except Exception as e:
        st.error(f"Tree render error: {e}")
        with st.expander("DOT source"):
            st.code(dot, language="dot")


# ═══════════════════════════════════════════════════════════════
# MAIN MRP FUNCTION
# ═══════════════════════════════════════════════════════════════
def run_mrp(export_file, stock_file, bom_file, receipt_file):
    logs   = []
    log    = lambda msg: logs.append(msg)
    status = st.status("Running MRP engine ...", expanded=True)

    # ── SECTION 1: BOM ────────────────────────────────────────
    with status:
        st.write("► Building clean BOM ...")

    bom = pd.read_excel(bom_file)
    bom.columns = bom.columns.str.strip()
    if "Alt." in bom.columns:
        bom = bom.rename(columns={"Alt.":"Alt"})

    bom["Level"] = pd.to_numeric(bom["Level"], errors="coerce").fillna(0).astype(int)
    bom = bom.reset_index(drop=True)

    parents, stack = [], {}
    for i in range(len(bom)):
        lvl    = bom.loc[i,"Level"]
        parent = bom.loc[i,"BOM Header"] if lvl==1 else stack.get(lvl-1)
        stack  = {k:v for k,v in stack.items() if k<=lvl}
        stack[lvl] = bom.loc[i,"Component"]
        parents.append(parent)
    bom["Parent"] = parents

    drop_cols = ["Plant","Usage","Quantity","Unit","BOM L/T","BOM code","Item",
                 "Mat. Group","Mat. Group Desc.","Pur. Group","Pur. Group Desc.",
                 "MRP Controller","MRP Controller Desc."]
    bom = bom.drop(columns=[c for c in drop_cols if c in bom.columns], errors="ignore")

    for old,new in [("Component description","Component descriptio"),
                    ("BOM header description","BOM header descripti")]:
        if old in bom.columns:
            bom = bom.rename(columns={old:new})

    keep = ["BOM Header","BOM header descripti","Alt","Level","Path","Parent",
            "Component","Component descriptio","Required Qty","Base unit",
            "Procurement type","Special procurement"]
    missing_bom = [c for c in ["BOM Header","Level","Component","Required Qty"]
                   if c not in bom.columns]
    if missing_bom:
        st.error(f"Missing required BOM columns: {missing_bom}")
        return None

    bom = bom[[c for c in keep if c in bom.columns]].copy()
    for col,default in [("Alt","0"),("Special procurement",""),
                        ("Procurement type",""),("Component descriptio","")]:
        if col not in bom.columns:
            bom[col] = default

    bom["Component"]            = bom["Component"].astype(str).str.strip()
    bom["BOM Header"]           = bom["BOM Header"].astype(str).str.strip()
    bom["Special procurement"]  = bom["Special procurement"].astype(str).str.strip()
    bom["Procurement type"]     = bom["Procurement type"].astype(str).str.strip()
    bom["Component descriptio"] = bom["Component descriptio"].astype(str).str.strip()
    bom["Required Qty"]         = pd.to_numeric(bom["Required Qty"], errors="coerce").fillna(0)
    bom["Alt"] = pd.to_numeric(bom["Alt"], errors="coerce").fillna(0).astype(int).astype(str)

    log(f"BOM rows: {len(bom):,}  |  Unique headers: {bom['BOM Header'].nunique()}")

    # ── SECTION 2: EXPORT FILE ────────────────────────────────
    with status:
        st.write("► Parsing Export file (DM00/EM00 demand + SF01 component orders) ...")

    try:
        fg_demand, prod_summary, date_cols = parse_export_file(export_file)
    except Exception as e:
        st.error(f"Export file parse error: {e}")
        return None

    if fg_demand.empty:
        st.error("No FG demand rows found. Export file must contain DM00 or EM00 "
                 "orders with FA-prefix Production Version.")
        return None

    # date_cols are string labels like '01-Apr-26'
    DATE_ORDER = {d:i for i,d in enumerate(date_cols)}

    log(f"FG demand rows: {len(fg_demand):,}  |  Unique FG: {fg_demand['BOM_Header'].nunique()}")
    log(f"Date columns: {date_cols}")
    log(f"Component prod orders (SF01): "
        f"{(prod_summary['Open_Production_Qty']>0).sum()} components")
    log(f"Component plan orders (LA): "
        f"{(prod_summary['Plan_Order_Qty']>0).sum()} components")

    # req_long in the format the MRP engine expects:
    # columns: BOM Header, Alt, Month (= Date_col label), FG_Demand
    req_long = (fg_demand
                .rename(columns={"BOM_Header":"BOM Header", "Date_col":"Month"})
                [["BOM Header","Alt","Month","FG_Demand"]]
                .copy())

    # ── SECTION 3: STOCK ─────────────────────────────────────
    with status:
        st.write("► Loading Stock ...")

    try:
        try:
            stock_raw = pd.read_excel(stock_file, sheet_name="Stock",
                                      usecols=[0,1], header=0,
                                      names=["Component","Stock_Qty"])
        except Exception:
            stock_file.seek(0)
            stock_raw = pd.read_excel(stock_file, sheet_name=0,
                                      usecols=[0,1], header=0,
                                      names=["Component","Stock_Qty"])
    except Exception as e:
        st.error(f"Stock file error: {e}")
        return None

    stock_raw = stock_raw.dropna(subset=["Component"]).copy()
    stock_raw["Component"] = stock_raw["Component"].astype(str).str.strip()
    stock_raw["Stock_Qty"] = pd.to_numeric(
        stock_raw["Stock_Qty"].astype(str).str.replace(",","",regex=False).str.strip(),
        errors="coerce").fillna(0)
    stock = stock_raw.groupby("Component")["Stock_Qty"].sum()
    log(f"Stock components: {len(stock):,}")

    # ── SECTION 4: RECEIPT QTY ────────────────────────────────
    receipt_qty = load_receipt_qty(receipt_file)
    receipt_added = 0
    if not receipt_qty.empty:
        for comp, qty in receipt_qty.items():
            stock[comp] = float(stock.get(comp, 0)) + float(qty)
            receipt_added += 1
        log(f"Receipt quantities applied for {receipt_added} components")

    # ── SECTION 5: MRP HELPERS ────────────────────────────────
    def get_sfrac(rows, comp_col, gross_col):
        agg = rows.groupby([comp_col,"Month","Month_Order"], as_index=False)[gross_col].sum()
        sfrac = {}
        for comp2, grp in agg.groupby(comp_col):
            avail = float(stock.get(comp2, 0))
            for _, row in grp.sort_values("Month_Order").iterrows():
                g = float(row[gross_col])
                # sfrac = fraction of demand NOT covered (0 when excess, >0 when shortage)
                sfrac[(comp2, row["Month"])] = max(0.0, g-avail)/g if g>0 else 0.0
                avail = max(0.0, avail-g)
        return sfrac

    def make_report(gross_agg_df, comp_col):
        BASE = ["Component","Description","Month","Gross_Requirement",
                "Stock_Used","Shortage","Stock_Remaining"]
        if gross_agg_df.empty:
            return pd.DataFrame(columns=BASE)
        results = []
        for comp2, grp in gross_agg_df.groupby(comp_col):
            avail = float(stock.get(comp2, 0))
            desc  = grp["Desc"].iloc[0]
            for _, row in grp.sort_values("Month_Order").iterrows():
                gr       = float(row["Gross"])
                consumed = min(avail, gr)
                # Positive = shortage, Negative = excess (surplus stock over demand)
                shortage = gr - avail
                avail    = max(0.0, avail-gr)
                results.append({
                    "Component":comp2, "Description":desc, "Month":row["Month"],
                    "Gross_Requirement":gr, "Stock_Used":consumed,
                    "Shortage":shortage, "Stock_Remaining":avail
                })
        return pd.DataFrame(results, columns=BASE)

    def apply_sfrac(df2, gross_col, ph_col, sfrac_dict, comp_col):
        return df2.apply(
            lambda r: r[gross_col] if is_phantom(r[ph_col])
                      else r[gross_col]*sfrac_dict.get((r[comp_col],r["Month"]),1.0),
            axis=1)

    # ── SECTION 6: MRP EXPLOSION L1 → L4 ─────────────────────
    with status:
        st.write("► Running MRP explosion ...")

    # LEVEL 1
    bom_l1 = (bom[bom["Level"]==1]
              [["BOM Header","Alt","Component","Component descriptio",
                "Required Qty","Special procurement"]].copy()
              .rename(columns={"Component":"L1_Comp","Component descriptio":"L1_Desc",
                                "Required Qty":"L1_Qty","Special procurement":"L1_Ph"}))
    l1 = req_long.merge(bom_l1, on=["BOM Header","Alt"], how="inner")
    l1["L1_Gross"]    = l1["FG_Demand"] * l1["L1_Qty"]
    l1["Month_Order"] = l1["Month"].map(DATE_ORDER)
    l1_norm  = l1[~l1["L1_Ph"].apply(is_phantom)].copy()
    l1_sfrac = get_sfrac(l1_norm, "L1_Comp", "L1_Gross")
    l1["L1_Eff"] = apply_sfrac(l1, "L1_Gross", "L1_Ph", l1_sfrac, "L1_Comp")
    l1_agg = (l1_norm.groupby(["L1_Comp","L1_Desc","Month","Month_Order"], as_index=False)
              ["L1_Gross"].sum()
              .rename(columns={"L1_Comp":"Component","L1_Desc":"Desc","L1_Gross":"Gross"}))
    result_l1 = make_report(l1_agg, "Component")

    # LEVEL 2
    bom_l2 = (bom[bom["Level"]==2]
              [["BOM Header","Alt","Parent","Component","Component descriptio",
                "Required Qty","Special procurement"]].copy()
              .rename(columns={"Parent":"L1_Comp","Component":"L2_Comp",
                                "Component descriptio":"L2_Desc","Required Qty":"L2_Qty",
                                "Special procurement":"L2_Ph"}))
    l2 = l1.merge(bom_l2, on=["BOM Header","Alt","L1_Comp"], how="inner")
    l2["L2_Gross"] = l2["L1_Eff"] * l2["L2_Qty"]
    l2_norm  = l2[~l2["L2_Ph"].apply(is_phantom)].copy()
    l2_sfrac = get_sfrac(l2_norm, "L2_Comp", "L2_Gross")
    l2["L2_Eff"] = apply_sfrac(l2, "L2_Gross", "L2_Ph", l2_sfrac, "L2_Comp")
    l2_agg = (l2_norm.groupby(["L2_Comp","L2_Desc","Month","Month_Order"], as_index=False)
              ["L2_Gross"].sum()
              .rename(columns={"L2_Comp":"Component","L2_Desc":"Desc","L2_Gross":"Gross"}))
    result_l2 = make_report(l2_agg, "Component")

    # LEVEL 3
    bom_l3 = (bom[bom["Level"]==3]
              [["BOM Header","Alt","Parent","Component","Component descriptio",
                "Required Qty","Special procurement"]].copy()
              .rename(columns={"Parent":"L2_Comp","Component":"L3_Comp",
                                "Component descriptio":"L3_Desc","Required Qty":"L3_Qty",
                                "Special procurement":"L3_Ph"}))
    l3 = l2.merge(bom_l3, on=["BOM Header","Alt","L2_Comp"], how="inner")
    l3["L3_Gross"] = l3.apply(
        lambda r: r["L2_Eff"] if is_phantom(r["L3_Ph"]) else r["L2_Eff"]*r["L3_Qty"], axis=1)
    l3_norm  = l3[~l3["L3_Ph"].apply(is_phantom)].copy()
    l3_sfrac = get_sfrac(l3_norm, "L3_Comp", "L3_Gross")
    l3["L3_Eff"] = apply_sfrac(l3, "L3_Gross", "L3_Ph", l3_sfrac, "L3_Comp")
    l3_agg = (l3_norm.groupby(["L3_Comp","L3_Desc","Month","Month_Order"], as_index=False)
              ["L3_Gross"].sum()
              .rename(columns={"L3_Comp":"Component","L3_Desc":"Desc","L3_Gross":"Gross"}))
    result_l3 = make_report(l3_agg, "Component")

    # LEVEL 4
    bom_l4 = (bom[bom["Level"]==4]
              [["BOM Header","Alt","Parent","Component","Component descriptio",
                "Required Qty","Special procurement"]].copy()
              .rename(columns={"Parent":"L3_Comp","Component":"L4_Comp",
                                "Component descriptio":"L4_Desc","Required Qty":"L4_Qty",
                                "Special procurement":"L4_Ph"}))
    l4 = l3.merge(bom_l4, on=["BOM Header","Alt","L3_Comp"], how="inner")
    l4["L4_Gross"] = l4["L3_Eff"] * l4["L4_Qty"]
    l4_agg = (l4.groupby(["L4_Comp","L4_Desc","Month","Month_Order"], as_index=False)
              ["L4_Gross"].sum()
              .rename(columns={"L4_Comp":"Component","L4_Desc":"Desc","L4_Gross":"Gross"}))
    result_l4 = make_report(l4_agg, "Component")

    with status:
        st.write("► Building output ...")
    status.update(label="MRP complete ✅", state="complete", expanded=False)

    # ── SECTION 7: SUMMARY ────────────────────────────────────
    st.divider()
    st.subheader("📊 Summary")

    if not receipt_qty.empty:
        st.info(f"ℹ️ Receipt quantities applied for {receipt_added} components.")

    c1,c2,c3,c4 = st.columns(4)
    for col_ui,lbl,dfr in zip([c1,c2,c3,c4],["L1","L2","L3","L4"],
                               [result_l1,result_l2,result_l3,result_l4]):
        shortage_comps = dfr[dfr["Shortage"]>0]["Component"].nunique() if not dfr.empty else 0
        excess_comps   = dfr[dfr["Shortage"]<0]["Component"].nunique() if not dfr.empty else 0
        with col_ui:
            st.metric(f"L{lbl[-1]} components", dfr["Component"].nunique() if not dfr.empty else 0)
            st.metric("🔴 Shortage", shortage_comps)
            st.metric("🟢 Excess",   excess_comps)

    # ── SECTION 8: EXPORT ─────────────────────────────────────
    final_output = pd.concat([result_l1,result_l2,result_l3,result_l4], ignore_index=True)
    all_comps = final_output[["Component","Description"]].drop_duplicates(subset="Component").copy()

    # Pivot: component × date (shortage per date)
    pivot = (final_output
             .pivot_table(index=["Component","Description"], columns="Month",
                          values="Shortage", aggfunc="sum", fill_value=0)
             .reset_index())
    pivot = all_comps.merge(pivot, on=["Component","Description"], how="left").fillna(0)

    # Only keep date columns present in data, in chronological order
    date_out_cols = [d for d in date_cols if d in pivot.columns]

    # Cumulative carry-forward:
    # Positive values = shortage, Negative values = excess (surplus stock over demand)
    # cumsum preserves sign — excess in early dates reduces later shortage correctly
    if date_out_cols:
        pivot[date_out_cols] = pivot[date_out_cols].cumsum(axis=1)

    bom_master = (bom[["Component","Procurement type","Special procurement"]]
                  .drop_duplicates(subset="Component"))
    stock_df   = stock.reset_index().rename(columns={"Stock_Qty":"Stock"})

    pivot = (pivot
             .merge(bom_master,   on="Component", how="left")
             .merge(stock_df,     on="Component", how="left")
             .merge(prod_summary, on="Component", how="left"))

    pivot["Procurement type"]    = pivot["Procurement type"].fillna("")
    pivot["Special procurement"] = pivot["Special procurement"].fillna("")
    pivot["Stock"]               = pivot["Stock"].fillna(0)
    pivot["Confirmed_Qty"]       = pivot["Confirmed_Qty"].fillna(0)
    pivot["Open_Production_Qty"] = pivot["Open_Production_Qty"].fillna(0)
    pivot["Plan_Order_Qty"]      = pivot["Plan_Order_Qty"].fillna(0)

    if not receipt_qty.empty:
        rq_df = receipt_qty.reset_index()
        rq_df.columns = ["Component","Receipt_Qty"]
        pivot = pivot.merge(rq_df, on="Component", how="left")
        pivot["Receipt_Qty"] = pivot["Receipt_Qty"].fillna(0)
        extra_cols = ["Receipt_Qty"]
    else:
        extra_cols = []

    pivot = pivot.rename(columns={"Description":"Component descri"})
    final_cols = (
        ["Component","Component descri","Procurement type","Special procurement",
         "Confirmed_Qty","Open_Production_Qty","Plan_Order_Qty","Stock"]
        + extra_cols + date_out_cols
    )
    for c in final_cols:
        if c not in pivot.columns:
            num_set = set(date_out_cols + ["Confirmed_Qty","Open_Production_Qty",
                                            "Plan_Order_Qty","Stock","Receipt_Qty"])
            pivot[c] = 0 if c in num_set else ""

    pivot = pivot[final_cols].sort_values("Component").reset_index(drop=True)

    st.divider()
    st.subheader("📋 Output preview")
    st.dataframe(pivot.head(200), use_container_width=True)
    st.caption(f"{len(pivot):,} rows · {len(date_out_cols)} date columns · "
               f"positive = shortage · negative = excess stock")

    buf = io.BytesIO()
    pivot.to_excel(buf, index=False, engine="openpyxl")
    buf.seek(0)
    st.download_button(
        "⬇️ Download mrp_final.xlsx", data=buf, file_name="mrp_final.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True, type="primary"
    )

    with st.expander("Run log"):
        for line in logs:
            st.text(line)

    return dict(
        bom=bom, fg_demand=fg_demand, date_cols=date_cols,
        stock=stock, prod_summary=prod_summary,
        result_l1=result_l1, result_l2=result_l2,
        result_l3=result_l3, result_l4=result_l4,
    )


# ═══════════════════════════════════════════════════════════════
# SESSION STATE + ENTRY POINT
# ═══════════════════════════════════════════════════════════════
if "mrp_results" not in st.session_state:
    st.session_state["mrp_results"] = None

if not run_btn and export_file is None:
    st.info("Upload your files in the sidebar, then click **▶ Run MRP**.")
elif run_btn:
    if export_file is None or stock_file is None or bom_file is None:
        st.warning("Export file, Stock file and BOM file are all mandatory.")
    else:
        try:
            results = run_mrp(export_file, stock_file, bom_file, receipt_file)
            if results is not None:
                st.session_state["mrp_results"] = results
        except Exception as e:
            st.exception(e)

if st.session_state["mrp_results"] is not None:
    r = st.session_state["mrp_results"]
    try:
        show_search_section(
            bom=r["bom"], fg_demand_df=r["fg_demand"],
            date_cols=r["date_cols"], stock=r["stock"],
            prod_summary=r["prod_summary"]
        )
    except Exception as e:
        st.error(f"Search error: {e}")
