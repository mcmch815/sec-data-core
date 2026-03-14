"""SEC Financial Statement Viewer.

Displays financial statements from sec_annual.db (or test_annual.db).

Usage:
    # Against test DB:
    MART_DB=/path/to/test_annual.db conda run -n tf python viewer/app.py

    # Against production DB (default):
    conda run -n tf python viewer/app.py
"""

import os
import sqlite3
from pathlib import Path

from flask import Flask, request, redirect, url_for

from sec_core.paths import MART_DB_PATH

app = Flask(__name__)

_MART_PATH = Path(os.environ.get("MART_DB", MART_DB_PATH))


def get_conn():
    return sqlite3.connect(f"file:{_MART_PATH}?mode=ro", uri=True,
                           check_same_thread=False)


def fmt_value(value, uom, negating):
    if value is None:
        return ""
    v = -value if negating == "1" else value
    if uom == "USD":
        return f"${v:,.0f}"
    return f"{v:,.4g}"


# ---------------------------------------------------------------------------
# HTML helpers
# ---------------------------------------------------------------------------

_BASE = """<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>SEC Viewer</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 2rem; color: #1a1a1a; }}
    h1 {{ font-size: 1.4rem; margin-bottom: 0.25rem; }}
    .sub {{ color: #555; font-size: 0.9rem; margin-bottom: 1.5rem; }}
    form {{ display: flex; gap: 1rem; align-items: flex-end; flex-wrap: wrap; margin-bottom: 2rem; }}
    label {{ display: flex; flex-direction: column; gap: 0.25rem; font-size: 0.85rem; color: #444; }}
    select, input {{ padding: 0.4rem 0.6rem; border: 1px solid #ccc; border-radius: 4px;
                     font-size: 0.95rem; min-width: 160px; }}
    button {{ padding: 0.45rem 1.2rem; background: #1a1a1a; color: #fff; border: none;
              border-radius: 4px; cursor: pointer; font-size: 0.95rem; }}
    button:hover {{ background: #333; }}
    table {{ border-collapse: collapse; width: 100%; max-width: 900px; font-size: 0.9rem; }}
    th {{ text-align: left; padding: 0.5rem 0.75rem; background: #f4f4f4;
          border-bottom: 2px solid #ddd; white-space: nowrap; }}
    td {{ padding: 0.4rem 0.75rem; border-bottom: 1px solid #eee; }}
    td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    tr.inpth td {{ color: #555; font-style: italic; }}
    tr:hover td {{ background: #fafafa; }}
    .back {{ font-size: 0.85rem; margin-bottom: 1rem; }}
    .back a {{ color: #555; text-decoration: none; }}
    .back a:hover {{ text-decoration: underline; }}
    .stmt-tabs {{ display: flex; gap: 0.5rem; margin-bottom: 1.5rem; }}
    .stmt-tab {{ padding: 0.35rem 0.9rem; border: 1px solid #ccc; border-radius: 4px;
                 text-decoration: none; color: #333; font-size: 0.9rem; }}
    .stmt-tab.active {{ background: #1a1a1a; color: #fff; border-color: #1a1a1a; }}
    .uom {{ color: #888; font-size: 0.78rem; margin-left: 0.3rem; }}
    .empty {{ color: #888; font-style: italic; }}
  </style>
</head>
<body>
{body}
</body>
</html>"""


def page(body):
    return _BASE.format(body=body)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    conn = get_conn()
    companies = conn.execute(
        "SELECT cik, name FROM companies ORDER BY name"
    ).fetchall()
    conn.close()

    options = "\n".join(
        f'<option value="{cik}">{name} ({cik})</option>'
        for cik, name in companies
    )
    body = f"""
    <h1>SEC Financial Statement Viewer</h1>
    <p class="sub">{len(companies):,} companies &mdash; {_MART_PATH.name}</p>
    <form action="/company" method="get">
      <label>Company
        <select name="cik">
          <option value="">— select —</option>
          {options}
        </select>
      </label>
      <button type="submit">View</button>
    </form>
    """
    return page(body)


@app.route("/company")
def company():
    cik = request.args.get("cik", "").strip()
    if not cik:
        return redirect(url_for("index"))

    conn = get_conn()
    name_row = conn.execute(
        "SELECT name FROM companies WHERE cik=?", (cik,)
    ).fetchone()
    if not name_row:
        conn.close()
        return page(f'<p>Company {cik} not found. <a href="/">Back</a></p>')

    periods = conn.execute(
        "SELECT DISTINCT p.fy, p.ddate, f.stmt "
        "FROM periods p JOIN facts f ON p.cik=f.cik AND p.ddate=f.ddate "
        "WHERE p.cik=? ORDER BY p.ddate DESC, f.stmt",
        (cik,)
    ).fetchall()
    conn.close()

    # Group by fy
    by_fy = {}
    for fy, ddate, stmt in periods:
        by_fy.setdefault((fy, ddate), set()).add(stmt)

    rows_html = ""
    for (fy, ddate), stmts in sorted(by_fy.items(), key=lambda x: x[0][1], reverse=True):
        stmt_links = " ".join(
            f'<a href="/statement?cik={cik}&ddate={ddate}&stmt={s}">{s}</a>'
            for s in sorted(stmts)
        )
        rows_html += f"<tr><td>FY {fy}</td><td>{ddate}</td><td>{stmt_links}</td></tr>"

    body = f"""
    <div class="back"><a href="/">← All companies</a></div>
    <h1>{name_row[0]}</h1>
    <p class="sub">CIK: {cik}</p>
    <table>
      <thead><tr><th>Fiscal Year</th><th>Period End</th><th>Statements</th></tr></thead>
      <tbody>{rows_html}</tbody>
    </table>
    """
    return page(body)


@app.route("/statement")
def statement():
    cik   = request.args.get("cik", "").strip()
    ddate = request.args.get("ddate", "").strip()
    stmt  = request.args.get("stmt", "IS").strip().upper()

    if not cik or not ddate:
        return redirect(url_for("index"))

    conn = get_conn()

    name_row = conn.execute(
        "SELECT name FROM companies WHERE cik=?", (cik,)
    ).fetchone()
    fy_row = conn.execute(
        "SELECT fy FROM periods WHERE cik=? AND ddate=?", (cik, ddate)
    ).fetchone()

    facts = conn.execute("""
        SELECT tag, plabel, label, report, line, value, uom, negating, inpth
        FROM facts
        WHERE cik=? AND ddate=? AND stmt=?
        ORDER BY report, line
    """, (cik, ddate, stmt)).fetchall()

    # Available stmts for this period (for tab switching)
    avail_stmts = [r[0] for r in conn.execute(
        "SELECT DISTINCT stmt FROM facts WHERE cik=? AND ddate=? ORDER BY stmt",
        (cik, ddate)
    ).fetchall()]
    conn.close()

    company_name = name_row[0] if name_row else cik
    fy = fy_row[0] if fy_row else ddate[:4]

    stmt_labels = {"IS": "Income Statement", "BS": "Balance Sheet", "CF": "Cash Flow"}

    tabs_html = "".join(
        f'<a class="stmt-tab{"  active" if s == stmt else ""}" '
        f'href="/statement?cik={cik}&ddate={ddate}&stmt={s}">{s} — {stmt_labels.get(s, s)}</a>'
        for s in avail_stmts
    )

    if not facts:
        rows_html = '<tr><td colspan="3" class="empty">No data for this statement.</td></tr>'
    else:
        rows_html = ""
        for tag, plabel, label, report, line, value, uom, negating, inpth in facts:
            display_label = plabel or label or tag
            display_val = fmt_value(value, uom, negating)
            uom_badge = f'<span class="uom">{uom}</span>' if uom and uom != "USD" else ""
            inpth_class = ' class="inpth"' if inpth == "1" else ""
            rows_html += (
                f"<tr{inpth_class}>"
                f"<td>{display_label}{uom_badge}</td>"
                f'<td class="num">{display_val}</td>'
                f"<td>{tag}</td>"
                f"</tr>"
            )

    body = f"""
    <div class="back"><a href="/">← All companies</a> / <a href="/company?cik={cik}">{company_name}</a></div>
    <h1>{company_name} &mdash; FY {fy}</h1>
    <p class="sub">CIK: {cik} &nbsp;|&nbsp; Period end: {ddate}</p>
    <div class="stmt-tabs">{tabs_html}</div>
    <table>
      <thead><tr><th>Line Item</th><th style="text-align:right">Value</th><th>Tag</th></tr></thead>
      <tbody>{rows_html}</tbody>
    </table>
    """
    return page(body)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import socket
    host_ip = socket.gethostbyname(socket.gethostname())
    print(f"DB: {_MART_PATH}")
    print(f"Open: http://{host_ip}:5050")
    app.run(host="0.0.0.0", port=5050, debug=True)
