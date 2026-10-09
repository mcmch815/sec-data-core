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

from flask import Flask, jsonify, request, redirect, url_for

from sec_core.paths import MART_DB_PATH

app = Flask(__name__)

_MART_PATH = Path(os.environ.get("MART_DB", MART_DB_PATH))


def get_conn():
    return sqlite3.connect(f"file:{_MART_PATH}?mode=ro", uri=True,
                           check_same_thread=False)


def fmt_value(value, uom):
    if value is None:
        return ""
    if uom == "USD":
        return f"${value:,.0f}"
    return f"{value:,.4g}"


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
    select, input[type=text] {{ padding: 0.4rem 0.6rem; border: 1px solid #ccc; border-radius: 4px;
                     font-size: 0.95rem; min-width: 320px; }}
    button {{ padding: 0.45rem 1.2rem; background: #1a1a1a; color: #fff; border: none;
              border-radius: 4px; cursor: pointer; font-size: 0.95rem; }}
    button:hover {{ background: #333; }}
    .search-wrap {{ position: relative; display: inline-block; }}
    .search-wrap input {{ width: 360px; box-sizing: border-box; }}
    .ac-dropdown {{ position: absolute; top: 100%; left: 0; right: 0; background: #fff;
                    border: 1px solid #ccc; border-top: none; border-radius: 0 0 4px 4px;
                    max-height: 320px; overflow-y: auto; z-index: 100; box-shadow: 0 4px 8px rgba(0,0,0,0.1); }}
    .ac-item {{ padding: 0.45rem 0.7rem; cursor: pointer; font-size: 0.9rem; border-bottom: 1px solid #f0f0f0; }}
    .ac-item:last-child {{ border-bottom: none; }}
    .ac-item:hover, .ac-item.selected {{ background: #f0f4ff; }}
    .ac-item .cik {{ color: #888; font-size: 0.8rem; margin-left: 0.5rem; }}
    .ac-hint {{ padding: 0.45rem 0.7rem; color: #aaa; font-size: 0.85rem; font-style: italic; }}
    .tbl-wrap {{ overflow-x: auto; }}
    table {{ border-collapse: collapse; width: 100%; font-size: 0.85rem; white-space: nowrap; }}
    th {{ text-align: left; padding: 0.45rem 0.65rem; background: #f4f4f4;
          border-bottom: 2px solid #ddd; position: sticky; top: 0; }}
    td {{ padding: 0.35rem 0.65rem; border-bottom: 1px solid #eee; }}
    td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    tr.inpth td {{ color: #666; font-style: italic; }}
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
    .badge {{ display: inline-block; padding: 0.1rem 0.4rem; border-radius: 3px;
              font-size: 0.75rem; font-weight: 600; }}
    .badge-C {{ background: #e8f5e9; color: #2e7d32; }}
    .badge-D {{ background: #fff3e0; color: #e65100; }}
    .badge-I {{ background: #e3f2fd; color: #1565c0; }}
    .badge-dur {{ background: #f3e5f5; color: #6a1b9a; }}
  </style>
</head>
<body>
{body}
</body>
</html>"""


def page(body):
    return _BASE.format(body=body)


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

@app.route("/api/search")
def api_search():
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify(results=[])
    pattern = f"%{q}%"
    conn = get_conn()
    rows = conn.execute(
        "SELECT cik, name FROM companies WHERE name LIKE ? OR cik LIKE ? ORDER BY name LIMIT 30",
        (pattern, pattern)
    ).fetchall()
    conn.close()
    return jsonify(results=[{"cik": r[0], "name": r[1]} for r in rows])


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    conn = get_conn()
    count = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    conn.close()

    body = f"""
    <h1>SEC Financial Statement Viewer</h1>
    <p class="sub">{count:,} companies &mdash; {_MART_PATH.name}</p>
    <form action="/company" method="get" onsubmit="return false;">
      <label>Company (name or CIK)
        <div class="search-wrap">
          <input type="text" id="company-search" autocomplete="off"
                 placeholder="Start typing a name or CIK…" />
          <div class="ac-dropdown" id="ac-dropdown" style="display:none"></div>
        </div>
      </label>
    </form>
    <script>
    (function() {{
      const input = document.getElementById('company-search');
      const dropdown = document.getElementById('ac-dropdown');
      let timer, selected = -1, items = [];

      input.addEventListener('input', function() {{
        clearTimeout(timer);
        const q = input.value.trim();
        if (q.length < 2) {{ dropdown.style.display = 'none'; return; }}
        timer = setTimeout(() => fetchResults(q), 220);
      }});

      input.addEventListener('keydown', function(e) {{
        if (!items.length) return;
        if (e.key === 'ArrowDown') {{ e.preventDefault(); setSelected(selected + 1); }}
        else if (e.key === 'ArrowUp') {{ e.preventDefault(); setSelected(selected - 1); }}
        else if (e.key === 'Enter') {{ e.preventDefault(); if (selected >= 0) navigate(items[selected].cik); }}
        else if (e.key === 'Escape') {{ dropdown.style.display = 'none'; }}
      }});

      document.addEventListener('click', function(e) {{
        if (!input.contains(e.target) && !dropdown.contains(e.target))
          dropdown.style.display = 'none';
      }});

      function fetchResults(q) {{
        fetch('/api/search?q=' + encodeURIComponent(q))
          .then(r => r.json())
          .then(data => {{
            items = data.results;
            selected = -1;
            renderDropdown();
          }});
      }}

      function renderDropdown() {{
        if (!items.length) {{
          dropdown.innerHTML = '<div class="ac-hint">No matches</div>';
          dropdown.style.display = 'block';
          return;
        }}
        dropdown.innerHTML = items.map((it, i) =>
          `<div class="ac-item" data-cik="${{it.cik}}" data-idx="${{i}}">
             ${{it.name}}<span class="cik">${{it.cik}}</span>
           </div>`
        ).join('');
        dropdown.querySelectorAll('.ac-item').forEach(el => {{
          el.addEventListener('mousedown', function(e) {{
            e.preventDefault();
            navigate(el.dataset.cik);
          }});
          el.addEventListener('mouseover', function() {{
            setSelected(parseInt(el.dataset.idx));
          }});
        }});
        dropdown.style.display = 'block';
      }}

      function setSelected(idx) {{
        const els = dropdown.querySelectorAll('.ac-item');
        if (!els.length) return;
        idx = Math.max(0, Math.min(idx, els.length - 1));
        selected = idx;
        els.forEach((el, i) => el.classList.toggle('selected', i === idx));
        els[idx].scrollIntoView({{ block: 'nearest' }});
      }}

      function navigate(cik) {{
        window.location.href = '/company?cik=' + encodeURIComponent(cik);
      }}
    }})();
    </script>
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
        SELECT tag, label, plabel, report, line, qtrs,
               version, uom, value, display_value,
               crdr, iord, datatype, negating, inpth
        FROM facts
        WHERE cik=? AND ddate=? AND stmt=?
        ORDER BY report, line
    """, (cik, ddate, stmt)).fetchall()

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
        rows_html = '<tr><td colspan="15" class="empty">No data for this statement.</td></tr>'
    else:
        rows_html = ""
        for (tag, label, plabel, report, line, qtrs,
             version, uom, value, display_value,
             crdr, iord, datatype, negating, inpth) in facts:

            display_label = plabel or label or tag
            fmt_val = fmt_value(display_value, uom)
            uom_badge = f'<span class="uom">{uom}</span>' if uom and uom != "USD" else ""
            inpth_class = ' class="inpth"' if inpth == "1" else ""

            crdr_badge = (f'<span class="badge badge-{crdr}">{crdr}</span>' if crdr else "")
            iord_label = {"I": "instant", "D": "duration"}.get(iord, iord or "")
            iord_badge = (f'<span class="badge badge-{"I" if iord == "I" else "dur"}">{iord_label}</span>'
                          if iord else "")

            rows_html += (
                f"<tr{inpth_class}>"
                f"<td>{display_label}</td>"
                f'<td class="num">{fmt_val}{uom_badge}</td>'
                f"<td>{tag}</td>"
                f"<td>{label or ''}</td>"
                f'<td class="num">{qtrs if qtrs is not None else ""}</td>'
                f'<td class="num">{report}</td>'
                f'<td class="num">{line}</td>'
                f"<td>{version or ''}</td>"
                f"<td>{uom or ''}</td>"
                f"<td>{crdr_badge}</td>"
                f"<td>{iord_badge}</td>"
                f"<td>{datatype or ''}</td>"
                f'<td class="num">{"1" if negating == "1" else ""}</td>'
                f'<td class="num">{"1" if inpth == "1" else ""}</td>'
                f'<td class="num">{display_value if display_value is not None else ""}</td>'
                f'<td class="num">{value if value is not None else ""}</td>'
                f"</tr>"
            )

    body = f"""
    <div class="back"><a href="/">← All companies</a> / <a href="/company?cik={cik}">{company_name}</a></div>
    <h1>{company_name} &mdash; FY {fy}</h1>
    <p class="sub">CIK: {cik} &nbsp;|&nbsp; Period end: {ddate}</p>
    <div class="stmt-tabs">{tabs_html}</div>
    <div class="tbl-wrap">
    <table>
      <thead><tr>
        <th>Line Item</th><th>Value</th><th>Tag</th><th>Label</th>
        <th>qtrs</th><th>report</th><th>line</th><th>version</th>
        <th>uom</th><th>crdr</th><th>iord</th><th>datatype</th>
        <th>neg</th><th>inpth</th><th>display_value</th><th>value</th>
      </tr></thead>
      <tbody>{rows_html}</tbody>
    </table>
    </div>
    """
    return page(body)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print(f"DB: {_MART_PATH}")
    print("Open: http://localhost:5050")
    app.run(host="0.0.0.0", port=5050, debug=True)
