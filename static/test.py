@app.get("/api/monthly_sales")
def monthly_sales():
    f = parse_filters(request)
    value = "qty" if f["metric"] == "qty" else "amt"

    # 0 or missing = no top filter, same behaviour as before
    top_limit = int(request.args.get("top_limit", 0) or 0)

    joins, wh, params = build_customer_filters("s", f, use_sold_to_name=False)

    # category
    cat_joins, cat_where = category_filters("s", f["category"])
    joins += cat_joins
    wh    += cat_where

    # direct fields (indexable)
    if f["product_group"] != "ALL":
        wh.append("s.product_group = %s")
        params.append(f["product_group"])
    if f["pattern"] != "ALL":
        wh.append("s.pattern = %s")
        params.append(f["pattern"])

    base_where_sql = ("WHERE " + " AND ".join(wh)) if wh else ""

    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    try:
        top_sold_to = None

        # 1) If top_limit > 0, get top N sold_to first
        if top_limit > 0:
            top_sql = f"""
              SELECT s.sold_to AS sold_to
                FROM sales_2501_11 s
                {' '.join(joins)}
                {base_where_sql}
               GROUP BY s.sold_to
               ORDER BY SUM(s.{value}) DESC
               LIMIT %s
            """
            cur.execute(top_sql, tuple(params) + (top_limit,))
            top_rows = cur.fetchall()
            top_sold_to = [r["sold_to"] for r in top_rows]

            # If nothing found, just return zeros for all 12 months
            if not top_sold_to:
                return jsonify([{"month": m, "value": 0} for m in range(1, 12)])

        # 2) Monthly totals, optionally restricted to the top N sold_to
        wh2 = list(wh)
        params2 = list(params)

        if top_sold_to:
            placeholders = ",".join(["%s"] * len(top_sold_to))
            wh2.append(f"s.sold_to IN ({placeholders})")
            params2.extend(top_sold_to)

        where_sql2 = ("WHERE " + " AND ".join(wh2)) if wh2 else ""

        monthly_sql = f"""
          SELECT s.month AS month_num, SUM(s.{value}) AS monthly_total
            FROM sales_2501_11 s
            {' '.join(joins)}
            {where_sql2}
           GROUP BY s.month
           ORDER BY s.month
        """
        cur.execute(monthly_sql, tuple(params2))
        rows = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    month_map = {int(r["month_num"]): float(r["monthly_total"] or 0) for r in rows}
    return jsonify([{"month": m, "value": month_map.get(m, 0)} for m in range(1, 12)])