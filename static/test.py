@app.get("/api/daily_sales")
def daily_sales():
    f = parse_filters(request)
    value = "qty" if f["metric"] == "qty" else "amt"

    # 0 or missing = no top filter
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

            if not top_sold_to:
                # no matching customers – all days = 0
                return jsonify([{"day": d, "value": 0} for d in range(1, 31)])

        # 2) Daily totals, optionally restricted to top N sold_to
        wh2 = list(wh)
        params2 = list(params)

        if top_sold_to:
            placeholders = ",".join(["%s"] * len(top_sold_to))
            wh2.append(f"s.sold_to IN ({placeholders})")
            params2.extend(top_sold_to)

        where_sql2 = ("WHERE " + " AND ".join(wh2)) if wh2 else ""

        daily_sql = f"""
          SELECT s.day AS day_num, SUM(s.{value}) AS daily_total
            FROM sales_2511 s
            {' '.join(joins)}
            {where_sql2}
           GROUP BY s.day
           ORDER BY s.day
        """
        cur.execute(daily_sql, tuple(params2))
        rows = cur.fetchall()

    finally:
        cur.close()
        conn.close()

    day_map = {int(r["day_num"]): float(r["daily_total"] or 0) for r in rows}
    return jsonify([{"day": d, "value": day_map.get(d, 0)} for d in range(1, 31)])

#