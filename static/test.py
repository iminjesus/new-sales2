try:
        top_sold_to = None

        # 1) Get Top N sold_to from baseline table (sales_25_2601)
        if top_limit > 0:
            top_sold_to = get_top_sold_to_from_baseline(
                cur, f, top_limit, value
            )                                               # CHANGED

            if not top_sold_to:
                # no matching customers – all days = 0
                return jsonify([{"day": d, "value": 0} for d in range(1, 32)])

        # 2) Daily totals, optionally restricted to the baseline top sold_to set
        wh2 = list(wh)
        params2 = list(params)

        if top_sold_to:
            placeholders = ",".join(["%s"] * len(top_sold_to))
            wh2.append(f"s.sold_to IN ({placeholders})")
            params2.extend(top_sold_to)

        where_sql2 = ("WHERE " + " AND ".join(wh2)) if wh2 else ""

        daily_sql = f"""
        SELECT s.day AS day_num, SUM(s.{value}) AS daily_total
            FROM sales_2601 s
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
    return jsonify([{"day": d, "value": day_map.get(d, 0)} for d in range(1, 32)])