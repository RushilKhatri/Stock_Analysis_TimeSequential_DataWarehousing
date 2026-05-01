import os
from typing import Dict, List

from flask import Flask, jsonify, render_template, request
import mysql.connector

from config_reader import PipelineConfig

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(BASE_DIR)

pc = PipelineConfig.load('pipeline_config.xml')

LOC_DIMENSIONS: Dict[str, List[str]] = {
    'loc_apex': [],
    'loc_company': ['company_name'],
    'loc_sector': ['sector'],
    'loc_industry': ['industry'],
    'loc_company_sector': ['company_name', 'sector'],
    'loc_company_industry': ['company_name', 'industry'],
    'loc_sector_industry': ['sector', 'industry'],
    'loc_company_sector_industry': ['company_name', 'sector', 'industry'],
}

LOC_METRICS: Dict[str, Dict[str, str]] = {
    'volume_sum': {'label': 'Volume Sum', 'expr': 'SUM(f.volume)'},
    'volume_avg': {'label': 'Volume Avg', 'expr': 'AVG(f.volume)'},
    'sma_30_sum': {'label': 'SMA 30 Sum', 'expr': 'SUM(f.sma_20)'},
    'sma_30_avg': {'label': 'SMA 30 Avg', 'expr': 'AVG(f.sma_20)'},
}


def _db_password() -> str:
    env_name = (pc.db_password_env or '').strip()
    if not env_name:
        return ''
    return os.environ.get(env_name, env_name)


def get_conn():
    return mysql.connector.connect(
        host=pc.db_host,
        port=pc.db_port,
        user=pc.db_user,
        password=_db_password(),
        database=pc.db_name,
    )


app = Flask(__name__)


@app.get('/')
def index():
    return render_template('index.html')


@app.get('/api/meta')
def api_meta():
    return jsonify({
        'tables': list(LOC_DIMENSIONS.keys()),
        'dimensions': LOC_DIMENSIONS,
        'metrics': LOC_METRICS,
    })


@app.get('/api/filter-options')
def api_filter_options():
    table = request.args.get('table', 'loc_apex')
    dims = LOC_DIMENSIONS.get(table, [])

    selected = {dim: request.args.get(dim, '').strip() for dim in dims}
    options = {}

    if not dims:
        return jsonify({'options': options})

    conn = get_conn()
    cur = conn.cursor()
    try:
        for dim in dims:
            where = []
            params = []
            for other_dim in dims:
                val = selected.get(other_dim, '')
                if other_dim == dim:
                    continue
                if val:
                    where.append(f"{other_dim} = %s")
                    params.append(val)

            sql = f"SELECT DISTINCT {dim} FROM dim_stock WHERE is_current = 1"
            if where:
                sql += " AND " + " AND ".join(where)
            sql += f" AND {dim} IS NOT NULL AND {dim} <> '' ORDER BY {dim}"

            cur.execute(sql, tuple(params))
            options[dim] = [row[0] for row in cur.fetchall()]

        return jsonify({'options': options})
    finally:
        cur.close()
        conn.close()


@app.get('/api/series')
def api_series():
    table = request.args.get('table', 'loc_apex')
    dims = LOC_DIMENSIONS.get(table, [])
    metric = request.args.get('metric', 'volume_avg')
    metric_info = LOC_METRICS.get(metric, LOC_METRICS['volume_avg'])

    filters = {}
    for dim in dims:
        val = request.args.get(dim, '').strip()
        if val:
            filters[dim] = val

    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    try:
        sql = """
            SELECT
                dt.trade_date AS trade_date,
                dt.trade_time AS trade_time,
                {metric_expr} AS metric_value
            FROM fact_stock_bar f
            JOIN dim_time dt ON dt.time_key = f.time_key
        """.format(metric_expr=metric_info['expr'])
        params = []

        if dims:
            sql += " JOIN dim_stock ds ON ds.stock_key = f.stock_key"

        where = []
        for dim, val in filters.items():
            where.append(f"ds.{dim} = %s")
            params.append(val)

        if where:
            sql += " WHERE " + " AND ".join(where)

        sql += """
            GROUP BY dt.time_key, dt.trade_date, dt.trade_time
            ORDER BY dt.time_key
        """

        cur.execute(sql, tuple(params))
        rows = cur.fetchall()

        data = []
        for r in rows:
            ts_iso = f"{r['trade_date']}T{r['trade_time']}"
            data.append({
                'ts': ts_iso,
                'metric_value': float(r['metric_value'] or 0),
            })

        return jsonify({'rows': data, 'metric': metric, 'metric_label': metric_info['label']})
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    port = int(os.environ.get('FRONTEND_PORT', '5050'))
    app.run(host='127.0.0.1', port=port, debug=True)
