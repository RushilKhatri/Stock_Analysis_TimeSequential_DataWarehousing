import os
import sys
import math
import time
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from collections import deque
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np

# ── MySQL connector ─────────────────────────────────────────
try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
except ImportError:
    print("ERROR: mysql-connector-python not installed.")
    print("Run:  pip install mysql-connector-python")
    sys.exit(1)

# ── Import your existing config_reader ──────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config_reader import WindowConfig, SchemaConfig, QueryConfig, PipelineConfig

# ═══════════════════════════════════════════════════════════
# LOGGING SETUP
# ═══════════════════════════════════════════════════════════
logging.basicConfig(
    level   = logging.INFO,
    format  = '%(asctime)s  %(levelname)-8s  %(message)s',
    datefmt = '%H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('pipeline.log', mode='w'),
    ]
)
log = logging.getLogger('ETL')


# ═══════════════════════════════════════════════════════════
# STEP 1 ── DATABASE CONNECTION
# ═══════════════════════════════════════════════════════════

class DBConnection:

    def __init__(self, pc: PipelineConfig):
        self.pc     = pc
        self.conn   = None
        self.cursor = None

    # ── Connect (create DB if it does not exist) ─────────
    def connect(self):
        env_name = (self.pc.db_password_env or '').strip()
        password = os.environ.get(env_name, '') if env_name else ''
        if not password:
            # Backward-compatible fallback: if XML contains a literal password
            # instead of an env-var name, use it directly.
            if env_name:
                password = env_name
                log.warning(
                    f"Environment variable '{env_name}' not found. "
                    f"Using value from pipeline_config.xml as direct password."
                )
            else:
                log.warning(
                    "No database password configured. Trying passwordless connection."
                )

        # First connect WITHOUT specifying a database so we can create it
        log.info(f"Connecting to MySQL at {self.pc.db_host}:{self.pc.db_port} "
                 f"as '{self.pc.db_user}'...")
        try:
            self.conn = mysql.connector.connect(
                host     = self.pc.db_host,
                port     = self.pc.db_port,
                user     = self.pc.db_user,
                password = password,
            )
            self.cursor = self.conn.cursor(buffered=True)

            # Create the database if it does not exist
            self.cursor.execute(
                f"CREATE DATABASE IF NOT EXISTS `{self.pc.db_name}` "
                f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
            )
            self.cursor.execute(f"USE `{self.pc.db_name}`;")
            self.conn.commit()
            log.info(f"Connected. Using database '{self.pc.db_name}'.")

        except MySQLError as e:
            log.error(f"MySQL connection failed: {e}")
            log.error("Check: host, port, user, password, and that MySQL is running.")
            sys.exit(1)

    # ── Execute a single SQL statement ────────────────────
    def execute(self, sql: str, params: tuple = ()):
        try:
            self.cursor.execute(sql, params)
        except MySQLError as e:
            log.error(f"SQL error: {e}\nSQL: {sql[:200]}")
            raise

    # ── Execute many rows at once (bulk insert) ───────────
    def executemany(self, sql: str, rows: List[tuple]):
        try:
            self.cursor.executemany(sql, rows)
        except MySQLError as e:
            log.error(f"Bulk insert error: {e}")
            raise

    # ── Run a SELECT and return all rows ──────────────────
    def fetchall(self, sql: str, params: tuple = ()) -> List[tuple]:
        self.cursor.execute(sql, params)
        return self.cursor.fetchall()

    def commit(self):
        self.conn.commit()

    def begin(self):
        try:
            if getattr(self.conn, 'in_transaction', False):
                return
            self.conn.start_transaction()
        except Exception as e:
            # MySQL connector can report an already-active transaction in some flows.
            if 'Transaction already in progress' in str(e):
                return
            raise

    def rollback(self):
        self.conn.rollback()

    def execute_script(self, sql_script: str):
        stmt = []
        in_block_comment = False

        for raw_line in sql_script.splitlines():
            line = raw_line.strip()

            if in_block_comment:
                if '*/' in line:
                    in_block_comment = False
                continue

            if not line:
                continue

            if line.startswith('/*'):
                if '*/' not in line:
                    in_block_comment = True
                continue

            if line.startswith('--'):
                continue

            stmt.append(raw_line)
            if ';' in raw_line:
                candidate = '\n'.join(stmt).strip()
                if candidate:
                    self.cursor.execute(candidate)
                stmt = []

        trailing = '\n'.join(stmt).strip()
        if trailing:
            self.cursor.execute(trailing)

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn and self.conn.is_connected():
            self.conn.close()
            log.info("MySQL connection closed.")


# ═══════════════════════════════════════════════════════════
# STEP 2 ── CREATE ALL TABLES FROM schema_config.xml
# ═══════════════════════════════════════════════════════════

class SchemaManager:
    # MySQL does not support BIGSERIAL or NUMERIC — map them
    TYPE_MAP = {
        'BIGSERIAL':   'BIGINT AUTO_INCREMENT',
        'SERIAL':      'INT AUTO_INCREMENT',
        'BOOLEAN':     'TINYINT(1)',
        'TEXT':        'TEXT',
    }

    def __init__(self, db: DBConnection, sc: SchemaConfig):
        self.db = db
        self.sc = sc

    def _map_type(self, dtype: str) -> str:
        upper = dtype.upper()
        if upper in self.TYPE_MAP:
            return self.TYPE_MAP[upper]
        if upper.startswith('NUMERIC('):
            return upper.replace('NUMERIC', 'DECIMAL', 1)
        return dtype

    def _build_dim_ddl(self, dim) -> str:
        lines = []
        for col in dim.columns:
            dtype   = self._map_type(col.dtype)
            dtype_u = dtype.upper()
            if (
                col.key == 'PK'
                and 'AUTO_INCREMENT' not in dtype_u
                and any(t in dtype_u for t in ('INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'MEDIUMINT'))
            ):
                dtype = f"{dtype} AUTO_INCREMENT"
            notnull = ' NOT NULL' if not col.nullable else ''
            pk      = ' PRIMARY KEY' if col.key == 'PK' else ''
            lines.append(f"  `{col.name}` {dtype}{pk}{notnull}")
        return (f"CREATE TABLE IF NOT EXISTS `{dim.name}` (\n"
                + ',\n'.join(lines) + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")

    def _build_fact_ddl(self) -> str:
        fact  = self.sc.fact
        lines = []
        fk_cols = {fk['column']: fk['references'] for fk in fact.foreign_keys}
        for col in fact.columns:
            dtype   = self._map_type(col.dtype)
            dtype_u = dtype.upper()
            if (
                col.key == 'PK'
                and 'AUTO_INCREMENT' not in dtype_u
                and any(t in dtype_u for t in ('INT', 'BIGINT', 'SMALLINT', 'TINYINT', 'MEDIUMINT'))
            ):
                dtype = f"{dtype} AUTO_INCREMENT"
            notnull = ' NOT NULL' if not col.nullable else ''
            pk      = ' PRIMARY KEY' if col.key == 'PK' else ''
            lines.append(f"  `{col.name}` {dtype}{pk}{notnull}")
        # Add foreign key constraints
        fk_lines = []
        for col_name, ref in fk_cols.items():
            ref_table, ref_col = ref.split('.')
            fk_lines.append(
                f"  FOREIGN KEY (`{col_name}`) REFERENCES `{ref_table}`(`{ref_col}`)"
            )
        all_lines = lines + fk_lines
        return (f"CREATE TABLE IF NOT EXISTS `{fact.name}` (\n"
                + ',\n'.join(all_lines)
                + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")

    def create_all_tables(self):
        log.info("Creating dimension tables from schema_config.xml ...")
        for dim in sorted(self.sc.dimensions, key=lambda d: d.ordinal):
            ddl = self._build_dim_ddl(dim)
            self.db.execute(ddl)
            self.db.commit()
            log.info(f"  Table '{dim.name}' ready  ({len(dim.columns)} columns)")

        log.info("Creating fact table ...")
        ddl = self._build_fact_ddl()
        self.db.execute(ddl)
        self.db.commit()
        log.info(f"  Table '{self.sc.fact.name}' ready  ({len(self.sc.fact.columns)} columns)")

        # Create indexes
        current_db_rows = self.db.fetchall("SELECT DATABASE()")
        current_db = current_db_rows[0][0] if current_db_rows and current_db_rows[0] else self.sc.database
        for ix in self.sc.fact.indexes:
            exists_sql = """
                SELECT 1
                FROM information_schema.statistics
                WHERE table_schema = %s
                  AND table_name = %s
                  AND index_name = %s
                LIMIT 1
            """
            sql = (f"CREATE INDEX `{ix['name']}` "
                   f"ON `{self.sc.fact.name}` ({ix['columns']});")
            try:
                exists = self.db.fetchall(exists_sql, (current_db, self.sc.fact.name, ix['name']))
                if not exists:
                    self.db.execute(sql)
                    self.db.commit()
            except Exception:
                pass   # Index already exists — fine
        log.info("All tables and indexes created.")


class LOCSchemaManager:
    """
    Creates LOC stock-hierarchy tables from an external SQL file.
    The SQL file is expected to be non-destructive (CREATE TABLE only).
    """

    def __init__(self, db: DBConnection, sql_file: Optional[str]):
        self.db = db
        self.sql_file = sql_file

    def ensure_tables(self):
        if not self.sql_file:
            log.info("LOC schema SQL file is not configured; skipping LOC table creation.")
            return
        if not os.path.isfile(self.sql_file):
            log.warning(f"LOC schema SQL file not found: {self.sql_file}. Skipping LOC table creation.")
            return
        with open(self.sql_file, 'r', encoding='utf-8') as f:
            script = f.read()
        self.db.execute_script(script)
        self.db.commit()
        log.info(f"LOC schema ensured from '{self.sql_file}'.")


class LOCMaterializer:
    """
    Materializes 8 stock-hierarchy lattice cuboids using fact_stock_bar + dim_stock,
    scoped to one closed window at a time, then incrementally upserts into LOC tables.
    """

    def __init__(self, db: DBConnection):
        self.db = db

    def materialize_for_window(self, window_key: int):
        self._upsert_apex(window_key)
        self._upsert_company(window_key)
        self._upsert_sector(window_key)
        self._upsert_industry(window_key)
        self._upsert_company_sector(window_key)
        self._upsert_company_industry(window_key)
        self._upsert_sector_industry(window_key)
        self._upsert_company_sector_industry(window_key)

    def _upsert_apex(self, window_key: int):
        sql = """
            INSERT INTO `loc_apex`
                (id, volume_sum, volume_avg, sma_30_sum, sma_30_avg, ticker_no, tick_timestamp)
            SELECT
                1,
                COALESCE(SUM(f.volume), 0) AS volume_sum,
                COALESCE(AVG(f.volume), 0) AS volume_avg,
                COALESCE(SUM(f.sma_20), 0) AS sma_30_sum,
                COALESCE(AVG(f.sma_20), 0) AS sma_30_avg,
                COUNT(*) AS ticker_no,
                MAX(f.ingestion_ts) AS tick_timestamp
            FROM fact_stock_bar f
            WHERE f.window_key = %s
            ON DUPLICATE KEY UPDATE
                volume_sum = loc_apex.volume_sum + VALUES(volume_sum),
                sma_30_sum = loc_apex.sma_30_sum + VALUES(sma_30_sum),
                ticker_no = loc_apex.ticker_no + VALUES(ticker_no),
                volume_avg = CASE WHEN (loc_apex.ticker_no + VALUES(ticker_no)) = 0
                                  THEN 0
                                  ELSE (loc_apex.volume_sum + VALUES(volume_sum)) / (loc_apex.ticker_no + VALUES(ticker_no))
                             END,
                sma_30_avg = CASE WHEN (loc_apex.ticker_no + VALUES(ticker_no)) = 0
                                  THEN 0
                                  ELSE (loc_apex.sma_30_sum + VALUES(sma_30_sum)) / (loc_apex.ticker_no + VALUES(ticker_no))
                             END,
                tick_timestamp = GREATEST(COALESCE(loc_apex.tick_timestamp, '1970-01-01 00:00:00'),
                                          COALESCE(VALUES(tick_timestamp), '1970-01-01 00:00:00'))
        """
        self.db.execute(sql, (window_key,))

    def _upsert_company(self, window_key: int):
        sql = """
            INSERT INTO `loc_company`
                (company_name, volume_sum, volume_avg, sma_30_sum, sma_30_avg, ticker_no, tick_timestamp)
            SELECT
                ds.company_name,
                COALESCE(SUM(f.volume), 0),
                COALESCE(AVG(f.volume), 0),
                COALESCE(SUM(f.sma_20), 0),
                COALESCE(AVG(f.sma_20), 0),
                COUNT(*),
                MAX(f.ingestion_ts)
            FROM fact_stock_bar f
            JOIN dim_stock ds ON ds.stock_key = f.stock_key
            WHERE f.window_key = %s
            GROUP BY ds.company_name
            ON DUPLICATE KEY UPDATE
                volume_sum = loc_company.volume_sum + VALUES(volume_sum),
                sma_30_sum = loc_company.sma_30_sum + VALUES(sma_30_sum),
                ticker_no = loc_company.ticker_no + VALUES(ticker_no),
                volume_avg = CASE WHEN (loc_company.ticker_no + VALUES(ticker_no)) = 0
                                  THEN 0
                                  ELSE (loc_company.volume_sum + VALUES(volume_sum)) / (loc_company.ticker_no + VALUES(ticker_no))
                             END,
                sma_30_avg = CASE WHEN (loc_company.ticker_no + VALUES(ticker_no)) = 0
                                  THEN 0
                                  ELSE (loc_company.sma_30_sum + VALUES(sma_30_sum)) / (loc_company.ticker_no + VALUES(ticker_no))
                             END,
                tick_timestamp = GREATEST(COALESCE(loc_company.tick_timestamp, '1970-01-01 00:00:00'),
                                          COALESCE(VALUES(tick_timestamp), '1970-01-01 00:00:00'))
        """
        self.db.execute(sql, (window_key,))

    def _upsert_sector(self, window_key: int):
        sql = """
            INSERT INTO `loc_sector`
                (sector, volume_sum, volume_avg, sma_30_sum, sma_30_avg, ticker_no, tick_timestamp)
            SELECT
                ds.sector,
                COALESCE(SUM(f.volume), 0),
                COALESCE(AVG(f.volume), 0),
                COALESCE(SUM(f.sma_20), 0),
                COALESCE(AVG(f.sma_20), 0),
                COUNT(*),
                MAX(f.ingestion_ts)
            FROM fact_stock_bar f
            JOIN dim_stock ds ON ds.stock_key = f.stock_key
            WHERE f.window_key = %s
            GROUP BY ds.sector
            ON DUPLICATE KEY UPDATE
                volume_sum = loc_sector.volume_sum + VALUES(volume_sum),
                sma_30_sum = loc_sector.sma_30_sum + VALUES(sma_30_sum),
                ticker_no = loc_sector.ticker_no + VALUES(ticker_no),
                volume_avg = CASE WHEN (loc_sector.ticker_no + VALUES(ticker_no)) = 0
                                  THEN 0
                                  ELSE (loc_sector.volume_sum + VALUES(volume_sum)) / (loc_sector.ticker_no + VALUES(ticker_no))
                             END,
                sma_30_avg = CASE WHEN (loc_sector.ticker_no + VALUES(ticker_no)) = 0
                                  THEN 0
                                  ELSE (loc_sector.sma_30_sum + VALUES(sma_30_sum)) / (loc_sector.ticker_no + VALUES(ticker_no))
                             END,
                tick_timestamp = GREATEST(COALESCE(loc_sector.tick_timestamp, '1970-01-01 00:00:00'),
                                          COALESCE(VALUES(tick_timestamp), '1970-01-01 00:00:00'))
        """
        self.db.execute(sql, (window_key,))

    def _upsert_industry(self, window_key: int):
        sql = """
            INSERT INTO `loc_industry`
                (industry, volume_sum, volume_avg, sma_30_sum, sma_30_avg, ticker_no, tick_timestamp)
            SELECT
                ds.industry,
                COALESCE(SUM(f.volume), 0),
                COALESCE(AVG(f.volume), 0),
                COALESCE(SUM(f.sma_20), 0),
                COALESCE(AVG(f.sma_20), 0),
                COUNT(*),
                MAX(f.ingestion_ts)
            FROM fact_stock_bar f
            JOIN dim_stock ds ON ds.stock_key = f.stock_key
            WHERE f.window_key = %s
            GROUP BY ds.industry
            ON DUPLICATE KEY UPDATE
                volume_sum = loc_industry.volume_sum + VALUES(volume_sum),
                sma_30_sum = loc_industry.sma_30_sum + VALUES(sma_30_sum),
                ticker_no = loc_industry.ticker_no + VALUES(ticker_no),
                volume_avg = CASE WHEN (loc_industry.ticker_no + VALUES(ticker_no)) = 0
                                  THEN 0
                                  ELSE (loc_industry.volume_sum + VALUES(volume_sum)) / (loc_industry.ticker_no + VALUES(ticker_no))
                             END,
                sma_30_avg = CASE WHEN (loc_industry.ticker_no + VALUES(ticker_no)) = 0
                                  THEN 0
                                  ELSE (loc_industry.sma_30_sum + VALUES(sma_30_sum)) / (loc_industry.ticker_no + VALUES(ticker_no))
                             END,
                tick_timestamp = GREATEST(COALESCE(loc_industry.tick_timestamp, '1970-01-01 00:00:00'),
                                          COALESCE(VALUES(tick_timestamp), '1970-01-01 00:00:00'))
        """
        self.db.execute(sql, (window_key,))

    def _upsert_company_sector(self, window_key: int):
        sql = """
            INSERT INTO `loc_company_sector`
                (company_name, sector, volume_sum, volume_avg, sma_30_sum, sma_30_avg, ticker_no, tick_timestamp)
            SELECT
                ds.company_name,
                ds.sector,
                COALESCE(SUM(f.volume), 0),
                COALESCE(AVG(f.volume), 0),
                COALESCE(SUM(f.sma_20), 0),
                COALESCE(AVG(f.sma_20), 0),
                COUNT(*),
                MAX(f.ingestion_ts)
            FROM fact_stock_bar f
            JOIN dim_stock ds ON ds.stock_key = f.stock_key
            WHERE f.window_key = %s
            GROUP BY ds.company_name, ds.sector
            ON DUPLICATE KEY UPDATE
                volume_sum = loc_company_sector.volume_sum + VALUES(volume_sum),
                sma_30_sum = loc_company_sector.sma_30_sum + VALUES(sma_30_sum),
                ticker_no = loc_company_sector.ticker_no + VALUES(ticker_no),
                volume_avg = CASE WHEN (loc_company_sector.ticker_no + VALUES(ticker_no)) = 0
                                  THEN 0
                                  ELSE (loc_company_sector.volume_sum + VALUES(volume_sum)) / (loc_company_sector.ticker_no + VALUES(ticker_no))
                             END,
                sma_30_avg = CASE WHEN (loc_company_sector.ticker_no + VALUES(ticker_no)) = 0
                                  THEN 0
                                  ELSE (loc_company_sector.sma_30_sum + VALUES(sma_30_sum)) / (loc_company_sector.ticker_no + VALUES(ticker_no))
                             END,
                tick_timestamp = GREATEST(COALESCE(loc_company_sector.tick_timestamp, '1970-01-01 00:00:00'),
                                          COALESCE(VALUES(tick_timestamp), '1970-01-01 00:00:00'))
        """
        self.db.execute(sql, (window_key,))

    def _upsert_company_industry(self, window_key: int):
        sql = """
            INSERT INTO `loc_company_industry`
                (company_name, industry, volume_sum, volume_avg, sma_30_sum, sma_30_avg, ticker_no, tick_timestamp)
            SELECT
                ds.company_name,
                ds.industry,
                COALESCE(SUM(f.volume), 0),
                COALESCE(AVG(f.volume), 0),
                COALESCE(SUM(f.sma_20), 0),
                COALESCE(AVG(f.sma_20), 0),
                COUNT(*),
                MAX(f.ingestion_ts)
            FROM fact_stock_bar f
            JOIN dim_stock ds ON ds.stock_key = f.stock_key
            WHERE f.window_key = %s
            GROUP BY ds.company_name, ds.industry
            ON DUPLICATE KEY UPDATE
                volume_sum = loc_company_industry.volume_sum + VALUES(volume_sum),
                sma_30_sum = loc_company_industry.sma_30_sum + VALUES(sma_30_sum),
                ticker_no = loc_company_industry.ticker_no + VALUES(ticker_no),
                volume_avg = CASE WHEN (loc_company_industry.ticker_no + VALUES(ticker_no)) = 0
                                  THEN 0
                                  ELSE (loc_company_industry.volume_sum + VALUES(volume_sum)) / (loc_company_industry.ticker_no + VALUES(ticker_no))
                             END,
                sma_30_avg = CASE WHEN (loc_company_industry.ticker_no + VALUES(ticker_no)) = 0
                                  THEN 0
                                  ELSE (loc_company_industry.sma_30_sum + VALUES(sma_30_sum)) / (loc_company_industry.ticker_no + VALUES(ticker_no))
                             END,
                tick_timestamp = GREATEST(COALESCE(loc_company_industry.tick_timestamp, '1970-01-01 00:00:00'),
                                          COALESCE(VALUES(tick_timestamp), '1970-01-01 00:00:00'))
        """
        self.db.execute(sql, (window_key,))

    def _upsert_sector_industry(self, window_key: int):
        sql = """
            INSERT INTO `loc_sector_industry`
                (sector, industry, volume_sum, volume_avg, sma_30_sum, sma_30_avg, ticker_no, tick_timestamp)
            SELECT
                ds.sector,
                ds.industry,
                COALESCE(SUM(f.volume), 0),
                COALESCE(AVG(f.volume), 0),
                COALESCE(SUM(f.sma_20), 0),
                COALESCE(AVG(f.sma_20), 0),
                COUNT(*),
                MAX(f.ingestion_ts)
            FROM fact_stock_bar f
            JOIN dim_stock ds ON ds.stock_key = f.stock_key
            WHERE f.window_key = %s
            GROUP BY ds.sector, ds.industry
            ON DUPLICATE KEY UPDATE
                volume_sum = loc_sector_industry.volume_sum + VALUES(volume_sum),
                sma_30_sum = loc_sector_industry.sma_30_sum + VALUES(sma_30_sum),
                ticker_no = loc_sector_industry.ticker_no + VALUES(ticker_no),
                volume_avg = CASE WHEN (loc_sector_industry.ticker_no + VALUES(ticker_no)) = 0
                                  THEN 0
                                  ELSE (loc_sector_industry.volume_sum + VALUES(volume_sum)) / (loc_sector_industry.ticker_no + VALUES(ticker_no))
                             END,
                sma_30_avg = CASE WHEN (loc_sector_industry.ticker_no + VALUES(ticker_no)) = 0
                                  THEN 0
                                  ELSE (loc_sector_industry.sma_30_sum + VALUES(sma_30_sum)) / (loc_sector_industry.ticker_no + VALUES(ticker_no))
                             END,
                tick_timestamp = GREATEST(COALESCE(loc_sector_industry.tick_timestamp, '1970-01-01 00:00:00'),
                                          COALESCE(VALUES(tick_timestamp), '1970-01-01 00:00:00'))
        """
        self.db.execute(sql, (window_key,))

    def _upsert_company_sector_industry(self, window_key: int):
        sql = """
            INSERT INTO `loc_company_sector_industry`
                (company_name, sector, industry, volume_sum, volume_avg, sma_30_sum, sma_30_avg, ticker_no, tick_timestamp)
            SELECT
                ds.company_name,
                ds.sector,
                ds.industry,
                COALESCE(SUM(f.volume), 0),
                COALESCE(AVG(f.volume), 0),
                COALESCE(SUM(f.sma_20), 0),
                COALESCE(AVG(f.sma_20), 0),
                COUNT(*),
                MAX(f.ingestion_ts)
            FROM fact_stock_bar f
            JOIN dim_stock ds ON ds.stock_key = f.stock_key
            WHERE f.window_key = %s
            GROUP BY ds.company_name, ds.sector, ds.industry
            ON DUPLICATE KEY UPDATE
                volume_sum = loc_company_sector_industry.volume_sum + VALUES(volume_sum),
                sma_30_sum = loc_company_sector_industry.sma_30_sum + VALUES(sma_30_sum),
                ticker_no = loc_company_sector_industry.ticker_no + VALUES(ticker_no),
                volume_avg = CASE WHEN (loc_company_sector_industry.ticker_no + VALUES(ticker_no)) = 0
                                  THEN 0
                                  ELSE (loc_company_sector_industry.volume_sum + VALUES(volume_sum)) / (loc_company_sector_industry.ticker_no + VALUES(ticker_no))
                             END,
                sma_30_avg = CASE WHEN (loc_company_sector_industry.ticker_no + VALUES(ticker_no)) = 0
                                  THEN 0
                                  ELSE (loc_company_sector_industry.sma_30_sum + VALUES(sma_30_sum)) / (loc_company_sector_industry.ticker_no + VALUES(ticker_no))
                             END,
                tick_timestamp = GREATEST(COALESCE(loc_company_sector_industry.tick_timestamp, '1970-01-01 00:00:00'),
                                          COALESCE(VALUES(tick_timestamp), '1970-01-01 00:00:00'))
        """
        self.db.execute(sql, (window_key,))


# ═══════════════════════════════════════════════════════════
# STEP 3 ── SEED DIMENSION TABLES
# ═══════════════════════════════════════════════════════════

class DimensionLoader:

    def __init__(self, db: DBConnection, sc: SchemaConfig):
        self.db = db
        self.sc = sc
        self._stock_key_cache: Dict[str, int] = {}
        self._time_key_cache:  Dict[str, int] = {}
        self._window_key_cache: Dict[int, int] = {}

    def _refresh_stock_cache(self):
        self._stock_key_cache.clear()
        results = self.db.fetchall(
            """
            SELECT MAX(stock_key) AS stock_key, ticker
            FROM `dim_stock`
            WHERE is_current = 1
            GROUP BY ticker
            """
        )
        for stock_key, ticker in results:
            self._stock_key_cache[ticker] = stock_key

    # ── DIM_STOCK ──────────────────────────────────────────
    def load_stocks(self):
        dim = next(d for d in self.sc.dimensions if d.name == 'dim_stock')
        # Read seed data from XML
        seed_el = None
        tree = ET.parse('schema_config.xml')
        root = tree.getroot()
        for d in root.findall('dimensions/dimension'):
            if d.get('name') == 'dim_stock':
                seed_el = d.find('seed_data')
                break

        if seed_el is None:
            log.warning("No <seed_data> found in schema_config.xml for dim_stock.")
            return

        today = datetime.today().strftime('%Y-%m-%d')
        rows  = []
        for s in seed_el.findall('stock'):
            rows.append((
                s.get('ticker'), s.get('company'), s.get('sector'),
                s.get('industry'), s.get('exchange'), s.get('currency', 'USD'),
                s.get('market_cap_tier', 'LARGE'), s.get('index_membership', ''),
                today, None, 1
            ))

        sql = """
            INSERT IGNORE INTO `dim_stock`
              (ticker, company_name, sector, industry, exchange, currency,
               market_cap_tier, index_membership, eff_from, eff_to, is_current)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        self.db.executemany(sql, rows)
        self.db.commit()
        log.info(f"  dim_stock seeded with {len(rows)} tickers.")

        # Build lookup cache: ticker -> stock_key
        self._refresh_stock_cache()

    def enrich_stocks_from_master(self, csv_path: Optional[str]):
        """
        Upsert stock metadata from a CSV file into dim_stock.
        Expected columns:
          ticker, company_name, sector, industry, exchange, currency,
          market_cap_tier, index_membership
        """
        if not csv_path:
            return
        if not os.path.isfile(csv_path):
            log.warning(f"Stock master CSV not found: {csv_path}. Skipping dim_stock enrichment.")
            return

        try:
            df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
        except Exception as e:
            log.warning(f"Failed to read stock master CSV '{csv_path}': {e}")
            return

        required = {'ticker', 'company_name', 'sector', 'industry', 'exchange'}
        missing = [c for c in required if c not in df.columns]
        if missing:
            log.warning(f"Stock master CSV missing required columns {missing}. Skipping enrichment.")
            return

        today = datetime.today().strftime('%Y-%m-%d')
        updated = 0
        inserted = 0

        for _, row in df.iterrows():
            ticker = str(row.get('ticker', '')).strip().upper()
            if not ticker:
                continue

            company_name = str(row.get('company_name', ticker)).strip() or ticker
            sector = str(row.get('sector', 'UNKNOWN')).strip() or 'UNKNOWN'
            industry = str(row.get('industry', 'UNKNOWN')).strip() or 'UNKNOWN'
            exchange = str(row.get('exchange', 'UNKNOWN')).strip() or 'UNKNOWN'
            currency = str(row.get('currency', 'USD')).strip() or 'USD'
            market_cap_tier = str(row.get('market_cap_tier', 'UNKNOWN')).strip() or 'UNKNOWN'
            index_membership = str(row.get('index_membership', '')).strip()

            existing = self.db.fetchall(
                "SELECT stock_key FROM `dim_stock` WHERE ticker = %s AND is_current = 1 LIMIT 1",
                (ticker,),
            )

            if existing:
                self.db.execute(
                    """
                    UPDATE `dim_stock`
                    SET company_name=%s,
                        sector=%s,
                        industry=%s,
                        exchange=%s,
                        currency=%s,
                        market_cap_tier=%s,
                        index_membership=%s
                    WHERE ticker=%s
                      AND is_current=1
                    """,
                    (
                        company_name, sector, industry, exchange,
                        currency, market_cap_tier, index_membership,
                        ticker,
                    ),
                )
                updated += 1
            else:
                self.db.execute(
                    """
                    INSERT INTO `dim_stock`
                      (ticker, company_name, sector, industry, exchange, currency,
                       market_cap_tier, index_membership, eff_from, eff_to, is_current)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        ticker, company_name, sector, industry, exchange, currency,
                        market_cap_tier, index_membership, today, None, 1,
                    ),
                )
                inserted += 1

        self.db.commit()
        self._refresh_stock_cache()
        log.info(
            f"  dim_stock enriched from '{csv_path}' (updated={updated}, inserted={inserted})."
        )

    # ── DIM_TIME ───────────────────────────────────────────
    def load_time_for_df(self, df: pd.DataFrame):
        """
        Inserts a DIM_TIME row for every unique timestamp in the CSV.
        Only inserts rows that do not already exist.
        """
        unique_ts = df['timestamp'].unique()
        rows = []
        for ts_str in unique_ts:
            ts = pd.to_datetime(ts_str)
            time_key = int(ts.strftime('%Y%m%d%H%M'))
            if time_key in self._time_key_cache:
                continue
            minute_of_day = (ts.hour * 60 + ts.minute) - (9 * 60 + 30)
            if ts.hour < 9 or (ts.hour == 9 and ts.minute < 30):
                session = 'PRE'
            elif ts.hour >= 16:
                session = 'AFTER'
            elif ts.hour == 15 and ts.minute >= 50:
                session = 'CLOSE'
            else:
                session = 'OPEN'
            quarter = (ts.month - 1) // 3 + 1
            is_open = 1 if session in ('OPEN', 'CLOSE') else 0
            rows.append((
                time_key,
                ts.strftime('%Y-%m-%d'),
                ts.strftime('%H:%M:%S'),
                ts.hour,
                minute_of_day,
                session,
                ts.strftime('%A'),
                int(ts.strftime('%V')),
                ts.month,
                quarter,
                ts.year,
                is_open,
                None   # window_id filled later
            ))
            self._time_key_cache[ts_str] = time_key

        if rows:
            sql = """
                INSERT IGNORE INTO `dim_time`
                  (time_key, trade_date, trade_time, hour, minute_of_day,
                   trading_session, day_of_week, week_of_year, month, quarter,
                   year, is_market_open, window_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            self.db.executemany(sql, rows)
            self.db.commit()
            log.info(f"  dim_time populated with {len(rows)} timestamp rows.")

    # ── DIM_WINDOW (inserted per window as it closes) ──────
    def insert_window(self, window_id: int, start_ts: str,
                      end_ts: str, bar_count: int,
                      wc: WindowConfig, auto_commit: bool = True) -> int:
        sql = """
            INSERT INTO `dim_window`
              (window_id, window_start_ts, window_end_ts,
               window_size_min, slide_size_min, window_type,
               bar_count, trade_date)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """
        trade_date = start_ts[:10]
        self.db.execute(sql, (
            window_id, start_ts, end_ts,
            wc.window_size, wc.slide_velocity, wc.window_type,
            bar_count, trade_date
        ))
        if auto_commit:
            self.db.commit()
        window_key = self.db.cursor.lastrowid
        self._window_key_cache[window_id] = window_key
        return window_key

    def get_stock_key(self, ticker: str) -> Optional[int]:
        return self._stock_key_cache.get(ticker)

    def ensure_stock_key(self, ticker: str) -> Optional[int]:
        """
        Return stock_key for ticker; if missing in dim_stock, insert a minimal row.
        This prevents window skips when CSV has tickers not present in seed_data.
        """
        existing = self._stock_key_cache.get(ticker)
        if existing is not None:
            return existing

        today = datetime.today().strftime('%Y-%m-%d')
        insert_sql = """
            INSERT IGNORE INTO `dim_stock`
              (ticker, company_name, sector, industry, exchange, currency,
               market_cap_tier, index_membership, eff_from, eff_to, is_current)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        self.db.execute(insert_sql, (
            ticker,
            ticker,            # company_name fallback
            'UNKNOWN',         # sector
            'UNKNOWN',         # industry
            'UNKNOWN',         # exchange
            'USD',             # currency
            'UNKNOWN',         # market_cap_tier
            '',                # index_membership
            today,
            None,
            1,
        ))
        self.db.commit()

        lookup_sql = "SELECT stock_key FROM `dim_stock` WHERE ticker = %s LIMIT 1"
        rows = self.db.fetchall(lookup_sql, (ticker,))
        if not rows:
            return None

        stock_key = rows[0][0]
        self._stock_key_cache[ticker] = stock_key
        log.info(f"  dim_stock auto-added ticker '{ticker}' with stock_key={stock_key}")
        return stock_key

    def get_time_key(self, ts_str: str) -> Optional[int]:
        return self._time_key_cache.get(ts_str)

    def get_window_key(self, window_id: int) -> Optional[int]:
        return self._window_key_cache.get(window_id)


# ═══════════════════════════════════════════════════════════
# STEP 4 ── INDICATOR COMPUTATION
# ═══════════════════════════════════════════════════════════

class IndicatorEngine:

    def __init__(self, indicators: List[Dict]):
        # Read lookback values from XML config
        self._cfg = {ind['name']: ind for ind in indicators}
        sma_lb  = int(self._cfg.get('sma_20', {}).get('lookback', 20))
        ema_sp  = int(self._cfg.get('ema_9',  {}).get('span',     9))
        rsi_pd  = int(self._cfg.get('rsi_14', {}).get('periods',  14))

        self._sma_lb   = sma_lb
        self._ema_span = ema_sp
        self._rsi_pd   = rsi_pd
        self._ema_alpha = 2 / (ema_sp + 1)

        # Per-ticker state
        self._closes:   Dict[str, deque] = {}   # rolling close buffer
        self._ema:      Dict[str, float] = {}   # last EMA value
        self._gains:    Dict[str, deque] = {}   # RSI gain buffer
        self._losses:   Dict[str, deque] = {}   # RSI loss buffer
        self._vwap_num: Dict[str, float] = {}   # VWAP numerator
        self._vwap_den: Dict[str, float] = {}   # VWAP denominator
        self._last_date: Dict[str, str]  = {}   # track day change for VWAP reset

    def _ensure(self, ticker: str):
        if ticker not in self._closes:
            self._closes[ticker]   = deque(maxlen=max(self._sma_lb, self._rsi_pd + 1))
            self._ema[ticker]      = None
            self._gains[ticker]    = deque(maxlen=self._rsi_pd)
            self._losses[ticker]   = deque(maxlen=self._rsi_pd)
            self._vwap_num[ticker] = 0.0
            self._vwap_den[ticker] = 0.0
            self._last_date[ticker] = ''

    def compute(self, ticker: str, ts: str,
                open_: float, high: float, low: float,
                close: float, volume: int) -> Dict:
        self._ensure(ticker)
        trade_date = ts[:10]

        # Reset VWAP at start of each new trading day
        if trade_date != self._last_date[ticker]:
            self._vwap_num[ticker] = 0.0
            self._vwap_den[ticker] = 0.0
            self._last_date[ticker] = trade_date

        closes = self._closes[ticker]

        # ── bar_return ─────────────────────────────────────
        bar_return = 0.0
        if closes:
            prev = closes[-1]
            bar_return = (close - prev) / prev if prev != 0 else 0.0

        # ── bar_range ──────────────────────────────────────
        bar_range = round(high - low, 4)

        # ── dollar_volume ──────────────────────────────────
        dollar_volume = round(close * volume, 2)

        # ── VWAP (cumulative intraday) ─────────────────────
        typical_price = (high + low + close) / 3
        self._vwap_num[ticker] += typical_price * volume
        self._vwap_den[ticker] += volume
        vwap = (self._vwap_num[ticker] / self._vwap_den[ticker]
                if self._vwap_den[ticker] > 0 else close)

        # ── Update close buffer ────────────────────────────
        closes.append(close)

        # ── SMA-20 ─────────────────────────────────────────
        sma_20 = (sum(list(closes)[-self._sma_lb:]) / min(len(closes), self._sma_lb))

        # ── EMA-9 ──────────────────────────────────────────
        if self._ema[ticker] is None:
            self._ema[ticker] = close
        else:
            self._ema[ticker] = (close * self._ema_alpha
                                 + self._ema[ticker] * (1 - self._ema_alpha))
        ema_9 = self._ema[ticker]

        # ── RSI-14 ─────────────────────────────────────────
        if bar_return > 0:
            self._gains[ticker].append(bar_return)
            self._losses[ticker].append(0.0)
        else:
            self._gains[ticker].append(0.0)
            self._losses[ticker].append(abs(bar_return))

        avg_gain = sum(self._gains[ticker]) / max(len(self._gains[ticker]), 1)
        avg_loss = sum(self._losses[ticker]) / max(len(self._losses[ticker]), 1)
        if avg_loss == 0:
            rsi_14 = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi_14 = 100 - (100 / (1 + rs))

        return {
            'bar_return':    round(bar_return,   6),
            'bar_range':     round(bar_range,    4),
            'dollar_volume': round(dollar_volume, 2),
            'vwap':          round(vwap,         4),
            'sma_20':        round(sma_20,       4),
            'ema_9':         round(ema_9,        4),
            'rsi_14':        round(rsi_14,       3),
        }


# ═══════════════════════════════════════════════════════════
# STEP 5 ── WINDOW ENGINE + FACT TABLE LOADER
# ═══════════════════════════════════════════════════════════

class WindowEngine:
    """
    Maintains a sliding window buffer per ticker.
    After every S new bars (slide_velocity), closes the current window,
    registers it in DIM_WINDOW, bulk-inserts window bars into FACT_STOCK_BAR,
    materializes LOC cuboids, prunes old FACT rows, then commits.
    """

    def __init__(self, db: DBConnection, wc: WindowConfig,
                 dim_loader: DimensionLoader,
                 pc: PipelineConfig,
                 loc_materializer: LOCMaterializer):
        self.db         = db
        self.wc         = wc
        self.dim_loader = dim_loader
        self.pc         = pc
        self.loc_materializer = loc_materializer

        self._buffers:    Dict[str, deque] = {}  # per-ticker bar buffer (size W)
        self._bar_counts: Dict[str, int]   = {}  # total bars seen per ticker
        self._window_id   = 0                     # global window sequence counter

    def _ensure(self, ticker: str):
        if ticker not in self._buffers:
            self._buffers[ticker]    = deque(maxlen=self.wc.window_size)
            self._bar_counts[ticker] = 0

    def ingest_bar(self, bar: Dict):
        """Called once per CSV row after indicators are computed."""
        ticker = bar['ticker']
        self._ensure(ticker)
        self._bar_counts[ticker] += 1
        self._buffers[ticker].append(bar)

        # Skip warmup period
        if self._bar_counts[ticker] <= self.wc.warmup_bars:
            return

        # Fire a window every S bars after warmup
        bars_past_warmup = self._bar_counts[ticker] - self.wc.warmup_bars
        if bars_past_warmup % self.wc.slide_velocity != 0:
            return

        buf = list(self._buffers[ticker])
        if len(buf) < self.wc.min_bars:
            return

        self._close_window(ticker, buf)

    def _close_window(self, ticker: str, bars: List[Dict]):
        self._window_id += 1
        win_id    = self._window_id
        start_ts  = bars[0]['timestamp']
        end_ts    = bars[-1]['timestamp']
        bar_count = len(bars)

        stock_key = self.dim_loader.ensure_stock_key(ticker)
        if stock_key is None:
            log.warning(f"Skipping window {win_id}: stock_key not found for ticker '{ticker}'.")
            return

        fact_rows = []
        for i, bar in enumerate(bars):
            time_key = self.dim_loader.get_time_key(bar['timestamp'])
            if time_key is None:
                continue
            fact_rows.append((
                time_key,
                stock_key,
                bar['open'],
                bar['high'],
                bar['low'],
                bar['close'],
                bar['volume'],
                bar.get('trade_count', None),
                bar['vwap'],
                bar['dollar_volume'],
                bar['bar_return'],
                bar['bar_range'],
                bar['sma_20'],
                bar['ema_9'],
                bar['rsi_14'],
                1 if i == 0 else 0,
                1 if i == len(bars) - 1 else 0,
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            ))

        if not fact_rows:
            log.warning(f"Skipping window {win_id}: no valid fact rows were generated.")
            return

        try:
            self.db.begin()

            window_key = self.dim_loader.insert_window(
                win_id, start_ts, end_ts, bar_count, self.wc, auto_commit=False
            )

            sql = """
                INSERT INTO `fact_stock_bar`
                  (time_key, stock_key, window_key,
                   open_price, high_price, low_price, close_price,
                   volume, trade_count, vwap, dollar_volume,
                   bar_return, bar_range, sma_20, ema_9, rsi_14,
                   is_window_first_bar, is_window_last_bar, ingestion_ts)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            rows_with_window = [
                (
                    row[0], row[1], window_key,
                    row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9],
                    row[10], row[11], row[12], row[13], row[14], row[15], row[16], row[17],
                )
                for row in fact_rows
            ]
            self.db.executemany(sql, rows_with_window)

            if self.pc.materialize_loc_tables:
                self.loc_materializer.materialize_for_window(window_key)

            pruned = 0
            if self.pc.prune_after_loc_update and self.pc.fact_retention_keep_latest > 0:
                pruned = self._prune_fact_global(self.pc.fact_retention_keep_latest)

            self.db.commit()
            log.info(
                f"  Window {win_id:>4} | {ticker:<6} | {start_ts} | "
                f"fact_rows={len(rows_with_window)} | loc={'on' if self.pc.materialize_loc_tables else 'off'} "
                f"| pruned={pruned}"
            )
        except Exception as e:
            self.db.rollback()
            log.error(
                f"Window transaction failed (window_id={win_id}, ticker={ticker}, start_ts={start_ts}): {e}"
            )
            raise

    def _prune_fact_global(self, keep_latest: int) -> int:
        """Delete old FACT rows globally, keeping only latest N rows by (time_key, bar_key)."""
        sql = """
            DELETE f
            FROM fact_stock_bar f
            JOIN (
                SELECT bar_key
                FROM (
                    SELECT
                        bar_key,
                        ROW_NUMBER() OVER (ORDER BY time_key DESC, bar_key DESC) AS rn
                    FROM fact_stock_bar
                ) ranked
                WHERE rn > %s
            ) d ON d.bar_key = f.bar_key
        """
        self.db.execute(sql, (keep_latest,))
        return self.db.cursor.rowcount


# ═══════════════════════════════════════════════════════════
# STEP 6 ── MAIN PIPELINE ORCHESTRATOR
# ═══════════════════════════════════════════════════════════

class Pipeline:
    """
    Orchestrates everything:
      1. Load all XML configs
      2. Connect to MySQL
      3. Create tables
      4. Seed dimensions
      5. Read CSV row by row → compute indicators → slide window
    """

    def __init__(self):
        base = os.path.dirname(os.path.abspath(__file__))
        os.chdir(base)
        log.info("=" * 60)
        log.info("  STOCK BOARD ANALYSIS — ETL PIPELINE STARTING")
        log.info("=" * 60)

        # Load all configs from XML
        log.info("Loading XML configurations ...")
        self.wc = WindowConfig.load('window_config.xml')
        self.sc = SchemaConfig.load('schema_config.xml')
        self.pc = PipelineConfig.load('pipeline_config.xml')

        # Optional: keep loading read-only lattice query catalog for diagnostics
        self.qc = QueryConfig.load('lattice_queries.xml')

        self.wc.summary()
        self.sc.summary()
        self.qc.summary()
        self.pc.summary()

    def run(self):
        # ── Connect to MySQL ──────────────────────────────
        db = DBConnection(self.pc)
        db.connect()

        # ── Create tables from XML schema ─────────────────
        schema_mgr = SchemaManager(db, self.sc)
        schema_mgr.create_all_tables()

        # ── Ensure LOC stock-hierarchy tables exist ───────
        if self.pc.auto_create_loc_tables:
            loc_schema_mgr = LOCSchemaManager(db, self.pc.loc_schema_sql_file)
            loc_schema_mgr.ensure_tables()
        else:
            log.info("LOC table auto-creation disabled by pipeline_config.xml")

        # ── Seed dimensions ────────────────────────────────
        dim_loader = DimensionLoader(db, self.sc)
        dim_loader.load_stocks()
        dim_loader.enrich_stocks_from_master(self.pc.stock_master_csv_path)

        # ── Read CSV ───────────────────────────────────────
        csv_path = self.pc.csv_path
        if not os.path.isfile(csv_path):
            log.error(f"CSV not found: {csv_path}")
            log.error("Update pipeline_config.xml → csv_source/file_path")
            db.close()
            return

        log.info(f"Reading CSV: {csv_path}")
        df = pd.read_csv(csv_path, dtype=str)
        log.info(f"  {len(df):,} rows loaded from CSV.")

        # Populate DIM_TIME for all timestamps in one shot
        dim_loader.load_time_for_df(df)

        # ── Set up indicator engine and window engine ──────
        indicator_eng = IndicatorEngine(self.pc.indicators)
        loc_materializer = LOCMaterializer(db)
        window_eng    = WindowEngine(db, self.wc, dim_loader, self.pc, loc_materializer)

        # ── Process each row in timestamp order ───────────
        log.info("Starting time-sequential row processing ...")
        start_time = time.time()
        total      = len(df)

        for idx, row in df.iterrows():
            # Parse numeric fields
            open_  = float(row['open'])
            high   = float(row['high'])
            low    = float(row['low'])
            close  = float(row['close'])
            volume = int(row['volume'])
            trade_count_raw = row.get('trade_count', None)
            if trade_count_raw is None or pd.isna(trade_count_raw) or str(trade_count_raw).strip() == '':
                trade_count = None
            else:
                trade_count = int(float(trade_count_raw))
            ticker = row['ticker']
            ts     = row['timestamp']

            # Compute all indicators for this bar
            indicators = indicator_eng.compute(
                ticker, ts, open_, high, low, close, volume
            )

            # Build complete bar dict
            bar = {
                'timestamp':   ts,
                'ticker':      ticker,
                'open':        open_,
                'high':        high,
                'low':         low,
                'close':       close,
                'volume':      volume,
                'trade_count': trade_count,
                **indicators,
            }

            # Feed to window engine
            window_eng.ingest_bar(bar)

            # Progress log every 10,000 rows
            if (idx + 1) % 10000 == 0:
                elapsed  = time.time() - start_time
                pct      = (idx + 1) / total * 100
                rows_sec = (idx + 1) / elapsed if elapsed > 0 else 0
                log.info(f"  Progress: {idx+1:,}/{total:,} rows "
                         f"({pct:.1f}%)  |  {rows_sec:.0f} rows/sec  "
                         f"|  Windows so far: {window_eng._window_id}")

        # ── Done ───────────────────────────────────────────
        elapsed = time.time() - start_time
        log.info("=" * 60)
        log.info(f"  PIPELINE COMPLETE")
        log.info(f"  Total rows processed : {total:,}")
        log.info(f"  Total windows closed : {window_eng._window_id}")
        log.info(f"  Time taken           : {elapsed:.1f} seconds")
        log.info(f"  Rows per second      : {total/elapsed:.0f}")
        log.info("=" * 60)

        # Quick verification query
        result = db.fetchall("SELECT COUNT(*) FROM fact_stock_bar")
        log.info(f"  fact_stock_bar rows in DB : {result[0][0]:,}")
        result = db.fetchall("SELECT COUNT(*) FROM dim_window")
        log.info(f"  dim_window rows in DB     : {result[0][0]:,}")

        db.close()


# ═══════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════
if __name__ == '__main__':
    pipeline = Pipeline()
    pipeline.run()
