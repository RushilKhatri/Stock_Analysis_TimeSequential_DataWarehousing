"""
config_reader.py
────────────────────────────────────────────────────────────
Reads all XML configuration files and exposes them as Python
objects. The ETL engine, window engine, and query runner all
import from this file — they never hardcode any values.

Usage:
    from config_reader import WindowConfig, SchemaConfig, QueryConfig, PipelineConfig
"""

import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import List, Dict, Optional


# ═══════════════════════════════════════════════════════════
# 1. WINDOW CONFIG  ←  window_config.xml
# ═══════════════════════════════════════════════════════════

@dataclass
class WindowConfig:
    window_type:      str
    window_size:      int
    slide_velocity:   int
    overlap_bars:     int
    overlap_ratio:    float
    warmup_bars:      int
    min_bars:         int
    bar_size:         int
    bars_per_day:     int
    market_open:      str
    market_close:     str
    total_windows:    int
    mode:             str
    max_windows:      int
    log_every:        int

    @classmethod
    def load(cls, path: str = 'window_config.xml') -> 'WindowConfig':
        root = ET.parse(path).getroot()
        ws   = root.find('window_specs')
        gr   = root.find('granularity')
        wm   = root.find('window_math')
        ex   = root.find('execution')
        overlap_bars = int(ws.find('overlap_bars').text)
        window_size = int(ws.find('window_size').text)
        overlap_ratio_el = ws.find('overlap_ratio')
        overlap_ratio = (
            float(overlap_ratio_el.text)
            if overlap_ratio_el is not None
            else (overlap_bars / window_size if window_size else 0.0)
        )
        return cls(
            window_type    = ws.find('window_type').text,
            window_size    = window_size,
            slide_velocity = int(ws.find('slide_velocity').text),
            overlap_bars   = overlap_bars,
            overlap_ratio  = overlap_ratio,
            warmup_bars    = int(ws.find('warmup_bars').text),
            min_bars       = int(ws.find('min_bars_required').text),
            bar_size       = int(gr.find('bar_size').text),
            bars_per_day   = int(gr.find('bars_per_trading_day').text),
            market_open    = gr.find('trading_hours/market_open').text,
            market_close   = gr.find('trading_hours/market_close').text,
            total_windows  = int(wm.find('total_windows_per_day').text),
            mode           = ex.find('mode').text,
            max_windows    = int(ex.find('max_windows').text),
            log_every      = int(ex.find('log_every_n_windows').text),
        )

    def summary(self):
        print("=" * 55)
        print("  WINDOW CONFIGURATION")
        print("=" * 55)
        print(f"  Type           : {self.window_type}")
        print(f"  Window Size    : {self.window_size} min  ({self.window_size} bars)")
        print(f"  Slide Velocity : {self.slide_velocity} min")
        print(f"  Overlap        : {self.overlap_bars} bars  ({self.overlap_ratio}x ratio)")
        print(f"  Warmup         : {self.warmup_bars} bars")
        print(f"  Bar Grain      : {self.bar_size} minute(s)")
        print(f"  Bars / Day     : {self.bars_per_day}")
        print(f"  Windows / Day  : {self.total_windows}")
        print(f"  Mode           : {self.mode}")
        print("=" * 55)


# ═══════════════════════════════════════════════════════════
# 2. SCHEMA CONFIG  ←  schema_config.xml
# ═══════════════════════════════════════════════════════════

@dataclass
class ColumnDef:
    name:        str
    dtype:       str
    category:    str
    key:         str
    nullable:    bool
    source:      str
    description: str
    extra:       Dict[str, str] = field(default_factory=dict)


@dataclass
class DimensionDef:
    name:        str
    ordinal:     int
    description: str
    columns:     List[ColumnDef]
    hierarchy:   List[Dict]


@dataclass
class FactDef:
    name:        str
    description: str
    columns:     List[ColumnDef]
    foreign_keys: List[Dict]
    indexes:     List[Dict]


@dataclass
class SchemaConfig:
    database:    str
    dimensions:  List[DimensionDef]
    fact:        FactDef

    @classmethod
    def load(cls, path: str = 'schema_config.xml') -> 'SchemaConfig':
        root = ET.parse(path).getroot()
        db   = root.get('database', 'mysql')

        # ── Parse dimensions ──────────────────────────────
        dims = []
        for d in root.findall('dimensions/dimension'):
            cols = []
            for c in d.findall('columns/column'):
                extras = {k: v for k, v in c.attrib.items()
                          if k not in ('name','type','category','key','nullable','source','description')}
                cols.append(ColumnDef(
                    name        = c.get('name'),
                    dtype       = c.get('type'),
                    category    = c.get('category', ''),
                    key         = c.get('key', ''),
                    nullable    = c.get('nullable', 'true') == 'true',
                    source      = c.get('source', ''),
                    description = c.get('description', ''),
                    extra       = extras,
                ))
            hier = []
            for lv in d.findall('hierarchy/level'):
                hier.append({'ordinal': int(lv.get('ordinal')),
                             'column':  lv.get('column'),
                             'label':   lv.get('label')})
            dims.append(DimensionDef(
                name        = d.get('name'),
                ordinal     = int(d.get('ordinal', 0)),
                description = d.get('description', ''),
                columns     = cols,
                hierarchy   = hier,
            ))

        # ── Parse fact table ───────────────────────────────
        ft   = root.find('fact_table')
        fcols = []
        for c in ft.findall('columns/column'):
            extras = {k: v for k, v in c.attrib.items()
                      if k not in ('name','type','category','key','nullable','source','description')}
            fcols.append(ColumnDef(
                name        = c.get('name'),
                dtype       = c.get('type'),
                category    = c.get('category', ''),
                key         = c.get('key', ''),
                nullable    = c.get('nullable', 'true') == 'true',
                source      = c.get('source', ''),
                description = c.get('description', ''),
                extra       = extras,
            ))
        fks = [{'column': fk.get('column'), 'references': fk.get('references')}
               for fk in ft.findall('foreign_keys/fk')]
        idxs = [{'name': ix.get('name'), 'columns': ix.get('columns'), 'type': ix.get('type')}
                for ix in ft.findall('indexes/index')]

        fact = FactDef(
            name         = ft.get('name'),
            description  = ft.get('description', ''),
            columns      = fcols,
            foreign_keys = fks,
            indexes      = idxs,
        )

        return cls(database=db, dimensions=dims, fact=fact)

    # ── DDL generators ────────────────────────────────────

    def generate_dimension_ddl(self, dim: DimensionDef) -> str:
        """Auto-generates CREATE TABLE SQL for a dimension from XML."""
        lines = []
        for col in dim.columns:
            dtype = self._normalize_dtype(col.dtype)
            null  = '' if col.nullable else ' NOT NULL'
            pk    = ' PRIMARY KEY' if col.key == 'PK' else ''
            lines.append(f"    {col.name:<28} {dtype}{pk}{null}")
        cols_sql = ',\n'.join(lines)
        return f"CREATE TABLE IF NOT EXISTS {dim.name} (\n{cols_sql}\n);"

    @staticmethod
    def _normalize_dtype(dtype: str) -> str:
        dtype_upper = dtype.upper()
        if dtype_upper == 'BIGSERIAL':
            return 'BIGINT AUTO_INCREMENT'
        if dtype_upper.startswith('NUMERIC('):
            return dtype_upper.replace('NUMERIC', 'DECIMAL', 1)
        if dtype_upper == 'BOOLEAN':
            return 'TINYINT(1)'
        return dtype

    def generate_fact_ddl(self) -> str:
        """Auto-generates CREATE TABLE SQL for the fact table from XML."""
        lines = []
        for col in self.fact.columns:
            dtype = self._normalize_dtype(col.dtype)
            null = '' if col.nullable else ' NOT NULL'
            pk   = ' PRIMARY KEY' if col.key == 'PK' else ''
            ref  = ''
            for fk in self.fact.foreign_keys:
                if fk['column'] == col.name:
                    ref = f' REFERENCES {fk["references"]}'
                    break
            lines.append(f"    {col.name:<28} {dtype}{pk}{null}{ref}")
        cols_sql = ',\n'.join(lines)
        idx_sql = '\n'.join(
            f"CREATE INDEX {ix['name']} ON {self.fact.name}({ix['columns']});"
            for ix in self.fact.indexes
        )
        return (f"CREATE TABLE IF NOT EXISTS {self.fact.name} (\n{cols_sql}\n);\n\n"
                f"-- Indexes\n{idx_sql}")

    def generate_all_ddl(self) -> str:
        """Generates complete DDL for all dimensions + fact table."""
        parts = ["-- AUTO-GENERATED DDL from schema_config.xml\n"]
        for dim in sorted(self.dimensions, key=lambda d: d.ordinal):
            parts.append(f"-- {dim.name.upper()}: {dim.description}")
            parts.append(self.generate_dimension_ddl(dim))
            parts.append("")
        parts.append(f"-- {self.fact.name.upper()}: {self.fact.description}")
        parts.append(self.generate_fact_ddl())
        return '\n'.join(parts)

    def summary(self):
        print("=" * 55)
        print("  SCHEMA CONFIGURATION")
        print("=" * 55)
        print(f"  Database   : {self.database}")
        print(f"  Dimensions : {len(self.dimensions)}")
        for d in self.dimensions:
            print(f"    [{d.ordinal}] {d.name:<20} ({len(d.columns)} columns)")
        print(f"  Fact Table : {self.fact.name} ({len(self.fact.columns)} columns)")
        print("=" * 55)


# ═══════════════════════════════════════════════════════════
# 3. LATTICE QUERY CONFIG  ←  lattice_queries.xml
# ═══════════════════════════════════════════════════════════

@dataclass
class QueryDef:
    qid:         str
    category:    str
    title:       str
    description: str
    sql:         str
    enabled:     bool
    params:      Dict[str, str] = field(default_factory=dict)


@dataclass
class QueryConfig:
    thresholds: Dict[str, float]
    queries:    List[QueryDef]

    @classmethod
    def load(cls, path: str = 'lattice_queries.xml') -> 'QueryConfig':
        try:
            root = ET.parse(path).getroot()
        except (ET.ParseError, FileNotFoundError) as e:
            print(f"WARNING: Could not load query config '{path}': {e}")
            print("         Continuing with 0 lattice queries.")
            return cls(thresholds={}, queries=[])

        # Global thresholds (not used by lattice_queries.xml)
        thresholds = {}
        for t in root.findall('global_thresholds/threshold'):
            thresholds[t.get('name')] = float(t.get('value'))

        # All queries across classic categories (if present in future configs)
        queries = []
        for cat in root.findall('category'):
            cat_name = cat.get('name')
            for q in cat.findall('query'):
                sql_el = q.find('sql')
                sql    = sql_el.text.strip() if sql_el is not None else ''
                params = {}
                for p in q.findall('params/param'):
                    params[p.get('name')] = p.get('default', '')
                queries.append(QueryDef(
                    qid         = q.get('id'),
                    category    = cat_name,
                    title       = q.get('title'),
                    description = q.get('description', ''),
                    sql         = sql,
                    enabled     = q.get('enabled', 'true') == 'true',
                    params      = params,
                ))

        # Lattice/cuboid query format (lattice_queries.xml)
        for cuboid in root.findall('cuboid'):
            cuboid_id = cuboid.get('id', '')
            cuboid_name = cuboid.get('name', 'Cuboid')
            cat_name = f"{cuboid_id}:{cuboid_name}" if cuboid_id else cuboid_name
            for q in cuboid.findall('query'):
                sql_el = q.find('sql')
                sql = sql_el.text.strip() if sql_el is not None and sql_el.text else ''
                queries.append(QueryDef(
                    qid         = q.get('id'),
                    category    = cat_name,
                    title       = q.get('title', ''),
                    description = q.get('description', ''),
                    sql         = sql,
                    enabled     = q.get('enabled', 'true') == 'true',
                    params      = {},
                ))

        return cls(thresholds=thresholds, queries=queries)

    def enabled_queries(self) -> List[QueryDef]:
        return [q for q in self.queries if q.enabled]

    def enabled_window_scoped_queries(self) -> List[QueryDef]:
        """Return only enabled queries scoped to a single window (:win_id)."""
        result = []
        for q in self.enabled_queries():
            sql = q.sql
            if 'dw.window_id = :win_id' not in sql:
                continue
            if ':win_id - 1' in sql:
                continue
            if 'IN (:win_id' in sql:
                continue
            result.append(q)
        return result

    def get_query(self, qid: str) -> Optional[QueryDef]:
        return next((q for q in self.queries if q.qid == qid), None)

    def summary(self):
        print("=" * 55)
        print("  QUERY CONFIGURATION")
        print("=" * 55)
        cats = {}
        for q in self.queries:
            cats.setdefault(q.category, []).append(q)
        for cat, qs in cats.items():
            enabled = sum(1 for q in qs if q.enabled)
            print(f"  {cat:<20} : {enabled}/{len(qs)} queries enabled")
        print(f"  Total enabled  : {len(self.enabled_queries())}/{len(self.queries)}")
        print(f"  Window scoped  : {len(self.enabled_window_scoped_queries())}")
        print("  Thresholds:")
        for k, v in self.thresholds.items():
            print(f"    {k:<30} = {v}")
        print("=" * 55)


# ═══════════════════════════════════════════════════════════
# 4. PIPELINE CONFIG  ←  pipeline_config.xml
# ═══════════════════════════════════════════════════════════

@dataclass
class PipelineConfig:
    csv_path:      str
    db_engine:     str
    db_host:       str
    db_port:       int
    db_name:       str
    db_user:       str
    db_password_env: str
    redis_host:    Optional[str]
    redis_port:    Optional[int]
    redis_maxlen:  Optional[int]
    batch_path:    Optional[str]
    log_level:     str
    indicators:    List[Dict]
    stock_master_csv_path: Optional[str]
    auto_create_loc_tables: bool
    loc_schema_sql_file: Optional[str]
    materialize_loc_tables: bool
    prune_after_loc_update: bool
    fact_retention_keep_latest: int

    @classmethod
    def load(cls, path: str = 'pipeline_config.xml') -> 'PipelineConfig':
        root = ET.parse(path).getroot()
        db   = root.find('database')
        sl   = root.find('lambda_architecture/speed_layer')
        bl   = root.find('lambda_architecture/batch_layer')
        log  = root.find('logging')
        loc  = root.find('lattice_materialization')
        sm   = root.find('stock_master')

        indicators = []
        for ind in root.findall('indicators/indicator'):
            indicators.append(dict(ind.attrib))

        def _as_bool(v: Optional[str], default: bool) -> bool:
            if v is None:
                return default
            return v.strip().lower() in ('1', 'true', 'yes', 'y', 'on')

        auto_create = _as_bool(
            loc.findtext('auto_create_loc_tables') if loc is not None else None,
            True,
        )
        materialize = _as_bool(
            loc.findtext('materialize_loc_tables') if loc is not None else None,
            True,
        )
        prune_after = _as_bool(
            loc.findtext('prune_after_loc_update') if loc is not None else None,
            True,
        )
        keep_latest = int(
            (loc.findtext('fact_retention_keep_latest') if loc is not None else None)
            or 30
        )
        loc_schema_sql_file = (
            loc.findtext('loc_schema_sql_file') if loc is not None else 'lattice_cuboid_tables_stock_hierarchy.sql'
        )

        return cls(
            csv_path         = root.find('csv_source/file_path').text,
            db_engine        = db.get('engine', 'mysql'),
            db_host          = db.find('host').text,
            db_port          = int(db.find('port').text),
            db_name          = db.find('dbname').text,
            db_user          = db.find('user').text,
            db_password_env  = db.find('password_env_var').text,
            redis_host       = sl.find('host').text if sl is not None and sl.find('host') is not None else None,
            redis_port       = int(sl.find('port').text) if sl is not None and sl.find('port') is not None else None,
            redis_maxlen     = int(sl.find('stream_maxlen').text) if sl is not None and sl.find('stream_maxlen') is not None else None,
            batch_path       = bl.find('output_path').text if bl is not None and bl.find('output_path') is not None else None,
            log_level        = log.find('level').text,
            indicators       = indicators,
            stock_master_csv_path = sm.findtext('file_path') if sm is not None else None,
            auto_create_loc_tables = auto_create,
            loc_schema_sql_file = loc_schema_sql_file,
            materialize_loc_tables = materialize,
            prune_after_loc_update = prune_after,
            fact_retention_keep_latest = keep_latest,
        )

    def summary(self):
        print("=" * 55)
        print("  PIPELINE CONFIGURATION")
        print("=" * 55)
        print(f"  CSV Source  : {self.csv_path}")
        print(f"  DB          : {self.db_engine}://{self.db_user}@{self.db_host}:{self.db_port}/{self.db_name}")
        if self.redis_host and self.redis_port is not None:
            print(f"  Redis       : {self.redis_host}:{self.redis_port} (maxlen={self.redis_maxlen})")
        else:
            print("  Redis       : disabled")
        print(f"  Batch Store : {self.batch_path if self.batch_path else 'disabled'}")
        print(f"  Stock Master: {self.stock_master_csv_path if self.stock_master_csv_path else 'disabled'}")
        print(f"  LOC Create  : {'enabled' if self.auto_create_loc_tables else 'disabled'}")
        print(f"  LOC SQL     : {self.loc_schema_sql_file if self.loc_schema_sql_file else 'disabled'}")
        print(f"  LOC Update  : {'enabled' if self.materialize_loc_tables else 'disabled'}")
        print(f"  Fact Keep N : {self.fact_retention_keep_latest}")
        print(f"  Prune After : {'enabled' if self.prune_after_loc_update else 'disabled'}")
        print(f"  Log Level   : {self.log_level}")
        print(f"  Indicators  : {len(self.indicators)} defined")
        for ind in self.indicators:
            print(f"    {ind['name']:<12} type={ind['type']} lookback={ind.get('lookback', ind.get('span', ind.get('periods', '?')))}")
        print("=" * 55)


# ═══════════════════════════════════════════════════════════
# DEMO — run directly to verify all XML files load correctly
# ═══════════════════════════════════════════════════════════
if __name__ == '__main__':
    import os
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    print("\n>>> Loading window_config.xml")
    wc = WindowConfig.load('window_config.xml')
    wc.summary()

    print("\n>>> Loading schema_config.xml")
    sc = SchemaConfig.load('schema_config.xml')
    sc.summary()

    print("\n>>> Loading lattice_queries.xml")
    qc = QueryConfig.load('lattice_queries.xml')
    qc.summary()

    print("\n>>> Loading pipeline_config.xml")
    pc = PipelineConfig.load('pipeline_config.xml')
    pc.summary()

    print("\n>>> Auto-generated DDL from schema_config.xml:")
    print("-" * 55)
    print(sc.generate_all_ddl())

    print("\n>>> Sample: C0_Q1 SQL loaded from lattice_queries.xml:")
    print("-" * 55)
    q4 = qc.get_query('C0_Q1')
    print(f"  ID       : {q4.qid}")
    print(f"  Category : {q4.category}")
    print(f"  Title    : {q4.title}")
    print(f"  SQL      :\n{q4.sql[:300]}...")
