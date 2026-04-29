USE stock_dw;

/*
  New LOC lattice based on 3 stock hierarchy attributes:
  {company_name, sector, industry}  => 2^3 = 8 cuboids

  Measure columns kept as requested:
    volume_sum, volume_avg, sma_30_sum, sma_30_avg, ticker_no, tick_timestamp

  NOTE:
    fact_stock_bar currently has sma_20 (not sma_30).
    For now, sma_30_* is populated from sma_20 aggregations.
*/

/* ============================
    Table creation only (non-destructive)
    ============================ */

/* ============================
   C0: {}  (Apex)
   ============================ */
CREATE TABLE IF NOT EXISTS `loc_apex` (
    `id`             BIGINT        NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `volume_sum`     BIGINT        NULL,
    `volume_avg`     DECIMAL(18,4) NULL,
    `sma_30_sum`     DECIMAL(18,4) NULL,
    `sma_30_avg`     DECIMAL(18,4) NULL,
    `ticker_no`      BIGINT        NULL,
    `tick_timestamp` DATETIME      NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

/* ============================
   C1: {company_name}
   ============================ */
CREATE TABLE IF NOT EXISTS `loc_company` (
    `id`             BIGINT         NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `company_name`   VARCHAR(100)   NOT NULL,
    `volume_sum`     BIGINT         NULL,
    `volume_avg`     DECIMAL(18,4)  NULL,
    `sma_30_sum`     DECIMAL(18,4)  NULL,
    `sma_30_avg`     DECIMAL(18,4)  NULL,
    `ticker_no`      BIGINT         NULL,
    `tick_timestamp` DATETIME       NULL,
    UNIQUE KEY `uq_loc_company` (`company_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

/* ============================
   C1: {sector}
   ============================ */
CREATE TABLE IF NOT EXISTS `loc_sector` (
    `id`             BIGINT         NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `sector`         VARCHAR(50)    NOT NULL,
    `volume_sum`     BIGINT         NULL,
    `volume_avg`     DECIMAL(18,4)  NULL,
    `sma_30_sum`     DECIMAL(18,4)  NULL,
    `sma_30_avg`     DECIMAL(18,4)  NULL,
    `ticker_no`      BIGINT         NULL,
    `tick_timestamp` DATETIME       NULL,
    UNIQUE KEY `uq_loc_sector` (`sector`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

/* ============================
   C1: {industry}
   ============================ */
CREATE TABLE IF NOT EXISTS `loc_industry` (
    `id`             BIGINT         NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `industry`       VARCHAR(80)    NOT NULL,
    `volume_sum`     BIGINT         NULL,
    `volume_avg`     DECIMAL(18,4)  NULL,
    `sma_30_sum`     DECIMAL(18,4)  NULL,
    `sma_30_avg`     DECIMAL(18,4)  NULL,
    `ticker_no`      BIGINT         NULL,
    `tick_timestamp` DATETIME       NULL,
    UNIQUE KEY `uq_loc_industry` (`industry`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

/* ============================
   C2: {company_name, sector}
   ============================ */
CREATE TABLE IF NOT EXISTS `loc_company_sector` (
    `id`             BIGINT         NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `company_name`   VARCHAR(100)   NOT NULL,
    `sector`         VARCHAR(50)    NOT NULL,
    `volume_sum`     BIGINT         NULL,
    `volume_avg`     DECIMAL(18,4)  NULL,
    `sma_30_sum`     DECIMAL(18,4)  NULL,
    `sma_30_avg`     DECIMAL(18,4)  NULL,
    `ticker_no`      BIGINT         NULL,
    `tick_timestamp` DATETIME       NULL,
    UNIQUE KEY `uq_loc_company_sector` (`company_name`, `sector`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

/* ============================
   C2: {company_name, industry}
   ============================ */
CREATE TABLE IF NOT EXISTS `loc_company_industry` (
    `id`             BIGINT         NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `company_name`   VARCHAR(100)   NOT NULL,
    `industry`       VARCHAR(80)    NOT NULL,
    `volume_sum`     BIGINT         NULL,
    `volume_avg`     DECIMAL(18,4)  NULL,
    `sma_30_sum`     DECIMAL(18,4)  NULL,
    `sma_30_avg`     DECIMAL(18,4)  NULL,
    `ticker_no`      BIGINT         NULL,
    `tick_timestamp` DATETIME       NULL,
    UNIQUE KEY `uq_loc_company_industry` (`company_name`, `industry`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

/* ============================
   C2: {sector, industry}
   ============================ */
CREATE TABLE IF NOT EXISTS `loc_sector_industry` (
    `id`             BIGINT         NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `sector`         VARCHAR(50)    NOT NULL,
    `industry`       VARCHAR(80)    NOT NULL,
    `volume_sum`     BIGINT         NULL,
    `volume_avg`     DECIMAL(18,4)  NULL,
    `sma_30_sum`     DECIMAL(18,4)  NULL,
    `sma_30_avg`     DECIMAL(18,4)  NULL,
    `ticker_no`      BIGINT         NULL,
    `tick_timestamp` DATETIME       NULL,
    UNIQUE KEY `uq_loc_sector_industry` (`sector`, `industry`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

/* ============================
   C3: {company_name, sector, industry}
   ============================ */
CREATE TABLE IF NOT EXISTS `loc_company_sector_industry` (
    `id`             BIGINT         NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `company_name`   VARCHAR(100)   NOT NULL,
    `sector`         VARCHAR(50)    NOT NULL,
    `industry`       VARCHAR(80)    NOT NULL,
    `volume_sum`     BIGINT         NULL,
    `volume_avg`     DECIMAL(18,4)  NULL,
    `sma_30_sum`     DECIMAL(18,4)  NULL,
    `sma_30_avg`     DECIMAL(18,4)  NULL,
    `ticker_no`      BIGINT         NULL,
    `tick_timestamp` DATETIME       NULL,
    UNIQUE KEY `uq_loc_company_sector_industry` (`company_name`, `sector`, `industry`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


/* ==========================================================
   MATERIALIZATION QUERIES (join + group by from fact+dim_stock)
   COMMENTED OUT FOR NOW
   ========================================================== */

/* C0
INSERT INTO `loc_apex` (
    volume_sum, volume_avg, sma_30_sum, sma_30_avg, ticker_no, tick_timestamp
)
SELECT
    SUM(f.volume)                                   AS volume_sum,
    AVG(f.volume)                                   AS volume_avg,
    SUM(f.sma_20)                                   AS sma_30_sum,
    AVG(f.sma_20)                                   AS sma_30_avg,
    COUNT(*)                                        AS ticker_no,
    MAX(f.ingestion_ts)                             AS tick_timestamp
FROM fact_stock_bar f;

 C1 company
INSERT INTO `loc_company` (
    company_name, volume_sum, volume_avg, sma_30_sum, sma_30_avg, ticker_no, tick_timestamp
)
SELECT
    ds.company_name,
    SUM(f.volume),
    AVG(f.volume),
    SUM(f.sma_20),
    AVG(f.sma_20),
    COUNT(*),
    MAX(f.ingestion_ts)
FROM fact_stock_bar f
JOIN dim_stock ds ON ds.stock_key = f.stock_key
GROUP BY ds.company_name;

 C1 sector
INSERT INTO `loc_sector` (
    sector, volume_sum, volume_avg, sma_30_sum, sma_30_avg, ticker_no, tick_timestamp
)
SELECT
    ds.sector,
    SUM(f.volume),
    AVG(f.volume),
    SUM(f.sma_20),
    AVG(f.sma_20),
    COUNT(*),
    MAX(f.ingestion_ts)
FROM fact_stock_bar f
JOIN dim_stock ds ON ds.stock_key = f.stock_key
GROUP BY ds.sector;

 C1 industry
INSERT INTO `loc_industry` (
    industry, volume_sum, volume_avg, sma_30_sum, sma_30_avg, ticker_no, tick_timestamp
)
SELECT
    ds.industry,
    SUM(f.volume),
    AVG(f.volume),
    SUM(f.sma_20),
    AVG(f.sma_20),
    COUNT(*),
    MAX(f.ingestion_ts)
FROM fact_stock_bar f
JOIN dim_stock ds ON ds.stock_key = f.stock_key
GROUP BY ds.industry;

 C2 company-sector
INSERT INTO `loc_company_sector` (
    company_name, sector, volume_sum, volume_avg, sma_30_sum, sma_30_avg, ticker_no, tick_timestamp
)
SELECT
    ds.company_name,
    ds.sector,
    SUM(f.volume),
    AVG(f.volume),
    SUM(f.sma_20),
    AVG(f.sma_20),
    COUNT(*),
    MAX(f.ingestion_ts)
FROM fact_stock_bar f
JOIN dim_stock ds ON ds.stock_key = f.stock_key
GROUP BY ds.company_name, ds.sector;

 C2 company-industry
INSERT INTO `loc_company_industry` (
    company_name, industry, volume_sum, volume_avg, sma_30_sum, sma_30_avg, ticker_no, tick_timestamp
)
SELECT
    ds.company_name,
    ds.industry,
    SUM(f.volume),
    AVG(f.volume),
    SUM(f.sma_20),
    AVG(f.sma_20),
    COUNT(*),
    MAX(f.ingestion_ts)
FROM fact_stock_bar f
JOIN dim_stock ds ON ds.stock_key = f.stock_key
GROUP BY ds.company_name, ds.industry;

 C2 sector-industry
INSERT INTO `loc_sector_industry` (
    sector, industry, volume_sum, volume_avg, sma_30_sum, sma_30_avg, ticker_no, tick_timestamp
)
SELECT
    ds.sector,
    ds.industry,
    SUM(f.volume),
    AVG(f.volume),
    SUM(f.sma_20),
    AVG(f.sma_20),
    COUNT(*),
    MAX(f.ingestion_ts)
FROM fact_stock_bar f
JOIN dim_stock ds ON ds.stock_key = f.stock_key
GROUP BY ds.sector, ds.industry;

 C3 company-sector-industry
INSERT INTO `loc_company_sector_industry` (
    company_name, sector, industry, volume_sum, volume_avg, sma_30_sum, sma_30_avg, ticker_no, tick_timestamp
)
SELECT
    ds.company_name,
    ds.sector,
    ds.industry,
    SUM(f.volume),
    AVG(f.volume),
    SUM(f.sma_20),
    AVG(f.sma_20),
    COUNT(*),
    MAX(f.ingestion_ts)
FROM fact_stock_bar f
JOIN dim_stock ds ON ds.stock_key = f.stock_key
GROUP BY ds.company_name, ds.sector, ds.industry;
*/
