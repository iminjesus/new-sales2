-- ============================================================
-- Rebate structure tables
-- ============================================================

-- 1. Customer → rebate structure mapping
--    brand: HK = Hankook brand only, LF = LF brand only, TTL = HK+LF combined
DROP TABLE IF EXISTS rebate_customer_map;
CREATE TABLE rebate_customer_map (
    id             INT AUTO_INCREMENT PRIMARY KEY,
    sold_to        VARCHAR(20)  NOT NULL,
    brand          VARCHAR(4)   NOT NULL,   -- 'TTL', 'HK', 'LF'
    structure_name VARCHAR(80)  NOT NULL,
    UNIQUE KEY uq_sold_to_brand (sold_to, brand, structure_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2. Rebate tier definitions
--    unit:      A = Amount ($), Q = Quantity (units)
--    tier_order: 0 = base/zero tier
--    top_order:  highest meaningful tier (tiers beyond this repeat the same threshold/rate)
DROP TABLE IF EXISTS rebate_structure;
CREATE TABLE rebate_structure (
    id             INT AUTO_INCREMENT PRIMARY KEY,
    structure_name VARCHAR(80)   NOT NULL,
    unit           VARCHAR(1)    NOT NULL,   -- 'A' or 'Q'
    tier_order     TINYINT       NOT NULL,   -- 0..14
    top_order      TINYINT       NOT NULL,   -- last meaningful tier index
    threshold      DECIMAL(14,2) NOT NULL,   -- $ amount or unit count
    rate           DECIMAL(6,3)  NOT NULL,   -- rebate % e.g. 8.0
    UNIQUE KEY uq_struct_tier (structure_name, tier_order)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
