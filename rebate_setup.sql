-- ============================================================
-- Rebate structure tables
-- ============================================================

-- 1. Customer → rebate structure mapping
--    territory: TTL = all-territory combined, HK = HK only, LF = LF only
CREATE TABLE IF NOT EXISTS rebate_customer_map (
    id            INT AUTO_INCREMENT PRIMARY KEY,
    sold_to       VARCHAR(20)  NOT NULL,
    territory     VARCHAR(4)   NOT NULL,   -- 'TTL', 'HK', 'LF'
    structure_name VARCHAR(80) NOT NULL,
    UNIQUE KEY uq_sold_to_territory (sold_to, territory, structure_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2. Rebate tier definitions
--    unit: A = Amount ($), Q = Quantity (units)
CREATE TABLE IF NOT EXISTS rebate_structure (
    id             INT AUTO_INCREMENT PRIMARY KEY,
    structure_name VARCHAR(80)  NOT NULL,
    unit           VARCHAR(1)   NOT NULL,   -- 'A' or 'Q'
    tier_order     TINYINT      NOT NULL,   -- 0..14 (0 = base/zero tier)
    threshold      DECIMAL(14,2) NOT NULL,  -- $ amount or unit count
    rate           DECIMAL(6,3) NOT NULL,   -- rebate % e.g. 14.0
    UNIQUE KEY uq_struct_tier (structure_name, tier_order)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
