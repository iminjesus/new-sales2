-- ============================================================
-- Rebate structure tables
-- ============================================================

-- 1. Customer → rebate structure mapping
CREATE TABLE IF NOT EXISTS rebate_customer_map (
    id            INT AUTO_INCREMENT PRIMARY KEY,
    sold_to       VARCHAR(20)  NOT NULL,
    brand         ENUM('HK','LF') NOT NULL,
    structure_name VARCHAR(60) NOT NULL,
    UNIQUE KEY uq_sold_to_brand (sold_to, brand)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2. Rebate tier definitions
--    period: A=Annual, Q=Quarterly
--    unit:   A=Amount($), Q=Quantity(units)
CREATE TABLE IF NOT EXISTS rebate_structure (
    id             INT AUTO_INCREMENT PRIMARY KEY,
    structure_name VARCHAR(60)  NOT NULL,
    brand          ENUM('HK','LF') NOT NULL,
    period         ENUM('A','Q')   NOT NULL,
    unit           ENUM('A','Q')   NOT NULL,
    tier_order     TINYINT         NOT NULL,   -- 1..15
    threshold      DECIMAL(14,2)   NOT NULL,
    rate           DECIMAL(6,3)    NOT NULL,   -- e.g. 14.0 means 14%
    UNIQUE KEY uq_struct_tier (structure_name, tier_order)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

