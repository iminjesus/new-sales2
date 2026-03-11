-- =============================================================================
-- Migration: Normalize sales_2601 by removing line/product_group/pattern/inch.
-- These attributes now come from carrying_2602 via JOIN on m_code.
--
-- carrying_2602 already has: line, m_code, size, brand, pattern, product_group
-- No need to alter carrying_2602 — just drop redundant columns from sales tables.
-- =============================================================================

-- ── STEP 1 (optional sanity check): verify all sales_2601 materials exist in carrying_2602 ──
-- SELECT COUNT(DISTINCT s.material) AS missing
-- FROM sales_2601 s
-- LEFT JOIN carrying_2602 c ON c.m_code = s.material
-- WHERE c.m_code IS NULL;
-- If > 0, investigate before proceeding.

-- ── STEP 2: Remove redundant columns from sales_2601 ───────────────────────
ALTER TABLE sales_2601
  DROP COLUMN line,
  DROP COLUMN product_group,
  DROP COLUMN pattern,
  DROP COLUMN inch;

-- ── Later (when ready): apply same to monthly / yearly tables ───────────────
-- ALTER TABLE sales_25_2601
--   DROP COLUMN line,
--   DROP COLUMN product_group,
--   DROP COLUMN pattern,
--   DROP COLUMN inch;
--
-- ALTER TABLE sales_21_25
--   DROP COLUMN line,
--   DROP COLUMN product_group,
--   DROP COLUMN pattern,
--   DROP COLUMN inch;
