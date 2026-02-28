-- Migration: Replace base_token_type (SMALLINT enum) with base_token_address (VARCHAR(42) lowercase hex)
-- Database: marketcenter
-- Date: 2026-03-01
--
-- Replace {USDC_ADDRESS} and {POINTS_ADDRESS} with actual deployed contract addresses (lowercase hex).
-- Example:
--   USDC:   0x1a8da4723a4f7ad0a965cb3dbc31c72435b1b490
--   Points: 0x934e616ca9538397e3f9a12895c40ea2e8c45e48

-- ============================================================
-- marketcenter database
-- ============================================================

BEGIN;

-- 1. t_market (old column: token_type)
ALTER TABLE t_market ADD COLUMN base_token_address VARCHAR(42) NOT NULL DEFAULT '';
UPDATE t_market SET base_token_address = CASE
    WHEN token_type = 2 THEN '{USDC_ADDRESS}'
    ELSE '{POINTS_ADDRESS}'
END;
ALTER TABLE t_market DROP COLUMN token_type;

-- 2. t_option
ALTER TABLE t_option ADD COLUMN base_token_address VARCHAR(42) NOT NULL DEFAULT '';
UPDATE t_option SET base_token_address = CASE
    WHEN base_token_type = 2 THEN '{USDC_ADDRESS}'
    ELSE '{POINTS_ADDRESS}'
END;
ALTER TABLE t_option DROP COLUMN base_token_type;

-- 3. t_option_token_price
ALTER TABLE t_option_token_price ADD COLUMN base_token_address VARCHAR(42) NOT NULL DEFAULT '';
UPDATE t_option_token_price SET base_token_address = CASE
    WHEN base_token_type = 2 THEN '{USDC_ADDRESS}'
    ELSE '{POINTS_ADDRESS}'
END;
ALTER TABLE t_option_token_price DROP COLUMN base_token_type;

-- 4. t_user_market_follow
ALTER TABLE t_user_market_follow ADD COLUMN base_token_address VARCHAR(42) NOT NULL DEFAULT '';
UPDATE t_user_market_follow SET base_token_address = CASE
    WHEN base_token_type = 2 THEN '{USDC_ADDRESS}'
    ELSE '{POINTS_ADDRESS}'
END;
ALTER TABLE t_user_market_follow DROP COLUMN base_token_type;

-- 5. t_user_token_balance
ALTER TABLE t_user_token_balance ADD COLUMN base_token_address VARCHAR(42) NOT NULL DEFAULT '';
UPDATE t_user_token_balance SET base_token_address = CASE
    WHEN base_token_type = 2 THEN '{USDC_ADDRESS}'
    ELSE '{POINTS_ADDRESS}'
END;
ALTER TABLE t_user_token_balance DROP COLUMN base_token_type;

-- 6. t_order
ALTER TABLE t_order ADD COLUMN base_token_address VARCHAR(42) NOT NULL DEFAULT '';
UPDATE t_order SET base_token_address = CASE
    WHEN base_token_type = 2 THEN '{USDC_ADDRESS}'
    ELSE '{POINTS_ADDRESS}'
END;
ALTER TABLE t_order DROP COLUMN base_token_type;

-- 7. t_user_claim_result
ALTER TABLE t_user_claim_result ADD COLUMN base_token_address VARCHAR(42) NOT NULL DEFAULT '';
UPDATE t_user_claim_result SET base_token_address = CASE
    WHEN base_token_type = 2 THEN '{USDC_ADDRESS}'
    ELSE '{POINTS_ADDRESS}'
END;
ALTER TABLE t_user_claim_result DROP COLUMN base_token_type;

-- 8. t_user_asset_value
ALTER TABLE t_user_asset_value ADD COLUMN base_token_address VARCHAR(42) NOT NULL DEFAULT '';
UPDATE t_user_asset_value SET base_token_address = CASE
    WHEN base_token_type = 2 THEN '{USDC_ADDRESS}'
    ELSE '{POINTS_ADDRESS}'
END;
ALTER TABLE t_user_asset_value DROP COLUMN base_token_type;

-- 9. t_send_tx
ALTER TABLE t_send_tx ADD COLUMN base_token_address VARCHAR(42) NOT NULL DEFAULT '';
UPDATE t_send_tx SET base_token_address = CASE
    WHEN base_token_type = 2 THEN '{USDC_ADDRESS}'
    ELSE '{POINTS_ADDRESS}'
END;
ALTER TABLE t_send_tx DROP COLUMN base_token_type;

-- 10. t_user_transfer_tokens
ALTER TABLE t_user_transfer_tokens ADD COLUMN base_token_address VARCHAR(42) NOT NULL DEFAULT '';
UPDATE t_user_transfer_tokens SET base_token_address = CASE
    WHEN base_token_type = 2 THEN '{USDC_ADDRESS}'
    ELSE '{POINTS_ADDRESS}'
END;
ALTER TABLE t_user_transfer_tokens DROP COLUMN base_token_type;

-- 11. t_user_mint_points
ALTER TABLE t_user_mint_points ADD COLUMN base_token_address VARCHAR(42) NOT NULL DEFAULT '';
UPDATE t_user_mint_points SET base_token_address = CASE
    WHEN base_token_type = 2 THEN '{USDC_ADDRESS}'
    ELSE '{POINTS_ADDRESS}'
END;
ALTER TABLE t_user_mint_points DROP COLUMN base_token_type;

-- 12. t_user_market_position
ALTER TABLE t_user_market_position ADD COLUMN base_token_address VARCHAR(42) NOT NULL DEFAULT '';
UPDATE t_user_market_position SET base_token_address = CASE
    WHEN base_token_type = 2 THEN '{USDC_ADDRESS}'
    ELSE '{POINTS_ADDRESS}'
END;
ALTER TABLE t_user_market_position DROP COLUMN base_token_type;

COMMIT;

-- ============================================================
-- usercenter database (separate connection required)
-- ============================================================

BEGIN;

-- 13. t_user_notification
ALTER TABLE t_user_notification ADD COLUMN base_token_address VARCHAR(42) NOT NULL DEFAULT '';
UPDATE t_user_notification SET base_token_address = CASE
    WHEN base_token_type = 2 THEN '{USDC_ADDRESS}'
    ELSE '{POINTS_ADDRESS}'
END;
ALTER TABLE t_user_notification DROP COLUMN base_token_type;

COMMIT;

-- ============================================================
-- Redis cleanup (run after deployment)
-- ============================================================
-- Old all-time leaderboard keys use numeric prefix (leaderboard-1-*, leaderboard-2-*)
-- They will be rebuilt automatically by crontask with address-based keys.
-- Run manually after deploy:
--   redis-cli KEYS "leaderboard-[12]-*" | xargs redis-cli DEL
