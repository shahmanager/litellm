SET LOCAL lock_timeout = '5s';
ALTER TABLE "LiteLLM_AccountingDelivery" ADD COLUMN "legacy_takeover" BOOLEAN NOT NULL DEFAULT false;
