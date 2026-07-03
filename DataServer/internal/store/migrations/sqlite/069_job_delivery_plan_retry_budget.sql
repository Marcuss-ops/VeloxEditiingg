-- 069_job_delivery_plan_retry_budget.sql
-- Add per-plan retry_budget so DeliveryPlanResolver and FinalizeVerified
-- can stamp durable delivery attempt caps onto job_deliveries.

ALTER TABLE job_delivery_plans
ADD COLUMN retry_budget INTEGER NOT NULL DEFAULT 5;
