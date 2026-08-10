-- 137_worker_validation_details.sql
-- Complete the worker_validations schema for the validation report repository.

ALTER TABLE worker_validations
    ADD COLUMN exec_start TEXT;

ALTER TABLE worker_validations
    ADD COLUMN validated_at TEXT;

ALTER TABLE worker_validations
    ADD COLUMN failure_reason TEXT;

ALTER TABLE worker_validations
    ADD COLUMN created_at TEXT NOT NULL DEFAULT '';

ALTER TABLE worker_validations
    ADD COLUMN updated_at TEXT NOT NULL DEFAULT '';
