-- 0030_execution_step_domain_parity: align the persisted timeline schema with
-- ExecutionStep, whose workflow node key is optional and whose initial/skipped
-- attempt count is zero.
ALTER TABLE execution_steps
    ALTER COLUMN node_key DROP NOT NULL;

ALTER TABLE execution_steps
    DROP CONSTRAINT execution_steps_attempt_check,
    ADD CONSTRAINT execution_steps_attempt_nonnegative CHECK (attempt >= 0);
