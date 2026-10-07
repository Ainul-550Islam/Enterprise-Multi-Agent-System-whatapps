-- The execution worker persists timeline rows after a run. 0017 granted the
-- worker plane SELECT/UPDATE but omitted INSERT, so the first step row could
-- never be written by that least-privilege runtime role.
GRANT INSERT ON execution_steps TO mas_worker;
