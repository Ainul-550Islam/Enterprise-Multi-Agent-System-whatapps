# REVIEW RESOLUTION — Batch 0

## C-01 through C-12 (Compliance items)

| ID  | Description          | Fix Commit               | Pramāṇy Test Name       | Status  |
|-----|----------------------|--------------------------|-------------------------|---------|
| C-01 | Tenancy: org/tenant scoping | pending | test_tenancy_scoping | open |
| C-02 | Agent lifecycle: register/get | pending | test_agent_lifecycle | open |
| C-03 | Workflow: create/publish | pending | test_workflow_lifecycle | open |
| C-04 | Execution: submit/list | pending | test_execution_lifecycle | open |
| C-05 | Policy: CRUD operations | pending | test_policy_crud | open |
| C-06 | Quota: tenant-scoped reads | pending | test_quota_reads | open |
| C-07 | Analytics: overview/metrics | pending | test_analytics_metrics | open |
| C-08 | Audit events: paginated/redacted | pending | test_audit_events | open |
| C-09 | Connectors: register/probe | pending | test_connectors | open |
| C-10 | Webhooks: enable/disable | pending | test_webhooks | open |
| C-11 | OAuth: authorization callback | pending | test_oauth_callback | open |
| C-12 | SSE stream: per-principal caps | pending | test_sse_caps | open |

## W-01 through W-08 (Warning items)

| ID  | Description          | Fix Commit               | Pramāṇy Test Name       | Status  |
|-----|----------------------|--------------------------|-------------------------|---------|
| W-01 | Duplicate: module1-full-source | pending | test_module1_duplicate | open |
| W-02 | Version mismatch: 0.1.0 → 1.0.0 | pending | test_version_alignment | open |
| W-03 | CHANGELOG: duplicate SSE/Gov lines | pending | test_changelog_dedup | open |
| W-04 | README: not-included accuracy | pending | test_readme_check | open |
| W-05 | SECURITY.md: supported versions | pending | test_security_versions | open |
| W-06 | LICENSE: lawyer review needed | pending | test_license_review | open |
| W-07 | THIRD_PARTY: regenerate notices | pending | test_third_party_regenerate | open |
| W-08 | CI: evidence matrix cleanup | pending | test_ci_evidence_cleanup | open |