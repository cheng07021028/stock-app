V191-H155 Page17 full Page01 macro parity + same-run recommendation gate
Based on stock-app main 4d9e6a23a75bbe8f108b3abea615497da8fd7439 (2026-11-12).
1. Back up your current application.
2. Verify this base has not changed before deployment. Copy these 3 Python files to the same relative paths.
3. The existing 17 quick refresh remains available; use new full refresh for Page01 parity.
4. Full refresh requires the configured GITHUB_TOKEN GitHub Actions dispatch and runtime-data persistence.
5. Dispatch acknowledges request only; Page17 scheduler history is authority for actual results.
6. This fix does not bypass Page07 H126 authorization or promise profit.
7. Do not overwrite runtime-data, configurations, JSON, secrets, or credentials.
8. If a same-batch required upstream job fails/warns, Page07 recommendation is blocked.
