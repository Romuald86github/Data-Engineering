-- tests/loan_analytics_tests.sql

SELECT COUNT(*) as num_records
FROM {{ ref('loan_analytics') }};
