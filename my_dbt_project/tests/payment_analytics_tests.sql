-- tests/payment_analytics_tests.sql

SELECT COUNT(*) as num_records
FROM {{ ref('PaymentAnalytics') }};
