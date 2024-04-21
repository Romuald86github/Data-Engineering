-- analysis/payment_analytics_analysis.sql

SELECT
  *
FROM
  {{ ref('PaymentAnalytics') }};
