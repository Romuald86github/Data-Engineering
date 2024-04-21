-- analysis/loan_analytics_analysis.sql

SELECT
  *
FROM
  {{ ref('loan_analytics') }};
