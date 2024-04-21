WITH monthly_payments AS (
  SELECT
    EXTRACT(MONTH FROM date_repaid) AS "Month",
    COUNT(*) AS "TotalLoansPaid",
    SUM(amount_to_be_repaid) AS "TotalAmountPaid"
  FROM
    `your_project_id.your_dataset_id.loan_table`
  WHERE
    date_repaid IS NOT NULL
  GROUP BY
    "Month"
)

INSERT INTO {{ ref('loan_analytics') }}
SELECT
  "Month",
  "TotalLoansPaid",
  "TotalAmountPaid"
FROM
  monthly_payments
ORDER BY
  "Month";
