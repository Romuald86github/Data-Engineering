WITH payment_analytics AS (
  SELECT
    CASE 
      WHEN date_repaid <= expected_repayment_date THEN 'Yes'
      ELSE 'No'
    END AS "PaidOnTime",
    CASE
      WHEN date_trunc('day', date_repaid) <= expected_repayment_date AND date_repaid > expected_repayment_date THEN '< 7'
      ELSE '> 7'
    END AS "LateTime",
    EXTRACT(MONTH FROM date_repaid) AS "PaidMonth",
    SUM(amount_to_be_repaid) AS "AmountPaid",
    CASE 
      WHEN date_repaid > expected_repayment_date THEN 'Yes'
      ELSE 'No'
    END AS "DefaultStatus"
  FROM
    `your_project_id.your_dataset_id.loan_table`
  WHERE
    date_repaid IS NOT NULL
  GROUP BY
    "PaidOnTime",
    "LateTime",
    "PaidMonth",
    "DefaultStatus"
)

INSERT INTO {{ ref('PaymentAnalytics') }}
SELECT * FROM payment_analytics;
