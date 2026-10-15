-- Idempotent daily rollup: re-running or backfilling a date overwrites that date's row.
MERGE `{{ project }}.{{ dataset }}.daily_order_kpis` AS target
USING (
  WITH events AS (
    SELECT * EXCEPT (rn)
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingested_at) AS rn
      FROM `{{ project }}.{{ dataset }}.order_events`
      WHERE DATE(occurred_at) = '{{ ds }}'
    )
    WHERE rn = 1
  )
  SELECT
    DATE('{{ ds }}') AS kpi_date,
    COUNTIF(event_type = 'OrderCreated') AS orders_created,
    COUNT(DISTINCT IF(event_type = 'OrderCreated', customer_id, NULL)) AS active_customers,
    SUM(IF(event_type = 'OrderCreated', amount, 0)) AS gross_order_value,
    COUNTIF(event_type = 'PaymentCompleted') AS payments_completed,
    COUNTIF(event_type = 'PaymentFailed') AS payments_failed,
    SUM(IF(event_type = 'PaymentCompleted', amount, 0)) AS revenue,
    SAFE_DIVIDE(
      COUNTIF(event_type = 'PaymentCompleted'),
      COUNTIF(event_type IN ('PaymentCompleted', 'PaymentFailed'))
    ) AS payment_success_rate,
    SAFE_DIVIDE(
      SUM(IF(event_type = 'OrderCreated', amount, 0)),
      COUNTIF(event_type = 'OrderCreated')
    ) AS average_order_value
  FROM events
) AS source
ON target.kpi_date = source.kpi_date
WHEN MATCHED THEN UPDATE SET
  orders_created = source.orders_created,
  active_customers = source.active_customers,
  gross_order_value = source.gross_order_value,
  payments_completed = source.payments_completed,
  payments_failed = source.payments_failed,
  revenue = source.revenue,
  payment_success_rate = source.payment_success_rate,
  average_order_value = source.average_order_value,
  updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
  kpi_date, orders_created, active_customers, gross_order_value, payments_completed,
  payments_failed, revenue, payment_success_rate, average_order_value, updated_at
) VALUES (
  source.kpi_date, source.orders_created, source.active_customers, source.gross_order_value,
  source.payments_completed, source.payments_failed, source.revenue, source.payment_success_rate,
  source.average_order_value, CURRENT_TIMESTAMP()
)
