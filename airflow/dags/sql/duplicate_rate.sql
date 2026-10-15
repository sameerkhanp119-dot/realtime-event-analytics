-- Share of raw rows that are re-deliveries of an event already seen that day.
-- The rollup removes them; this check alerts when the stream's duplicate rate drifts above 5%.
SELECT SAFE_DIVIDE(COUNT(*) - COUNT(DISTINCT event_id), COUNT(*)) AS duplicate_rate
FROM `{{ project }}.{{ dataset }}.order_events`
WHERE DATE(occurred_at) = '{{ ds }}'
