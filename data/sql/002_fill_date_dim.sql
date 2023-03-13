DROP VIEW IF EXISTS date_span;
CREATE TEMP VIEW IF NOT EXISTS date_span AS 
WITH RECURSIVE dates(date) AS (
  VALUES('2007-01-01')
  UNION ALL
  SELECT date(date, '+1 day')
  FROM dates
  WHERE date < date(date(), '-1 year')
) SELECT date FROM dates;

INSERT OR IGNORE INTO date_dim (year, month, day, day_of_week) 
SELECT strftime('%Y', date) AS year, strftime('%m', date) AS month, strftime('%d', date) AS day, strftime('%w', date) AS day_of_week FROM date_span;