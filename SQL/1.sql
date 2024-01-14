SELECT
  date AS date,
  SUM(prod_price * prod_qty) AS ventes
FROM
  `dataset.TRANSACTION`
WHERE
  date BETWEEN DATE("2019-01-01") AND DATE("2019-12-31")
GROUP BY
  date
ORDER BY
  date ASC