WITH transaction_ventes AS (
  SELECT
    client_id AS client_id,
    prop_id AS prop_id,
    prod_price * prod_qty AS vente
  FROM
    `dataset.TRANSACTION`
  WHERE
    date BETWEEN DATE("2019-01-01") AND DATE("2019-12-31")
)
SELECT
  transaction_ventes.client_id AS client_id,
  SUM(IF(nomenclature.product_type = "MEUBLE", transaction_ventes.vente, 0)) AS ventes_meuble,
  SUM(IF(nomenclature.product_type = "DECO", transaction_ventes.vente, 0)) AS ventes_deco
FROM
  transaction_ventes
INNER JOIN
  `dataset.PRODUCT_NOMENCLATURE` AS nomenclature
ON
  transaction_ventes.prop_id = nomenclature.product_id
GROUP BY
  client_id