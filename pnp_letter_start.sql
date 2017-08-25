-- Create table that determines first letter frequency of Pride and Prejudice.
-- Obviously this could be done in Spark - this is an example workflow.

DROP TABLE IF EXISTS temp.ooziewrapper_example_pnp PURGE;
CREATE EXTERNAL TABLE temp.ooziewrapper_example_pnp
  (
    letter STRING,
    amount INT
  )
LOCATION 'hdfs://tmp/ooziewrapper_example/pnp_wordcount.txt';

CREATE TABLE temp.ooziewrapper_pnp_lettercount AS
SELECT
  SUBSTR(letter, 1) AS letter,
  SUM(amount) AS wc
FROM temp.ooziewrapper_example_pnp
GROUP BY SUBSTR(letter, 1);

DROP TABLE temp.ooziewrapper_example_pnp PURGE;
