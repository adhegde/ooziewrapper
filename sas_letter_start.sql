-- Create table that determines first letter frequency of Sense and Sensibility.
-- Obviously this could be done in Spark - this is an example workflow.

DROP TABLE IF EXISTS temp.ooziewrapper_example_sas PURGE;
CREATE EXTERNAL TABLE temp.ooziewrapper_example_sas
  (
    letter STRING,
    amount INT
  )
LOCATION 'hdfs://tmp/ooziewrapper_example/sas_wordcount.txt';

CREATE TABLE temp.ooziewrapper_sas_lettercount AS
SELECT
  SUBSTR(letter, 1) AS letter,
  SUM(amount) AS wc
FROM temp.ooziewrapper_example_sas
GROUP BY SUBSTR(letter, 1);

DROP TABLE temp.ooziewrapper_example_sas PURGE;
