-- Compare first letter frequency between Sense and Sensibility
-- and Pride and Prejudice.

CREATE TABLE temp.sas_pnp_first_letter AS
SELECT
  pnp.letter as letter,
  pnp.wc as pnp_wc,
  sas.wc as sas_wc
FROM
  temp.ooziewrapper_example_pnp pnp
JOIN
  temp.ooziewrapper_example_sas sas
ON
  pnp.letter = sas.letter;

DROP TABLE temp.ooziewrapper_example_pnp PURGE;
DROP TABLE temp.ooziewrapper_example_sas PURGE;
