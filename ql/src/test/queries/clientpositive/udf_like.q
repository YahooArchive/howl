DESCRIBE FUNCTION like;
DESCRIBE FUNCTION EXTENDED like;

EXPLAIN
SELECT '_%_' LIKE '%\_\%\_%', '__' LIKE '%\_\%\_%', '%%_%_' LIKE '%\_\%\_%', '%_%_%' LIKE '%\%\_\%',
  '_%_' LIKE '\%\_%', '%__' LIKE '__\%%', '_%' LIKE '\_\%\_\%%', '_%' LIKE '\_\%_%',
  '%_' LIKE '\%\_', 'ab' LIKE '\%\_', 'ab' LIKE '_a%', 'ab' LIKE 'a'
FROM src WHERE src.key = 86;

SELECT '_%_' LIKE '%\_\%\_%', '__' LIKE '%\_\%\_%', '%%_%_' LIKE '%\_\%\_%', '%_%_%' LIKE '%\%\_\%',
  '_%_' LIKE '\%\_%', '%__' LIKE '__\%%', '_%' LIKE '\_\%\_\%%', '_%' LIKE '\_\%_%',
  '%_' LIKE '\%\_', 'ab' LIKE '\%\_', 'ab' LIKE '_a%', 'ab' LIKE 'a'
FROM src WHERE src.key = 86;
