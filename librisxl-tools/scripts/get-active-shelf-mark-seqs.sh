#!/bin/sh

QUERY="
SET NOCOUNT ON
SELECT PrefixBeforeYear,
       CASE
           WHEN Brochure = 'true' THEN CONCAT(PrefixAfterYear, ' br')
           ELSE PrefixAfterYear
           END AS PrefixAfterYear,
       LastSerialNumber, Year
FROM T_CodeType, (
    SELECT CodeType_FKID, MAX(SerialNumber) as LastSerialNumber, Year
    FROM T_SerialNumber, (
        SELECT CodeType_FKID as CodeId, MAX(Year) as maxYear
        FROM T_SerialNumber
        WHERE ActiveStatus = 2
        GROUP BY CodeType_FKID
    ) as Years
    WHERE ActiveStatus = 2 AND Year = maxYear AND CodeType_FKID = CodeId
    GROUP BY CodeType_FKID, Year
) as Serial
WHERE CodeType_ID = CodeType_FKID;"

sqlcmd -S mssqlag3.kb.local -d signum -U Signum-read -Q "$QUERY" \
  -o ../../whelktool/scripts/2021/03/create-and-link-shelf-mark-seqs/active-in-old-db.csv -h-1 -s"," -W

truncate -s -1 ../../whelktool/scripts/2021/03/create-and-link-shelf-mark-seqs/active-in-old-db.csv