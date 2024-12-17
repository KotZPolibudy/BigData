SELECT logged_in_user();

CREATE DATABASE IF NOT EXISTS accidents;
USE accidents;

CREATE EXTERNAL TABLE IF NOT EXISTS zip_codes (
    zip_code STRING,
    borough STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${input_dir4}'
TBLPROPERTIES ("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS incidents (
    street STRING,
    zip_code STRING,
    victim_type STRING,
    injury_type STRING,
    num_injured INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = "^(.*?),(.*?),(.*?),(.*?)\\t(\\d+)$"
)
STORED AS TEXTFILE
LOCATION '${input_dir3}';

CREATE TABLE IF NOT EXISTS accidents_top3 (
    street STRING,
    person_type STRING,
    killed INT,
    injured INT
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${output_dir6}';

WITH total_injuries AS (
    SELECT
        a.street,
        a.victim_type,
        SUM(CASE WHEN a.injury_type = 'Killed' THEN a.num_injured ELSE 0 END) AS killed,
        SUM(CASE WHEN a.injury_type = 'Injured' THEN a.num_injured ELSE 0 END) AS injured,
        SUM(a.num_injured) AS total_count
    FROM
        incidents a
    JOIN
        zip_codes z
    ON
        a.zip_code = z.zip_code
    WHERE
        z.borough = 'MANHATTAN'
    GROUP BY
        a.street, a.victim_type
),

ranked_streets AS (
    SELECT
        street,
        victim_type,
        killed,
        injured,
        ROW_NUMBER() OVER (PARTITION BY victim_type ORDER BY total_count DESC) AS rank
    FROM
        total_injuries
)

INSERT OVERWRITE TABLE accidents_top3
SELECT
    street,
    victim_type AS person_type,
    killed,
    injured
FROM
    ranked_streets
WHERE
    rank <= 3
ORDER BY
    victim_type, rank;
