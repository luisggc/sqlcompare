CREATE OR REPLACE TABLE stats_previous AS
SELECT * FROM read_csv_auto('examples/stats_compare/previous.csv');

CREATE OR REPLACE TABLE stats_current AS
SELECT * FROM read_csv_auto('examples/stats_compare/current.csv');
