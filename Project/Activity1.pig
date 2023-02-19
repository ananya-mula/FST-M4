-- Load Data from HDFS
inputDialogues = LOAD 'hdfs:///user/ananya/inputs' USING PigStorage('\t') AS (name:chararray, line:chararray);

-- Filter out the first 2 lines
ranked = RANK inputDialogues;
OnlyDialogues = FILTER ranked BY (rank_inputDialogues >2);

-- Group by name
groupByName = GROUP OnlyDialogues BY name;

-- Count the number of lines by each character
names = FOREACH groupByName GENERATE $0 as name, COUNT($1) as no_of_lines;
namesOrdered = ORDER names BY no_of_lines DESC;

-- Remove the output folder
rmf hdfs:///user/ananya/outputs

-- Store result in HDFC
STORE namesOrdered INTO 'hdfs:///user/ananya/outputs' USING PigStorage('\t');
