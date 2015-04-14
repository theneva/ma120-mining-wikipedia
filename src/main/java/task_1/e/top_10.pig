records = LOAD 'output-from-1d.txt' USING PigStorage('\t')
    AS (word:chararray, count:int);
records_sorted = ORDER records BY count DESC;
top_10 = LIMIT records_sorted 10;
DUMP top_10;
