CREATE TABLE IF NOT EXISTS RATINGS (userid int, movieid int, rating float,timestmp date )
    COMMENT 'Movie ratings'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    STORED AS TEXTFILE;

    
LOAD DATA LOCAL INPATH '/mnt/c/Users/miles/Downloads/movielens/ratings.csv' OVERWRITE INTO TABLE ratings;


------------------------------

--hive queries

-----display ratings data

select * from ratings;

---- to find each category of ratings

select rating,count(rating) as ratings_count
from ratings
group by rating;