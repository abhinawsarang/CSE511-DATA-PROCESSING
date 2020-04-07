--1 
CREATE TABLE query1 AS
SELECT G.name AS name, count(*) AS moviecount
  FROM genres G JOIN hasagenre H
    ON G.genreid = H.genreid GROUP BY G.name;
--2
CREATE TABLE query2 AS
SELECT G.name AS name, avg(R.rating) AS rating
	FROM ratings R JOIN hasagenre H
	ON R.movieid = H.movieid
	JOIN genres G
	ON G.genreid = H.genreid GROUP BY G.name;
--3
CREATE TABLE tempquery3 AS
	SELECT count(*) AS countofratings, M.title AS title
	FROM ratings R JOIN movies M
	ON R.movieid = M.movieid
	GROUP BY M.title;

CREATE TABLE query3 AS
SELECT * FROM tempquery3 WHERE countofratings >= 10;
--4
CREATE TABLE query4 AS
SELECT movieid AS movieid, title AS title
	FROM movies WHERE movieid IN
	(
		SELECT movieid FROM hasagenre WHERE genreid IN
		(SELECT genreid FROM genres WHERE name = 'Comedy')
	);
--5
CREATE TABLE query5 AS
SELECT M.title AS title, avg(R.rating) AS average
	FROM ratings R JOIN movies M
	ON R.movieid = M.movieid
	GROUP BY M.title;
--6
CREATE TABLE query6 AS
SELECT avg(rating) AS average
	FROM ratings
	WHERE movieid IN
	(
		SELECT movieid FROM hasagenre WHERE genreid IN
		(SELECT genreid FROM genres WHERE name = 'Comedy')
	);
--7
CREATE TABLE query7 AS
SELECT avg(rating) AS average
	FROM ratings
	WHERE movieid IN
	(
		SELECT movieid FROM hasagenre WHERE genreid IN
		(SELECT genreid FROM genres WHERE name = 'Comedy')
	INTERSECT
		SELECT movieid FROM hasagenre WHERE genreid IN
		(SELECT genreid FROM genres WHERE name = 'Romance')
	);
--8
CREATE TABLE query8 AS
SELECT avg(rating) AS average
	FROM ratings
	WHERE movieid IN
	(
		SELECT movieid FROM hasagenre WHERE genreid IN
		(SELECT genreid FROM genres WHERE name = 'Romance')
	EXCEPT
		SELECT movieid FROM hasagenre WHERE genreid IN
		(SELECT genreid FROM genres WHERE name = 'Comedy')
	);
--9
CREATE TABLE query9 AS
SELECT movieid as movieid, rating as rating
	FROM ratings
	WHERE userid = :v1;

--10
CREATE VIEW moviesavgrating AS
SELECT M.movieid AS movieid, avg(R.rating) AS avgrating
	FROM ratings R JOIN movies M
	ON R.movieid = M.movieid
	GROUP BY M.movieid;

CREATE VIEW moviessim
AS SELECT m1.movieid AS movieid1, m2.movieid AS movieid2, 1 - (abs(m1.avgrating - m2.avgrating)/5) AS sim
FROM moviesavgrating m1, moviesavgrating m2;

CREATE VIEW ratedmovies AS SELECT DISTINCT movieid AS movieid, rating AS movierating FROM ratings WHERE userid = :v1;
CREATE VIEW notratedmovies AS SELECT DISTINCT movieid AS movieid FROM ratings EXCEPT SELECT DISTINCT movieid FROM ratings WHERE userid = :v1;

CREATE VIEW movieprediction
AS SELECT nrm.movieid AS notratedmovieid, sum(ms.sim * rm.movierating)/sum(ms.sim) AS predictionvalue
FROM ratedmovies rm CROSS JOIN notratedmovies nrm JOIN moviessim ms ON (nrm.movieid = ms.movieid1 AND rm.movieid = ms.movieid2) GROUP BY nrm.movieid;

CREATE TABLE  recommendation AS
SELECT title as title FROM movies WHERE movieid IN (SELECT notratedmovieid FROM movieprediction WHERE predictionvalue >3.9);