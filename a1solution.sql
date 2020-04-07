CREATE TABLE users (
	userid INT NOT NULL,
	name VARCHAR(255) NOT NULL,
	PRIMARY KEY (userid) );
	
CREATE TABLE movies (
	movieid INT NOT NULL,
	title VARCHAR(255) NOT NULL,
	PRIMARY KEY (movieid) );
	

CREATE TABLE taginfo (
	tagid INT NOT NULL,
	content VARCHAR(1000) NOT NULL,
	PRIMARY KEY (tagid) );

CREATE TABLE genres (
	genreid INT NOT NULL,
	name VARCHAR(255) NOT NULL,
	PRIMARY KEY (genreid) );


CREATE TABLE hasagenre (
	movieid INT NOT NULL,
	genreid INT NOT NULL,
	PRIMARY KEY (genreid, movieid),
	FOREIGN KEY (movieid) REFERENCES movies (movieid) ON DELETE CASCADE,
	FOREIGN KEY (genreid) REFERENCES genres (genreid) ON DELETE CASCADE
	);
			
CREATE TABLE ratings (
	userid INT NOT NULL,
	movieid INT NOT NULL,
	rating REAL NOT NULL CHECK (rating >= 0 and rating <= 5),
	timestamp BIGINT NOT NULL,
	PRIMARY KEY (userid, movieid),
	FOREIGN KEY (movieid) REFERENCES movies (movieid) ON DELETE CASCADE,
	FOREIGN KEY (userid) REFERENCES users (userid) ON DELETE CASCADE
	);
	
CREATE TABLE tags (
	userid INT NOT NULL,
	movieid INT NOT NULL,
	tagid INT NOT NULL,
	timestamp BIGINT NOT NULL,
	PRIMARY KEY (userid, movieid, tagid),
	FOREIGN KEY (movieid) REFERENCES movies (movieid) ON DELETE CASCADE,
	FOREIGN KEY (userid) REFERENCES users (userid) ON DELETE CASCADE,
	FOREIGN KEY (tagid) REFERENCES taginfo (tagid) ON DELETE CASCADE
	);

