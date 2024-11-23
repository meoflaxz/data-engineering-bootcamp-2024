CREATE TYPE films AS(
                  film TEXT,
                  votes INTEGER,
                  rating REAL,
                  filmid TEXT);

CREATE TYPE quality_class AS ENUM ('star','good','average','bad');


CREATE TABLE actors (
    actor TEXT,
    actorid TEXT,
    filmid TEXT,
    films films[],
    quality_class quality_class,
    is_active BOOLEAN,
    current_year INTEGER,
    PRIMARY KEY(actorid, filmid, current_year)
); 