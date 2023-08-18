CREATE DATABASE dw_spotify;

GRANT ALL PRIVILEGES ON DATABASE dw_spotify TO myuser;

CREATE TABLE tblSpotifyRank (
    Rank INT,
    Title TEXT,
    Artists TEXT,
    Date TEXT,
    Danceability DOUBLE PRECISION,
    Energy DOUBLE PRECISION,
    Loudness DOUBLE PRECISION,
    Speechiness DOUBLE PRECISION,
    Acousticness DOUBLE PRECISION,
    Instrumentalness DOUBLE PRECISION,
    Valence DOUBLE PRECISION,
    Num_Artists TEXT,
    Artist TEXT,
    Num_Nationality TEXT,
    Nationality TEXT,
    Continent TEXT,
    Total_Points INT,
    Ind_Points INT,
    Id TEXT,
    Song_URL TEXT
);