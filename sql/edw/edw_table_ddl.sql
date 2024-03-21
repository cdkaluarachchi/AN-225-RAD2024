CREATE SEQUENCE WEATHER_DATA_SEQ;
CREATE SEQUENCE LOCATION_DATA_SEQ;


CREATE TABLE MAIN (
    TEMP NUMERIC(3, 2),
    FEELS_LIKE NUMERIC(3, 2),
    TEMP_MIN NUMERIC(3, 2),
    TEMP_MAX NUMERIC(3, 2),
    PRESSURE NUMERIC(10, 2),
    HUMIDITY NUMERIC(4, 2),
    SEA_LEVEL NUMERIC(5, 2),
    GRND_LEVEL NUMERIC(5, 2)
    SYS_INSERT_DATETIME TIMESTAMP
);


CREATE TABLE DIM_LOCATION(
    LOCATION_ID INT NOT NULL DEFAULT NEXTVAL('LOCATION_DATA_SEQ'),
    COUNTRY VARCHAR(100),
    TIMEZONE INT,
    ID INT,
    LOC_NAME VARCHAR(100),
    LON NUMERIC,
    LAT NUMERIC,
    API_ID NUMERIC,
    SYS_INSERT_DATETIME TIMESTAMP
);



CREATE TABLE DIM_WEATHER (
	WEATHER_ID INT NOT NULL DEFAULT NEXTVAL('WEATHER_DATA_SEQ'),
	ID INT,
	MAIN VARCHAR(20),
	DESCR VARCHAR(50),
	ICON VARCHAR(10),
	API_ID NUMERIC,
    SYS_INSERT_DATETIME TIMESTAMP
);