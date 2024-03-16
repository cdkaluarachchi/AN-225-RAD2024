
CREATE SEQUENCE api_data_seq;

CREATE TABLE LZ_API_DATA (
    id integer NOT NULL DEFAULT nextval('api_data_seq'),
    api_data text
);