CREATE TABLE property_rumah (
    link VARCHAR PRIMARY KEY,
    ads_type VARCHAR,
    property_type VARCHAR,
    name VARCHAR,
    location VARCHAR,
    lot_size INT,
    building_size INT,
    n_bedroom INT,
    n_bathroom INT,
    n_carport INT,
    additional_features VARCHAR,
    price_rp BIGINT
);

CREATE TABLE stg_property_rumah (
    link VARCHAR PRIMARY KEY,
    ads_type VARCHAR,
    property_type VARCHAR,
    name VARCHAR,
    location VARCHAR,
    lot_size INT,
    building_size INT,
    n_bedroom INT,
    n_bathroom INT,
    n_carport INT,
    additional_features VARCHAR,
    price_rp BIGINT
);