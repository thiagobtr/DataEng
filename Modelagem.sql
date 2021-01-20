CREATE TABLE IF NOT EXISTS ADS
(id int PRIMARY key,
bedrooms_min numeric NULL,
bathrooms_min numeric NULL,
built_area_min numeric NULL,
city_name VARCHAR(100)NULL,
idno VARCHAR(20) ,
lat numeric NULL,
lon numeric NULL,
neighborhood VARCHAR NULL,
parking_space_min numeric NULL,
property_type VARCHAR(50)NULL,
sale_price numeric NULL,
state VARCHAR(20)NULL,
street VARCHAR NULL,
street_number varchar(10) null,
id_building int references BUILDINGS (id)
)

CREATE TABLE IF NOT EXISTS BUILDINGS (
id int PRIMARY key,
address VARCHAR,
address_number varchar(80),
neighborhood VARCHAR ,
city VARCHAR(60),
state VARCHAR(60),
cep varchar(10),
latitude NUMERIC,
longitude NUMERIC
)

CREATE TABLE IF NOT EXISTS AD_BUILDINGS(
id int GENERATED ALWAYS AS identity PRIMARY KEY,
id_ad int REFERENCES ADS(id),
id_build int REFERENCES BUILDINGS(id)
)