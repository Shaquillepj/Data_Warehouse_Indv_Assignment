CREATE SCHEMA Schema_wh;

CREATE TABLE Schema_wh.dim_contributing_factor ( 
	factor_key int64 NOT NULL  ,
	factor_name string  
 );

ALTER TABLE Schema_wh.dim_contributing_factor ADD PRIMARY KEY ( factor_key )  NOT ENFORCED;

CREATE TABLE Schema_wh.dim_date ( 
	date_key int64 NOT NULL  ,
	calendar_date date  ,
	year int64  ,
	month int64  ,
	day int64  ,
	weekday string  
 );

ALTER TABLE Schema_wh.dim_date ADD PRIMARY KEY ( date_key )  NOT ENFORCED;

CREATE TABLE Schema_wh.dim_location ( 
	location_key int64 NOT NULL  ,
	latitude float64  ,
	longitude float64  ,
	borough string  ,
	zip_code string  
 );

ALTER TABLE Schema_wh.dim_location ADD PRIMARY KEY ( location_key )  NOT ENFORCED;

CREATE TABLE Schema_wh.dim_street ( 
	street_key int64 NOT NULL  ,
	street_name string  
 );

ALTER TABLE Schema_wh.dim_street ADD PRIMARY KEY ( street_key )  NOT ENFORCED;

CREATE TABLE Schema_wh.dim_time ( 
	time_key int64 NOT NULL  ,
	clock_time time  ,
	hour int64  ,
	minute int64  
 );

ALTER TABLE Schema_wh.dim_time ADD PRIMARY KEY ( time_key )  NOT ENFORCED;

CREATE TABLE Schema_wh.dim_vehicle_type ( 
	vehicle_type_key int64 NOT NULL  ,
	vehicle_type string  
 );

ALTER TABLE Schema_wh.dim_vehicle_type ADD PRIMARY KEY ( vehicle_type_key )  NOT ENFORCED;

CREATE TABLE Schema_wh.fact_collision ( 
	collision_key int64 NOT NULL  ,
	collision_id string  ,
	date_key int64  ,
	time_key int64  ,
	location_key int64  ,
	persons_injured int64  ,
	persons_killed int64  ,
	pedestrians_injured int64  ,
	pedestrians_killed int64  ,
	cyclists_injured int64  ,
	cyclists_killed int64  ,
	motorists_injured int64  ,
	motorists_killed int64  ,
	ingestion_date date  
 );

ALTER TABLE Schema_wh.fact_collision ADD PRIMARY KEY ( collision_key )  NOT ENFORCED;

CREATE TABLE Schema_wh.br_collision_factor ( 
	row_id int64 NOT NULL  ,
	collision_key int64 NOT NULL  ,
	factor_key int64 NOT NULL  ,
	factor_index int64 NOT NULL  
 );

ALTER TABLE Schema_wh.br_collision_factor ADD PRIMARY KEY ( row_id )  NOT ENFORCED;

CREATE TABLE Schema_wh.br_collision_street ( 
	row_id int64 NOT NULL  ,
	collision_key int64 NOT NULL  ,
	street_key int64 NOT NULL  ,
	role string  
 );

ALTER TABLE Schema_wh.br_collision_street ADD PRIMARY KEY ( row_id )  NOT ENFORCED;

CREATE TABLE Schema_wh.br_collision_vehicle ( 
	row_id int64 NOT NULL  ,
	collision_key int64 NOT NULL  ,
	vehicle_type_key int64 NOT NULL  ,
	vehicle_index int64  
 );

ALTER TABLE Schema_wh.br_collision_vehicle ADD PRIMARY KEY ( row_id )  NOT ENFORCED;

ALTER TABLE Schema_wh.br_collision_factor ADD CONSTRAINT fk_br_collision_factor_fact_collision FOREIGN KEY ( collision_key ) REFERENCES Schema_wh.fact_collision( collision_key ) NOT ENFORCED;

ALTER TABLE Schema_wh.br_collision_factor ADD CONSTRAINT fk_br_collision_factor_dim_contributing_factor FOREIGN KEY ( factor_key ) REFERENCES Schema_wh.dim_contributing_factor( factor_key ) NOT ENFORCED;

ALTER TABLE Schema_wh.br_collision_street ADD CONSTRAINT fk_br_collision_street_dim_street FOREIGN KEY ( street_key ) REFERENCES Schema_wh.dim_street( street_key ) NOT ENFORCED;

ALTER TABLE Schema_wh.br_collision_street ADD CONSTRAINT fk_br_collision_street_fact_collision FOREIGN KEY ( collision_key ) REFERENCES Schema_wh.fact_collision( collision_key ) NOT ENFORCED;

ALTER TABLE Schema_wh.br_collision_vehicle ADD CONSTRAINT fk_br_collision_vehicle_dim_vehicle_type FOREIGN KEY ( vehicle_type_key ) REFERENCES Schema_wh.dim_vehicle_type( vehicle_type_key ) NOT ENFORCED;

ALTER TABLE Schema_wh.br_collision_vehicle ADD CONSTRAINT fk_br_collision_vehicle_fact_collision FOREIGN KEY ( collision_key ) REFERENCES Schema_wh.fact_collision( collision_key ) NOT ENFORCED;

ALTER TABLE Schema_wh.fact_collision ADD CONSTRAINT fk_fact_collision_dim_date FOREIGN KEY ( date_key ) REFERENCES Schema_wh.dim_date( date_key ) NOT ENFORCED;

ALTER TABLE Schema_wh.fact_collision ADD CONSTRAINT fk_fact_collision_dim_time FOREIGN KEY ( time_key ) REFERENCES Schema_wh.dim_time( time_key ) NOT ENFORCED;

ALTER TABLE Schema_wh.fact_collision ADD CONSTRAINT fk_fact_collision_dim_location FOREIGN KEY ( location_key ) REFERENCES Schema_wh.dim_location( location_key ) NOT ENFORCED;

