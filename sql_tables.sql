#
CREATE TABLE IF NOT EXISTS location(
    id INT SERIAL NOT NULL,
    city_name TEXT,
    state TEXT,
    latitude FLOAT,
    longitude FLOAT);
    
# 
CREATE TABLE IF NOT EXISTS temperature(
    datetime DATETIME,
    city_id INT,
    average_temp FLOAT,
    uncertainty_temp FLOAT
);

# 
CREATE TABLE IF NOT EXISTS time(
    date datetime,
    year INT,
    month INT,
    day INT
);

#
CREATE IF NOT EXISTS demographics(
    city TEXT,
    state TEXT,
    state_abb
    median_age FLOAT,
    male_pop INT,
    female_pop INT,
    total_pop INT,
    num_of_veterans INT,
    foreign_born INT,
    avg_household_size FLOAT,
    major_race TEXT,
    race_count INT
)

#
CREATE IF NOT EXISTS cost(
    world_rank INT,
    city TEXT,
    cost_of_living_index FLOAT,
    rent_index FLOAT,
    grocceries_index FLOAT,
    resterant_index FLOAT,
    local_purchases_index FLOAT
)

#
CREATE IF NOT EXITSTS diseases(
    year INT,
    state_abb TEXT,
    topic TEXT,
    date_value float,
    observation_type TEXT
)

#
CREATE IF NOT EXISTS fertility(
    year INT,
    num_births INT,
    fertility_rate FLOAT,
    crude_birth_rate FLOAT
)