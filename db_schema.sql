-- Properties Table
SET TIME ZONE 'UTC';

CREATE TABLE properties (
    listing_id TEXT PRIMARY KEY,
    property_type TEXT,
    mrd_type TEXT,
    mls_status TEXT,
    original_list_price FLOAT,
    list_price FLOAT,
    close_price FLOAT,
    association_fee FLOAT,
    tax_annual_amount FLOAT,
    tax_year INT,
    days_on_market INT,
    mlg_can_view BOOLEAN,
    mlg_can_use TEXT[], -- Using array for string slices
    street_number TEXT,
    street_name TEXT,
    city TEXT,
    postal_code TEXT,
    county_or_parish TEXT,
    township TEXT,
    rooms_total INT,
    bedrooms_total INT,
    bathrooms_full INT,
    bathrooms_half INT,
    garage_spaces FLOAT,
    lot_size_acres FLOAT,
    lot_size_dimensions TEXT,
    living_area FLOAT,
    mrd_age TEXT,
    year_built INT,
    public_remarks TEXT,
    modification_timestamp timestamptz,
    elementary_school TEXT,
    middle_or_junior_school TEXT,
    high_school TEXT,
    elementary_school_district TEXT,
    middle_or_junior_school_district TEXT,
    high_school_district TEXT,
    listing_agreement TEXT,
    waterfront_yn BOOLEAN,
    model TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Rooms Table
CREATE TABLE rooms (
    room_id SERIAL PRIMARY KEY,
    listing_id TEXT REFERENCES properties(listing_id),
    mrd_flooring TEXT,
    room_level TEXT,
    room_dimensions TEXT,
    room_type TEXT,
    room_key TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- UnitTypes Table
CREATE TABLE unit_types (
    unit_type_id SERIAL PRIMARY KEY,
    listing_id TEXT REFERENCES properties(listing_id),
    unit_type_key TEXT,
    floor_number TEXT,
    unit_number TEXT,
    unit_bedrooms_total INT,
    unit_bathrooms_total INT,
    unit_total_rent INT,
    unit_security_deposit TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Medias Table
CREATE TABLE medias (
    media_id SERIAL PRIMARY KEY,
    listing_id TEXT REFERENCES properties(listing_id),
    media_key TEXT,
    media_url TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_listing_id ON properties(listing_id);

-- Function to update 'updated_at' column
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
   NEW.updated_at = NOW();
   RETURN NEW;
END;
$$ language 'plpgsql';

-- Triggers to update 'updated_at' column
CREATE TRIGGER update_properties_modtime
BEFORE UPDATE ON properties
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_rooms_modtime
BEFORE UPDATE ON rooms
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_unit_types_modtime
BEFORE UPDATE ON unit_types
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_medias_modtime
BEFORE UPDATE ON medias
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

ALTER TABLE rooms ADD CONSTRAINT unique_listing_room UNIQUE(listing_id, room_key);
ALTER TABLE unit_types ADD CONSTRAINT unique_listing_unit_type UNIQUE(listing_id, unit_type_key);
ALTER TABLE medias ADD CONSTRAINT unique_listing_media UNIQUE(listing_id, media_key);
