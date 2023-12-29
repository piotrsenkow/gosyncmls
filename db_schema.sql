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
    street_dir_prefix TEXT,
    street_name TEXT,
    street_suffix TEXT,
    unit_number TEXT,
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


    accessibility_features TEXT[],
    heating TEXT[],
    water_source TEXT[],
    sewer TEXT[],
    lot_features TEXT[],
    roof TEXT[],
    community_features TEXT[],
    laundry_features TEXT[],
    cooling TEXT[],

    mls_area_major TEXT,
    mrd_actualstatus TEXT,
    mrd_actv_date timestamptz,
    association_fee_includes TEXT[],
    mrd_asq TEXT,
    mrd_assessor_sqft TEXT,
    mrd_bb TEXT,
    mrd_bldg_on_land TEXT,
    mrd_bmd TEXT,
    mrd_brbelow TEXT,
    mrd_can_owner_rent TEXT,
    mrd_currentlyleased TEXT,
    mrd_deed_garage_cost TEXT,
    mrd_din TEXT,
    mrd_disability_access TEXT,
    mrd_ext TEXT,
    mrd_fireplace_location TEXT,
    mrd_full_baths_bldg TEXT,
    mrd_garage_onsite TEXT,
    mrd_garage_ownership TEXT,
    mrd_garage_type TEXT,
    mrd_sp_incl_parking TEXT,
    mrd_half_baths_bldg TEXT,
    mrd_idx TEXT,
    mrd_lsz TEXT,
    mrd_maf TEXT,

    gross_income INT,
    additional_parcels_yn BOOLEAN,
    parcel_number TEXT,
    expiration_date TEXT,
    mrd_master_assoc_fee TEXT,
    mrd_main_sqft TEXT,
    mrd_unit_sqft TEXT,
    mrd_upper_sqft TEXT,
    mrd_lower_sqft TEXT,
    ownership TEXT,
    subdivision_name TEXT,
    mrd_mgt TEXT,
    mrd_min TEXT,
    mrd_min_lp TEXT,
    mrd_max_lp TEXT,
    mrd_min_rp TEXT,
    mrd_max_rp TEXT,

    cumulative_days_on_market INT,
    lease_term TEXT,
    mrd_new_constr_yn TEXT,
    mrd_orp TEXT,
    mrd_aon TEXT,
    mrd_b78 TEXT,
    mrd_bas TEXT,
    mrd_bd3 TEXT,
    close_date TEXT,
    frontage_length TEXT,

    mrd_parking_onsite TEXT,
    mrd_pkn TEXT,
    mrd_poo TEXT,
    mrd_pry TEXT,
    mrd_rd TEXT,
    mrd_recordmoddate TEXT,
    mrd_rehab_year TEXT,
    mrd_rental_property_type TEXT,
    mrd_rnp TEXT,
    mrd_rp TEXT,
    mrd_rti TEXT,
    mrd_sdp TEXT,
    mrd_short_sale TEXT,
    mrd_smi TEXT,
    mrd_sqft_comments TEXT,
    mrd_ten TEXT,
    mrd_tla TEXT,
    mrd_tmu TEXT,
    mrd_tnu TEXT,
    mrd_tpc TEXT,
    mrd_tpe TEXT,
    mrd_txc TEXT,
    mrd_ud TEXT,
    mrd_ufl TEXT,

    net_operating_income INT,
    new_construction_yn BOOLEAN,
    off_market_date TEXT,
    operating_expense INT,
    original_entry_timestamp timestamptz,
    other_equipment TEXT[],
    other_structures TEXT[],
    parking_total INT,
    postal_code_plus4 TEXT,
    previous_list_price INT,
    purchase_contract_date TEXT,
    rent_includes TEXT[],
    standard_status TEXT,
    state_or_province TEXT,
    status_change_timestamp timestamptz,

    mrd_closed_buyer_brokerage_compensation TEXT,
    mrd_closed_buyer_brokerage_compensation_type TEXT,
    pets_allowed TEXT[],
    interior_features TEXT[],
    private_remarks TEXT,
    virtual_tour_url TEXT,
    total_actual_rent INT,
    trash_expense INT,
    water_sewer_expense INT,
    zoning TEXT,
    list_agent_email TEXT,
    list_agent_first_name TEXT,
    list_agent_last_name TEXT,
    list_agent_full_name TEXT,
    list_agent_mls_id TEXT,
    list_agent_mobile_phone TEXT,
    list_agent_key TEXT,
    list_office_mls_id TEXT,
    list_office_name TEXT,
    list_office_phone TEXT,
    listing_contract_date TEXT,

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

CREATE INDEX idx_city ON properties(city);
CREATE INDEX idx_county_or_parish ON properties(county_or_parish);
CREATE INDEX idx_mls_area_major ON properties(mls_area_major);
CREATE INDEX idx_mls_status ON properties(mls_status);
CREATE INDEX idx_mrd_type ON properties(mrd_type);
CREATE INDEX idx_township ON properties(township);
CREATE INDEX idx_postal_code ON properties(postal_code);
CREATE INDEX idx_property_type ON properties(property_type);

CREATE EXTENSION postgis;
