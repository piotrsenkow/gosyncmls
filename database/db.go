package database

import (
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	"github.com/piotrsenkow/gosyncmls/models"
	"github.com/piotrsenkow/gosyncmls/utils"
	"os"
	"time"
)

var Db *sql.DB

// InitializeDb initializes the database connection.
func InitializeDb() (*sql.DB, error) {
	//// Initialize the database connection
	//dbConnStr := viper.GetString("DB_CONN_STRING")
	dbConnStr := os.Getenv("DB_CONN_STRING")
	var err error
	Db, err = sql.Open("postgres", dbConnStr)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// insertOrUpdateProperty inserts or updates a property in the database.
func insertOrUpdateProperty(property models.Property) (error, string) {
	// Start a transaction
	tx, err := Db.Begin()
	if err != nil {
		utils.LogEvent("error", "Error: "+err.Error())
		return err, "line 33"
	}
	utils.LogEvent("info", fmt.Sprintf("Inserting/Updating Property: %+v", property))
	var realtyAnalyticaPropertyId int
	// Insert or update into the properties table
	err = tx.QueryRow(`
        INSERT INTO properties (
            listing_id, property_type, mrd_type, mls_status, 
            original_list_price, list_price, close_price, association_fee, 
            tax_annual_amount, tax_year, days_on_market, mlg_can_view, 
            mlg_can_use, street_number,street_dir_prefix, street_name, street_suffix, unit_number, city, postal_code, 
            county_or_parish, township, rooms_total, bedrooms_total, 
            bathrooms_full, bathrooms_half, garage_spaces, lot_size_acres, 
            lot_size_dimensions, living_area, mrd_age, year_built, 
            public_remarks, modification_timestamp, elementary_school, 
            middle_or_junior_school, high_school, elementary_school_district, 
            middle_or_junior_school_district, high_school_district, 
            listing_agreement, waterfront_yn, model,
            accessibility_features, heating, water_source, sewer, lot_features,
            roof, community_features, laundry_features, cooling, mls_area_major,
            mrd_actualstatus, mrd_actv_date, association_fee_includes, mrd_asq,
			mrd_assessor_sqft, mrd_bb, mrd_bldg_on_land, mrd_bmd, mrd_brbelow,
			mrd_can_owner_rent, mrd_currentlyleased, mrd_deed_garage_cost, mrd_din, 
			mrd_disability_access, mrd_ext, mrd_fireplace_location, mrd_full_baths_bldg, 
			mrd_garage_onsite, mrd_garage_ownership, mrd_garage_type, mrd_sp_incl_parking, 
			mrd_half_baths_bldg, mrd_idx, mrd_lsz, mrd_maf, gross_income,
			additional_parcels_yn, parcel_number, expiration_date, mrd_master_assoc_fee,
			mrd_main_sqft, mrd_unit_sqft, mrd_upper_sqft, mrd_lower_sqft, ownership,
			subdivision_name, mrd_mgt, mrd_min, mrd_min_lp, mrd_max_lp, mrd_min_rp, mrd_max_rp, 
			cumulative_days_on_market, lease_term, mrd_new_constr_yn, mrd_orp, mrd_aon, mrd_b78,
			mrd_bas, mrd_bd3, close_date, frontage_length, mrd_parking_onsite,
			mrd_pkn, mrd_poo, mrd_pry, mrd_rd, mrd_recordmoddate, mrd_rehab_year, 
			mrd_rental_property_type, mrd_rnp, mrd_rp, mrd_rti, mrd_sdp, mrd_short_sale,
			mrd_smi, mrd_sqft_comments, mrd_ten, mrd_tla, mrd_tmu, mrd_tnu, mrd_tpc,
			mrd_tpe, mrd_txc, mrd_ud, mrd_ufl, net_operating_income, new_construction_yn,
			off_market_date, operating_expense, original_entry_timestamp, other_equipment, 
			other_structures, parking_total, postal_code_plus4, previous_list_price, purchase_contract_date, 
			rent_includes, standard_status, state_or_province, status_change_timestamp,
			mrd_closed_buyer_brokerage_compensation, mrd_closed_buyer_brokerage_compensation_type,
            pets_allowed, interior_features, private_remarks, virtual_tour_url,
            total_actual_rent, trash_expense, water_sewer_expense, 
			zoning, list_agent_email, list_agent_first_name, list_agent_last_name, list_agent_full_name,
			list_agent_mls_id, list_agent_mobile_phone, list_agent_key, list_office_mls_id, list_office_name,
			list_office_phone, listing_contract_date
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
			$21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38,
			$39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51, $52, $53, $54, $55, $56,
			$57, $58, $59, $60, $61, $62, $63, $64, $65, $66, $67, $68, $69, $70, $71, $72, $73, $74,
			$75, $76, $77, $78, $79, $80, $81, $82, $83, $84, $85, $86, $87, $88, $89, $90, $91, $92,
			$93, $94, $95, $96, $97, $98, $99, $100, $101, $102, $103, $104, $105, $106, $107, $108,
			$109, $110, $111, $112, $113, $114, $115, $116, $117, $118, $119, $120, $121, $122, $123, $124,
			$125, $126, $127, $128, $129, $130, $131, $132, $133, $134, $135, $136, $137, $138, $139, $140,
			$141, $142, $143, $144, $145, $146, $147, $148, $149, $150, $151, $152, $153, $154, $155, $156, $157, $158,
            $159, $160, $161, $162, $163, $164, $165
        )
        ON CONFLICT (listing_id) DO UPDATE SET 
            property_type = EXCLUDED.property_type,
            mrd_type = EXCLUDED.mrd_type,
            mls_status = EXCLUDED.mls_status,
            original_list_price = EXCLUDED.original_list_price,
            list_price = EXCLUDED.list_price,
            close_price = EXCLUDED.close_price,
            association_fee = EXCLUDED.association_fee,
            tax_annual_amount = EXCLUDED.tax_annual_amount,
            tax_year = EXCLUDED.tax_year,
            days_on_market = EXCLUDED.days_on_market,
            mlg_can_view = EXCLUDED.mlg_can_view,
            mlg_can_use = EXCLUDED.mlg_can_use,
            street_number = EXCLUDED.street_number,
            street_dir_prefix = EXCLUDED.street_dir_prefix,
            street_name = EXCLUDED.street_name,
            street_suffix = EXCLUDED.street_suffix,
            unit_number = EXCLUDED.unit_number,
            city = EXCLUDED.city,
            postal_code = EXCLUDED.postal_code,
            county_or_parish = EXCLUDED.county_or_parish,
            township = EXCLUDED.township,
            rooms_total = EXCLUDED.rooms_total,
            bedrooms_total = EXCLUDED.bedrooms_total,
            bathrooms_full = EXCLUDED.bathrooms_full,
            bathrooms_half = EXCLUDED.bathrooms_half,
            garage_spaces = EXCLUDED.garage_spaces,
            lot_size_acres = EXCLUDED.lot_size_acres,
            lot_size_dimensions = EXCLUDED.lot_size_dimensions,
            living_area = EXCLUDED.living_area,
            mrd_age = EXCLUDED.mrd_age,
            year_built = EXCLUDED.year_built,
            public_remarks = EXCLUDED.public_remarks,
            modification_timestamp = EXCLUDED.modification_timestamp,
            elementary_school = EXCLUDED.elementary_school,
            middle_or_junior_school = EXCLUDED.middle_or_junior_school,
            high_school = EXCLUDED.high_school,
            elementary_school_district = EXCLUDED.elementary_school_district,
            middle_or_junior_school_district = EXCLUDED.middle_or_junior_school_district,
            high_school_district = EXCLUDED.high_school_district,
            listing_agreement = EXCLUDED.listing_agreement,
            waterfront_yn = EXCLUDED.waterfront_yn,
            model = EXCLUDED.model,
            accessibility_features = EXCLUDED.accessibility_features,
			heating = EXCLUDED.heating,
			water_source = EXCLUDED.water_source,
			sewer = EXCLUDED.sewer,
			lot_features = EXCLUDED.lot_features,
			roof = EXCLUDED.roof,
			community_features = EXCLUDED.community_features,
			laundry_features = EXCLUDED.laundry_features,
			cooling = EXCLUDED.cooling,
			mls_area_major = EXCLUDED.mls_area_major,
			mrd_actualstatus = EXCLUDED.mrd_actualstatus,
			mrd_actv_date = EXCLUDED.mrd_actv_date,
			association_fee_includes = EXCLUDED.association_fee_includes,
			mrd_asq = EXCLUDED.mrd_asq,
			mrd_assessor_sqft = EXCLUDED.mrd_assessor_sqft,
			mrd_bb = EXCLUDED.mrd_bb,
			mrd_bldg_on_land = EXCLUDED.mrd_bldg_on_land,
			mrd_bmd = EXCLUDED.mrd_bmd,
			mrd_brbelow = EXCLUDED.mrd_brbelow,
			mrd_can_owner_rent = EXCLUDED.mrd_can_owner_rent,
			mrd_currentlyleased = EXCLUDED.mrd_currentlyleased,
			mrd_deed_garage_cost = EXCLUDED.mrd_deed_garage_cost,
			mrd_din = EXCLUDED.mrd_din,
			mrd_disability_access = EXCLUDED.mrd_disability_access,
			mrd_ext = EXCLUDED.mrd_ext,
			mrd_fireplace_location = EXCLUDED.mrd_fireplace_location,
			mrd_full_baths_bldg = EXCLUDED.mrd_full_baths_bldg,
			mrd_garage_onsite = EXCLUDED.mrd_garage_onsite,
			mrd_garage_ownership = EXCLUDED.mrd_garage_ownership,
			mrd_garage_type = EXCLUDED.mrd_garage_type,
			mrd_sp_incl_parking = EXCLUDED.mrd_sp_incl_parking,
			mrd_half_baths_bldg = EXCLUDED.mrd_half_baths_bldg,
			mrd_idx = EXCLUDED.mrd_idx,
			mrd_lsz = EXCLUDED.mrd_lsz,
			mrd_maf = EXCLUDED.mrd_maf,
			gross_income = EXCLUDED.gross_income,
			additional_parcels_yn = EXCLUDED.additional_parcels_yn,
			parcel_number = EXCLUDED.parcel_number,
			expiration_date = EXCLUDED.expiration_date,
			mrd_master_assoc_fee = EXCLUDED.mrd_master_assoc_fee,
			mrd_main_sqft = EXCLUDED.mrd_main_sqft,
			mrd_unit_sqft = EXCLUDED.mrd_unit_sqft,
			mrd_upper_sqft = EXCLUDED.mrd_upper_sqft,
			mrd_lower_sqft = EXCLUDED.mrd_lower_sqft,
			ownership = EXCLUDED.ownership,
			subdivision_name = EXCLUDED.subdivision_name,
			mrd_mgt = EXCLUDED.mrd_mgt,
			mrd_min = EXCLUDED.mrd_min,
			mrd_min_lp = EXCLUDED.mrd_min_lp,
			mrd_max_lp = EXCLUDED.mrd_max_lp,
			mrd_min_rp = EXCLUDED.mrd_min_rp,
			mrd_max_rp = EXCLUDED.mrd_max_rp,
			cumulative_days_on_market = EXCLUDED.cumulative_days_on_market,
			lease_term = EXCLUDED.lease_term,
			mrd_new_constr_yn = EXCLUDED.mrd_new_constr_yn,
			mrd_orp = EXCLUDED.mrd_orp,
			mrd_aon = EXCLUDED.mrd_aon,
			mrd_b78 = EXCLUDED.mrd_b78,
			mrd_bas = EXCLUDED.mrd_bas,
			mrd_bd3 = EXCLUDED.mrd_bd3,
			close_date = EXCLUDED.close_date,
			frontage_length = EXCLUDED.frontage_length,
			mrd_parking_onsite = EXCLUDED.mrd_parking_onsite,
			mrd_pkn = EXCLUDED.mrd_pkn,
			mrd_poo = EXCLUDED.mrd_poo,
			mrd_pry = EXCLUDED.mrd_pry,
			mrd_rd = EXCLUDED.mrd_rd,
			mrd_recordmoddate = EXCLUDED.mrd_recordmoddate,
			mrd_rehab_year = EXCLUDED.mrd_rehab_year,
			mrd_rental_property_type = EXCLUDED.mrd_rental_property_type,
			mrd_rnp = EXCLUDED.mrd_rnp,
			mrd_rp = EXCLUDED.mrd_rp,
			mrd_rti = EXCLUDED.mrd_rti,
			mrd_sdp = EXCLUDED.mrd_sdp,
			mrd_short_sale = EXCLUDED.mrd_short_sale,
			mrd_smi = EXCLUDED.mrd_smi,
			mrd_sqft_comments = EXCLUDED.mrd_sqft_comments,
			mrd_ten = EXCLUDED.mrd_ten,
			mrd_tla = EXCLUDED.mrd_tla,
			mrd_tmu = EXCLUDED.mrd_tmu,
			mrd_tnu = EXCLUDED.mrd_tnu,
			mrd_tpc = EXCLUDED.mrd_tpc,
			mrd_tpe = EXCLUDED.mrd_tpe,
			mrd_txc = EXCLUDED.mrd_txc,
			mrd_ud = EXCLUDED.mrd_ud,
			mrd_ufl = EXCLUDED.mrd_ufl,
			net_operating_income = EXCLUDED.net_operating_income,
			new_construction_yn = EXCLUDED.new_construction_yn,
			off_market_date = EXCLUDED.off_market_date,
			operating_expense = EXCLUDED.operating_expense,
			original_entry_timestamp = EXCLUDED.original_entry_timestamp,
			other_equipment = EXCLUDED.other_equipment,
			other_structures = EXCLUDED.other_structures,
			parking_total = EXCLUDED.parking_total,
			postal_code_plus4 = EXCLUDED.postal_code_plus4,
			previous_list_price = EXCLUDED.previous_list_price,
			purchase_contract_date = EXCLUDED.purchase_contract_date,
			rent_includes = EXCLUDED.rent_includes,
			standard_status = EXCLUDED.standard_status,
			state_or_province = EXCLUDED.state_or_province,
			status_change_timestamp = EXCLUDED.status_change_timestamp,
			mrd_closed_buyer_brokerage_compensation = EXCLUDED.mrd_closed_buyer_brokerage_compensation,
			mrd_closed_buyer_brokerage_compensation_type = EXCLUDED.mrd_closed_buyer_brokerage_compensation_type,
			pets_allowed = EXCLUDED.pets_allowed,
			interior_features = EXCLUDED.interior_features,
			private_remarks = EXCLUDED.private_remarks,
			virtual_tour_url = EXCLUDED.virtual_tour_url,
			total_actual_rent = EXCLUDED.total_actual_rent,
			trash_expense = EXCLUDED.trash_expense,
			water_sewer_expense = EXCLUDED.water_sewer_expense,
			zoning = EXCLUDED.zoning,
			list_agent_email = EXCLUDED.list_agent_email,
			list_agent_first_name = EXCLUDED.list_agent_first_name,
			list_agent_last_name = EXCLUDED.list_agent_last_name,
			list_agent_full_name = EXCLUDED.list_agent_full_name,
			list_agent_mls_id = EXCLUDED.list_agent_mls_id,
			list_agent_mobile_phone = EXCLUDED.list_agent_mobile_phone,
			list_agent_key = EXCLUDED.list_agent_key,
			list_office_mls_id = EXCLUDED.list_office_mls_id,
			list_office_name = EXCLUDED.list_office_name,
			list_office_phone = EXCLUDED.list_office_phone,
			listing_contract_date = EXCLUDED.listing_contract_date
        RETURNING ra_pid
    `,
		property.ListingId, property.PropertyType, property.MRDType, property.MLSStatus,
		property.OriginalListPrice, property.ListPrice, property.ClosePrice, property.AssociationFee,
		property.TaxAnnualAmount, property.TaxYear, property.DaysOnMarket, property.MlgCanView,
		pq.Array(property.MlgCanUse), property.StreetNumber, property.StreetDirPrefix, property.StreetName, property.StreetSuffix,
		property.UnitNumber, property.City, property.PostalCode, property.CountyOrParish, property.Township, property.RoomsTotal,
		property.BedroomsTotal, property.BathroomsFull, property.BathroomsHalf,
		property.GarageSpaces, property.LotSizeAcres, property.LotSizeDimensions, property.LivingArea,
		property.MrdAge, property.YearBuilt, property.PublicRemarks, property.ModificationTimestamp,
		property.ElementarySchool, property.MiddleOrJuniorSchool, property.HighSchool,
		property.ElementarySchoolDistrict, property.MiddleOrJuniorSchoolDistrict,
		property.HighSchoolDistrict, property.ListingAgreement, property.WaterfrontYN, property.Model,
		pq.Array(property.AccessibilityFeatures), pq.Array(property.Heating), pq.Array(property.WaterSource), pq.Array(property.Sewer),
		pq.Array(property.LotFeatures), pq.Array(property.Roof), pq.Array(property.CommunityFeatures), pq.Array(property.LaundryFeatures), pq.Array(property.Cooling),
		property.MLSAreaMajor, property.MRD_ACTUALSTATUS, property.MRD_ACTV_DATE, pq.Array(property.AssociationFeeIncludes), property.MRD_ASQ, property.MRD_ASSESSOR_SQFT,
		property.MRD_BB, property.MRD_BLDG_ON_LAND, property.MRD_BMD, property.MRD_BRBELOW, property.MRD_CAN_OWNER_RENT,
		property.MRD_CURRENTLYLEASED, property.MRD_DEED_GARAGE_COST, property.MRD_DIN, property.MRD_DISABILITY_ACCESS,
		property.MRD_EXT, property.MRD_FIREPLACE_LOCATION, property.MRD_FULL_BATHS_BLDG, property.MRD_GARAGE_ONSITE,
		property.MRD_GARAGE_OWNERSHIP, property.MRD_GARAGE_TYPE, property.MRD_SP_INCL_PARKING, property.MRD_HALF_BATHS_BLDG,
		property.MRD_IDX, property.MRD_LSZ, property.MRD_MAF, property.GrossIncome, property.AdditionalParcelsYN, property.ParcelNumber,
		property.ExpirationDate, property.MRD_MASTER_ASSOC_FEE, property.MRD_MAIN_SQFT, property.MRD_UNIT_SQFT, property.MRD_UPPER_SQFT,
		property.MRD_LOWER_SQFT, property.Ownership, property.SubdivisionName, property.MRD_MGT, property.MRD_MIN, property.MRD_MIN_LP,
		property.MRD_MAX_LP, property.MRD_MIN_RP, property.MRD_MAX_RP, property.CumulativeDaysOnMarket, property.LeaseTerm, property.MRD_NEW_CONSTR_YN,
		property.MRD_ORP, property.MRD_AON, property.MRD_B78, property.MRD_BAS, property.MRD_BD3, property.CloseDate, property.FrontageLength,
		property.MRD_PARKING_ONSITE, property.MRD_PKN, property.MRD_POO, property.MRD_PRY, property.MRD_RD, property.MRD_RECORDMODDATE,
		property.MRD_REHAB_YEAR, property.MRD_RENTAL_PROPERTY_TYPE, property.MRD_RNP, property.MRD_RP, property.MRD_RTI, property.MRD_SDP,
		property.MRD_SHORT_SALE, property.MRD_SMI, property.MRD_SQFT_COMMENTS, property.MRD_TEN, property.MRD_TLA, property.MRD_TMU, property.MRD_TNU,
		property.MRD_TPC, property.MRD_TPE, property.MRD_TXC, property.MRD_UD, property.MRD_UFL, property.NetOperatingIncome, property.NewConstructionYN,
		property.OffMarketDate, property.OperatingExpense, property.OriginalEntryTimestamp, pq.Array(property.OtherEquipment), pq.Array(property.OtherStructures),
		property.ParkingTotal, property.PostalCodePlus4, property.PreviousListPrice, property.PurchaseContractDate, pq.Array(property.RentIncludes),
		property.StandardStatus, property.StateOrProvince, property.StatusChangeTimestamp, property.MRD_ClosedBuyerBrokerageCompensation, property.MRD_ClosedBuyerBrokerageCompensationType,
		pq.Array(property.PetsAllowed), pq.Array(property.InteriorFeatures), property.PrivateRemarks, property.VirtualTourUrl,
		property.TotalActualRent, property.TrashExpense, property.WaterSewerExpense, property.Zoning, property.ListAgentEmail,
		property.ListAgentFirstName, property.ListAgentLastName, property.ListAgentFullName, property.ListAgentMlsId, property.ListAgentMobilePhone,
		property.ListAgentKey, property.ListOfficeMlsId, property.ListOfficeName, property.ListOfficePhone, property.ListingContractDate,
	).Scan(&realtyAnalyticaPropertyId)
	if err != nil {
		utils.LogEvent("error", "Error on line 677: "+err.Error())
		return err, "line 677"
	}
	utils.LogEvent("info", fmt.Sprintf("Created ra_pID: %d", realtyAnalyticaPropertyId))

	// Insert into the rooms table
	for _, room := range property.Rooms {
		result, err := tx.Exec(`
        INSERT INTO rooms (property_id, mrd_flooring, room_level, room_dimensions, room_type, room_key)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (property_id, room_key) DO UPDATE SET 
            mrd_flooring = EXCLUDED.mrd_flooring,
            room_level = EXCLUDED.room_level,
            room_dimensions = EXCLUDED.room_dimensions,
            room_type = EXCLUDED.room_type
    `, realtyAnalyticaPropertyId, room.MrdFlooring, room.RoomLevel, room.RoomDimensions, room.RoomType, room.RoomKey)
		if err != nil {
			utils.LogEvent("error", "Error on line 695")
			return err, "line 695"
		}
		rowsAffected, _ := result.RowsAffected()
		utils.LogEvent("info", fmt.Sprintf("Rows Affected in rooms: %d", rowsAffected))
	}

	// Insert into the unit_types table
	for _, unitType := range property.UnitTypes {
		result, err := tx.Exec(`
        INSERT INTO unit_types (property_id, unit_type_key, floor_number, unit_number, unit_bedrooms_total, unit_bathrooms_total, unit_total_rent, unit_security_deposit)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (property_id, unit_type_key) DO UPDATE SET 
            floor_number = EXCLUDED.floor_number,
            unit_number = EXCLUDED.unit_number,
            unit_bedrooms_total = EXCLUDED.unit_bedrooms_total,
            unit_bathrooms_total = EXCLUDED.unit_bathrooms_total,
            unit_total_rent = EXCLUDED.unit_total_rent,
            unit_security_deposit = EXCLUDED.unit_security_deposit
    `, realtyAnalyticaPropertyId, unitType.UnitTypeKey, unitType.FloorNumber, unitType.UnitNumber, unitType.UnitBedroomsTotal, unitType.UnitBathroomsTotal, unitType.UnitTotalRent, unitType.UnitSecurityDeposit)
		if err != nil {
			utils.LogEvent("error", "Error on line 716")
			return err, "line 321"
		}
		rowsAffected, _ := result.RowsAffected()
		utils.LogEvent("info", fmt.Sprintf("Rows Affected in unit types: %d", rowsAffected))
	}

	// Insert into the medias table
	for _, media := range property.Media {
		result, err := tx.Exec(`
        INSERT INTO medias (property_id, media_key, media_url)
        VALUES ($1, $2, $3)
        ON CONFLICT (property_id, media_key) DO UPDATE SET 
            media_url = EXCLUDED.media_url
    `, realtyAnalyticaPropertyId, media.MediaKey, media.MediaURL)
		if err != nil {
			utils.LogEvent("error", "Error on line 732")
			return err, "line 337"
		}
		rowsAffected, _ := result.RowsAffected()
		utils.LogEvent("info", fmt.Sprintf("Rows Affected in medias: %d", rowsAffected))
	}

	// Commit the transaction
	utils.LogEvent("info", "Committing transaction to database")
	err = tx.Commit()
	if err != nil {
		utils.LogEvent("error", "Failed to commit transaction: "+err.Error())
		return err, "line 348"
	} else {
		utils.LogEvent("info", "Transaction committed successfully")
	}
	return nil, ""
}

// deleteProperty deletes a property from the database.
func deleteProperty(property models.Property) error {
	// Start a transaction
	tx, err := Db.Begin()
	if err != nil {
		return err
	}

	// Get the property_id for the given listing_id
	var propertyId int
	err = tx.QueryRow("SELECT ra_pid FROM properties WHERE listing_id = $1", property.ListingId).Scan(&propertyId)
	if err != nil {
		return err // Handle error, property_id not found
	}

	// Delete from the child tables first to respect foreign key constraints
	_, err = tx.Exec(`DELETE FROM medias WHERE property_id = $1`, propertyId)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`DELETE FROM unit_types WHERE property_id = $1`, propertyId)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`DELETE FROM rooms WHERE property_id = $1`, propertyId)
	if err != nil {
		return err
	}

	// Delete from the properties table
	_, err = tx.Exec(`DELETE FROM properties WHERE property_id = $1`, propertyId)
	if err != nil {
		return err
	}

	// Commit the transaction
	return tx.Commit()
}

// ProcessData processes the data from the API response.
func ProcessData(data []models.Property) {
	for _, property := range data {
		if property.MlgCanView {
			// Insert or update in the database
			err, line := insertOrUpdateProperty(property)
			if err != nil {
				utils.LogEvent("trace", "Trace on : "+line+" :"+err.Error())
			}
		} else {
			// Delete from the database
			err := deleteProperty(property)
			if err != nil {
				utils.LogEvent("trace", "Trace: "+err.Error())
			}
		}
	}
}

// constructBaseURL constructs the base URL for the API request.
func constructBaseURL(lastTimestamp time.Time) string {
	timestampStr := lastTimestamp.Format("2006-01-02T15:04:05.999Z")
	return "https://api.mlsgrid.com/v2/Property?$filter=OriginatingSystemName%20eq%20'mred'%20and%20ModificationTimestamp%20gt%20" + timestampStr
}

// ConstructInitialImportURL constructs the initial import URL from where it last left off.
func ConstructInitialImportURL(lastTimestamp time.Time) string {
	return constructBaseURL(lastTimestamp) + "%20and%20MlgCanView%20eq%20true&$expand=Rooms,UnitTypes,Media&$top=1000"
}

// ConstructUpdateURL constructs the update URL.
func ConstructUpdateURL(lastTimestamp time.Time) string {
	return constructBaseURL(lastTimestamp) + "&$expand=Rooms,UnitTypes,Media&$top=1000"
}

// GetLastModificationTimestamp gets the last modification timestamp from the database.
func GetLastModificationTimestamp() (time.Time, error) {
	query := "SELECT MAX(modification_timestamp) at time zone 'utc' FROM properties"

	var timestamp time.Time
	err := Db.QueryRow(query).Scan(&timestamp)
	if err != nil {
		return time.Time{}, err
	}
	return timestamp, nil
}

// CheckIfPropertiesTableHasData Check if the properties table has any preexisting data
func CheckIfPropertiesTableHasData() (bool, error) {
	timestamp, err := GetLastModificationTimestamp()
	if err != nil {
		return false, err
	}

	// If timestamp is zero, then the table is empty
	return !timestamp.IsZero(), nil
}
