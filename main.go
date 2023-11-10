package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/rs/zerolog"
	"golang.org/x/time/rate"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type IntValue int

type Property struct {
	ListingId                    string    `json:"ListingId"`
	PropertyType                 string    `json:"PropertyType"`
	MRDType                      string    `json:"MRD_TYP"`
	MLSStatus                    string    `json:"MlsStatus"`
	OriginalListPrice            float64   `json:"OriginalListPrice"`
	ListPrice                    float64   `json:"ListPrice"`
	ClosePrice                   float64   `json:"ClosePrice"`
	AssociationFee               float64   `json:"AssociationFee"`
	TaxAnnualAmount              float64   `json:"TaxAnnualAmount"`
	TaxYear                      IntValue  `json:"TaxYear"`
	DaysOnMarket                 IntValue  `json:"DaysOnMarket"`
	MlgCanView                   bool      `json:"MlgCanView"`
	MlgCanUse                    []string  `json:"MlgCanUse"`
	StreetNumber                 string    `json:"StreetNumber"`
	StreetName                   string    `json:"StreetName"`
	City                         string    `json:"City"`
	PostalCode                   string    `json:"PostalCode"`
	CountyOrParish               string    `json:"CountyOrParish"`
	Township                     string    `json:"Township"`
	RoomsTotal                   int       `json:"RoomsTotal"`
	BedroomsTotal                IntValue  `json:"BedroomsTotal"`
	BathroomsFull                IntValue  `json:"BathroomsFull"`
	BathroomsHalf                IntValue  `json:"BathroomsHalf"`
	GarageSpaces                 float64   `json:"GarageSpaces"`
	LotSizeAcres                 float64   `json:"LotSizeAcres"`
	LotSizeDimensions            string    `json:"LotSizeDimensions"`
	LivingArea                   float64   `json:"LivingArea"`
	MrdAge                       string    `json:"MRD_AGE"`
	YearBuilt                    IntValue  `json:"YearBuilt"`
	PublicRemarks                string    `json:"PublicRemarks"`
	ModificationTimestamp        time.Time `json:"ModificationTimestamp"`
	ElementarySchool             string    `json:"ElementarySchool"`
	MiddleOrJuniorSchool         string    `json:"MiddleOrJuniorSchool"`
	HighSchool                   string    `json:"HighSchool"`
	ElementarySchoolDistrict     string    `json:"ElementarySchoolDistrict"`
	MiddleOrJuniorSchoolDistrict string    `json:"MiddleOrJuniorSchoolDistrict"`
	HighSchoolDistrict           string    `json:"HighSchoolDistrict"`
	ListingAgreement             string    `json:"ListingAgreement"`
	WaterfrontYN                 bool      `json:"WaterfrontYN"`
	Model                        string    `json:"Model"`

	AccessibilityFeatures []string `json:"AccessibilityFeatures"`
	Heating               []string `json:"Heating"`
	WaterSource           []string `json:"WaterSource"`
	Sewer                 []string `json:"Sewer"`
	LotFeatures           []string `json:"LotFeatures"`
	Roof                  []string `json:"Roof"`
	CommunityFeatures     []string `json:"CommunityFeatures"`
	LaundryFeatures       []string `json:"LaundryFeatures"`
	Cooling               []string `json:"Cooling"`

	MLSAreaMajor           string    `json:"MLSAreaMajor"`
	MRD_ACTUALSTATUS       string    `json:"MRD_ACTUALSTATUS"`
	MRD_ACTV_DATE          time.Time `json:"MRD_ACTV_DATE"`
	AssociationFeeIncludes []string  `json:"AssociationFeeIncludes"`
	MRD_ASQ                string    `json:"MRD_ASQ"`
	MRD_ASSESSOR_SQFT      string    `json:"MRD_ASSESSOR_SQFT"`
	MRD_BB                 string    `json:"MRD_BB"`
	MRD_BLDG_ON_LAND       string    `json:"MRD_BLDG_ON_LAND"`
	MRD_BMD                string    `json:"MRD_BMD"`
	MRD_BRBELOW            string    `json:"MRD_BRBELOW"`
	MRD_CAN_OWNER_RENT     string    `json:"MRD_CAN_OWNER_RENT"`
	MRD_CURRENTLYLEASED    string    `json:"MRD_CURRENTLYLEASED"`
	MRD_DEED_GARAGE_COST   string    `json:"MRD_DEED_GARAGE_COST"`
	MRD_DIN                string    `json:"MRD_DIN"`
	MRD_DISABILITY_ACCESS  string    `json:"MRD_DISABILITY_ACCESS"`
	MRD_EXT                string    `json:"MRD_EXT"`
	MRD_FIREPLACE_LOCATION string    `json:"MRD_FIREPLACE_LOCATION"`
	MRD_FULL_BATHS_BLDG    string    `json:"MRD_FULL_BATHS_BLDG"`
	MRD_GARAGE_ONSITE      string    `json:"MRD_GARAGE_ONSITE"`
	MRD_GARAGE_OWNERSHIP   string    `json:"MRD_GARAGE_OWNERSHIP"`
	MRD_GARAGE_TYPE        string    `json:"MRD_GARAGE_TYPE"`
	MRD_SP_INCL_PARKING    string    `json:"MRD_SP_INCL_PARKING"`
	MRD_HALF_BATHS_BLDG    string    `json:"MRD_HALF_BATHS_BLDG"`
	MRD_IDX                string    `json:"MRD_IDX"`
	MRD_LSZ                string    `json:"MRD_LSZ"`
	MRD_MAF                string    `json:"MRD_MAF"`

	GrossIncome          int       `json:"GrossIncome"`
	AdditionalParcelsYN  bool      `json:"AdditionalParcelsYN"`
	ParcelNumber         string    `json:"ParcelNumber"`
	ExpirationDate       time.Time `json:"ExpirationDate"`
	MRD_MASTER_ASSOC_FEE string    `json:"MRD_MASTER_ASSOC_FEE"`
	MRD_MAIN_SQFT        string    `json:"MRD_MAIN_SQFT"`
	MRD_UNIT_SQFT        string    `json:"MRD_UNIT_SQFT"`
	MRD_UPPER_SQFT       string    `json:"MRD_UPPER_SQFT"`
	MRD_LOWER_SQFT       string    `json:"MRD_LOWER_SQFT"`
	Ownership            string    `json:"Ownership"`
	SubdivisionName      string    `json:"SubdivisionName"`
	MRD_MGT              string    `json:"MRD_MGT"`
	MRD_MIN              string    `json:"MRD_MIN"`
	MRD_MIN_LP           string    `json:"MRD_MIN_LP"`
	MRD_MAX_LP           string    `json:"MRD_MAX_LP"`
	MRD_MIN_RP           string    `json:"MRD_MIN_RP"`
	MRD_MAX_RP           string    `json:"MRD_MAX_RP"`

	CumulativeDaysOnMarket int       `json:"CumulativeDaysOnMarket"`
	LeaseTerm              string    `json:"LeaseTerm"`
	MRD_NEW_CONSTR_YN      string    `json:"MRD_NEW_CONSTR_YN"`
	MRD_ORP                string    `json:"MRD_ORP"`
	MRD_AON                string    `json:"MRD_AON"`
	MRD_B78                string    `json:"MRD_B78"`
	MRD_BAS                string    `json:"MRD_BAS"`
	MRD_BD3                string    `json:"MRD_BD3"`
	CloseDate              time.Time `json:"CloseDate"`
	FrontageLength         string    `json:"FrontageLength"`

	MRD_PARKING_ONSITE       string `json:"MRD_PARKING_ONSITE"`
	MRD_PKN                  string `json:"MRD_PKN"`
	MRD_POO                  string `json:"MRD_POO"`
	MRD_PRY                  string `json:"MRD_PRY"`
	MRD_RD                   string `json:"MRD_RD"`
	MRD_RECORDMODDATE        string `json:"MRD_RECORDMODDATE"`
	MRD_REHAB_YEAR           string `json:"MRD_REHAB_YEAR"`
	MRD_RENTAL_PROPERTY_TYPE string `json:"MRD_RENTAL_PROPERTY_TYPE"`
	MRD_RNP                  string `json:"MRD_RNP"`
	MRD_RP                   string `json:"MRD_RP"`
	MRD_RTI                  string `json:"MRD_RTI"`
	MRD_SDP                  string `json:"MRD_SDP"`
	MRD_SHORT_SALE           string `json:"MRD_SHORT_SALE"`
	MRD_SMI                  string `json:"MRD_SMI"`
	MRD_SQFT_COMMENTS        string `json:"MRD_SQFT_COMMENTS"`
	MRD_TEN                  string `json:"MRD_TEN"`
	MRD_TLA                  string `json:"MRD_TLA"`
	MRD_TMU                  string `json:"MRD_TMU"`
	MRD_TNU                  string `json:"MRD_TNU"`
	MRD_TPC                  string `json:"MRD_TPC"`
	MRD_TPE                  string `json:"MRD_TPE"`
	MRD_TXC                  string `json:"MRD_TXC"`
	MRD_UD                   string `json:"MRD_UD"`
	MRD_UFL                  string `json:"MRD_UFL"`

	NetOperatingIncome     string    `json:"NetOperatingIncome"`
	NewConstructionYN      bool      `json:"NewConstructionYN"`
	OffMarketDate          time.Time `json:"OffMarketDate"`
	OperatingExpense       int       `json:"OperatingExpense"`
	OriginalEntryTimestamp time.Time `json:"OriginalEntryTimestamp"`
	OtherEquipment         []string  `json:"OtherEquipment"`
	OtherStructures        []string  `json:"OtherStructures"`
	ParkingTotal           int       `json:"ParkingTotal"`
	PostalCodePlus4        string    `json:"PostalCodePlus4"`
	PreviousListPrice      int       `json:"PreviousListPrice"`
	PurchaseContractDate   time.Time `json:"PurchaseContractDate"`
	RentIncludes           []string  `json:"RentIncludes"`
	StandardStatus         string    `json:"StandardStatus"`
	StateOrProvince        string    `json:"StateOrProvince"`
	StatusChangeTimestamp  time.Time `json:"StatusChangeTimestamp"`
	StreetDirPrefix        string    `json:"StreetDirPrefix"`
	StreetSuffix           string    `json:"StreetSuffix"`
	TotalActualRent        int       `json:"TotalActualRent"`
	TrashExpense           int       `json:"TrashExpense"`
	WaterSewerExpense      int       `json:"WaterSewerExpense"`
	Zoning                 string    `json:"Zoning"`
	ListAgentEmail         string    `json:"ListAgentEmail"`
	ListAgentFirstName     string    `json:"ListAgentFirstName"`
	ListAgentLastName      string    `json:"ListAgentLastName"`
	ListAgentFullName      string    `json:"ListAgentFullName"`
	ListAgentMlsId         string    `json:"ListAgentMlsId"`
	ListAgentMobilePhone   string    `json:"ListAgentMobilePhone"`
	ListAgentKey           string    `json:"ListAgentKey"`
	ListOfficeMlsId        string    `json:"ListOfficeMlsId"`
	ListOfficeName         string    `json:"ListOfficeName"`
	ListOfficePhone        string    `json:"ListOfficePhone"`
	ListingContractDate    time.Time `json:"ListingContractDate"`

	UnitTypes []UnitType `json:"UnitTypes"`
	Rooms     []Room     `json:"Rooms"`
	Media     []Media    `json:"Media"`
}

type Room struct {
	MrdFlooring    string `json:"MRD_Flooring"`
	RoomLevel      string `json:"RoomLevel"`
	RoomDimensions string `json:"RoomDimensions"`
	RoomType       string `json:"RoomType"`
	RoomKey        string `json:"RoomKey"`
}

type UnitType struct {
	UnitTypeKey         string `json:"UnitTypeKey"`
	FloorNumber         string `json:"MRD_FloorNumber"`
	UnitNumber          string `json:"UnitTypeType"`
	UnitBedroomsTotal   int    `json:"UnitTypeBedsTotal"`
	UnitBathroomsTotal  int    `json:"UnitTypeBathsTotal"`
	UnitTotalRent       int    `json:"UnitTypeActualRent"`
	UnitSecurityDeposit string `json:"MRD_SecurityDeposit"`
}

type Media struct {
	MediaKey string `json:"MediaKey"`
	MediaURL string `json:"MediaURL"`
}

type ApiResponse struct {
	Data     []Property `json:"value"`
	NextLink string     `json:"@odata.nextLink"`
}

type countingReader struct {
	r io.Reader
	n *int64
}

const (
	initialUrl         = "https://api.mlsgrid.com/v2/Property?$filter=OriginatingSystemName%20eq%20%27mred%27%20and%20MlgCanView%20eq%20true&$expand=Rooms%2CUnitTypes%2CMedia&$top=1000"
	MaxRequestsPerHour = 7200
	MaxRequestsPerDay  = 40000
	MaxDownloadPerHour = 4 * 1024 * 1024 * 1024 // 4GB in bytes
)

var (
	db     *sql.DB
	logger zerolog.Logger

	requestsThisHour int
	requestsToday    int
	httpClient       *http.Client

	// Defining limiters as package-level variables
	perSecondLimiter *rate.Limiter
	perHourLimiter   *rate.Limiter
	perDayLimiter    *rate.Limiter
	hourTicker       *time.Ticker
	dayTicker        *time.Ticker
	dataDownloaded   int64 = 0

	processDataSem = make(chan struct{}, 5)
)

func (iv *IntValue) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		*iv = IntValue(int(value))
	case int:
		*iv = IntValue(value)
	default:
		return fmt.Errorf("invalid format for integer value")
	}
	return nil
}

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	*cr.n += int64(n)
	return n, err
}

// Implement the withRetry function
func withRetry(attempts int, sleep time.Duration, fn func() error) error {
	for i := 0; ; i++ {
		err := fn()
		if err == nil {
			return nil // success
		}

		if i >= (attempts - 1) {
			return err // return the last error
		}

		logEvent("warn", fmt.Sprintf("Attempt %d failed; retrying in %v", i+1, sleep))
		time.Sleep(sleep)
		sleep *= 2
	}
}

func logEvent(eventType string, message string) {
	switch eventType {
	case "debug":
		logger.Debug().Msg(message)
	case "info":
		logger.Info().Msg(message)
	case "warn":
		logger.Warn().Msg(message)
	case "error":
		logger.Error().Msg(message)
	case "panic":
		logger.Panic().Msg(message)
	case "fatal":
		logger.Fatal().Msg(message)
	case "trace":
		logger.Trace().Msg(message)
	default:
		logger.Info().Msg(message)
	}
}

func canMakeRequest() bool {
	if !isWithinRateLimit(perDayLimiter, "day") {
		return false
	}
	if !isWithinRateLimit(perHourLimiter, "hour") {
		return false
	}
	if !isWithinRateLimit(perSecondLimiter, "second") {
		return false
	}
	if dataDownloaded >= MaxDownloadPerHour {
		logEvent("warn", "4 GB hourly download limit reached!")
	}
	return requestsThisHour <= MaxRequestsPerHour && dataDownloaded <= MaxDownloadPerHour &&
		requestsToday <= MaxRequestsPerDay
}

func isWithinRateLimit(limiter *rate.Limiter, period string) bool {
	reserve := limiter.Reserve()
	if !reserve.OK() {
		delay := reserve.Delay()
		logEvent("warn", fmt.Sprintf("Rate limited per %s, waiting for %v", period, delay))
		time.Sleep(delay)
		return false
	}
	return true
}

func handleApiErrors(fn func() error) {
	// What to do if API request failed
	operation := func() error {
		err := fn()
		if err != nil {
			// Log the error
			logEvent("info", "Backoff operation entered.")
			logEvent("error", "API Request Failed! "+err.Error())
		}
		return err
	}

	// Exponential backoff
	bo := backoff.NewExponentialBackOff()
	err := backoff.Retry(operation, bo)
	if err != nil {
		logEvent("error", "Failed after many retries using backoff")
	}
}

func startTickers() {
	// Anonymous function running on a GoThread that handles resetting of hourly / daily requests using time tickers
	// Also deleted
	go func() {
		for {
			select {
			case <-hourTicker.C:
				requestsThisHour = 0
				dataDownloaded = 0
				logEvent("info", "Hourly counter reset")
			case <-dayTicker.C:
				requestsToday = 0
				logEvent("info", "Daily counter reset")
			}
		}
	}()
}

func setupSignalHandlers() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-signals
		logEvent("info", "Received signal: "+sig.String())

		// Close database and API connections
		err := db.Close()
		if err != nil {
			logEvent("trace", "Trace: "+err.Error())
		}
		os.Exit(0)
	}()
}

func initialize() {
	// Initialize logger
	zerolog.TimeFieldFormat = "2006-01-02 15:04:05.000000"
	logger = zerolog.New(os.Stdout).With().Timestamp().Logger()

	// Initialize limiters
	perSecondLimiter = rate.NewLimiter(1.95, 2)
	perHourLimiter = rate.NewLimiter(rate.Limit(MaxRequestsPerHour)/3600, MaxRequestsPerHour)
	perDayLimiter = rate.NewLimiter(rate.Limit(MaxRequestsPerDay)/86400, MaxRequestsPerDay)

	// Reset counters every hour and day using tickers
	hourTicker = time.NewTicker(1 * time.Hour)
	dayTicker = time.NewTicker(24 * time.Hour)

	// Load configurations from environment variables
	dbConnStr := os.Getenv("DB_CONN_STRING")

	//// Initialize the database connection
	var err error
	db, err = sql.Open("postgres", dbConnStr)
	if err != nil {
		logEvent("fatal", "Failed to connect to the database")
	}

	// Initialize the HTTP client
	httpClient = &http.Client{}
}

func insertOrUpdateProperty(property Property) (error, string) {
	// Start a transaction
	tx, err := db.Begin()
	if err != nil {
		logEvent("error", "Error on line 250")
		return err, "line 250"
	}
	defer tx.Rollback()
	logEvent("info", fmt.Sprintf("Inserting/Updating Property: %+v", property))

	// Insert or update into the properties table
	result, err := tx.Exec(`
        INSERT INTO properties (
            listing_id, property_type, mrd_type, mls_status, 
            original_list_price, list_price, close_price, association_fee, 
            tax_annual_amount, tax_year, days_on_market, mlg_can_view, 
            mlg_can_use, street_number, street_name, city, postal_code, 
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
			mrd_tpe, mrd_txc, mrd_typ, mrd_ud, mrd_ufl, net_operating_income, new_construction_yn,
			off_market_date, operating_expense, original_entry_timestamp, other_equipment, 
			other_structures, parking_total, postal_code_plus4, previous_list_price, purchase_contract_date, 
			rent_includes, standard_status, state_or_province, status_change_timestamp,
			street_dir_prefix, street_suffix, total_actual_rent, trash_expense, water_sewer_expense, 
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
                $109, $110, $111, $112, $113, $114, $115, $116, $117, $118, $119, $120, $121, $122, $123, $124
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
            street_name = EXCLUDED.street_name,
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
			mrd_typ = EXCLUDED.mrd_typ,
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
			street_dir_prefix = EXCLUDED.street_dir_prefix,
			street_suffix = EXCLUDED.street_suffix,
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
    `,
		property.ListingId, property.PropertyType, property.MRDType, property.MLSStatus,
		property.OriginalListPrice, property.ListPrice, property.ClosePrice, property.AssociationFee,
		property.TaxAnnualAmount, property.TaxYear, property.DaysOnMarket, property.MlgCanView,
		pq.Array(property.MlgCanUse), property.StreetNumber, property.StreetName, property.City,
		property.PostalCode, property.CountyOrParish, property.Township, property.RoomsTotal,
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
		property.StandardStatus, property.StateOrProvince, property.StatusChangeTimestamp, property.StreetDirPrefix, property.StreetSuffix,
		property.TotalActualRent, property.TrashExpense, property.WaterSewerExpense, property.Zoning, property.ListAgentEmail,
		property.ListAgentFirstName, property.ListAgentLastName, property.ListAgentFullName, property.ListAgentMlsId, property.ListAgentMobilePhone,
		property.ListAgentKey, property.ListOfficeMlsId, property.ListOfficeName, property.ListOfficePhone, property.ListingContractDate,
	)
	if err != nil {
		logEvent("error", "Error on line 677")
		return err, "line 677"
	}
	rowsAffected, _ := result.RowsAffected()
	logEvent("info", fmt.Sprintf("Rows Affected in properties: %d", rowsAffected))

	// Insert into the rooms table
	for _, room := range property.Rooms {
		result, err = tx.Exec(`
        INSERT INTO rooms (listing_id, mrd_flooring, room_level, room_dimensions, room_type, room_key)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (listing_id, room_key) DO UPDATE SET 
            mrd_flooring = EXCLUDED.mrd_flooring,
            room_level = EXCLUDED.room_level,
            room_dimensions = EXCLUDED.room_dimensions,
            room_type = EXCLUDED.room_type
    `, property.ListingId, room.MrdFlooring, room.RoomLevel, room.RoomDimensions, room.RoomType, room.RoomKey)
		if err != nil {
			logEvent("error", "Error on line 695")
			return err, "line 695"
		}
		rowsAffected, _ := result.RowsAffected()
		logEvent("info", fmt.Sprintf("Rows Affected in rooms: %d", rowsAffected))
	}

	// Insert into the unit_types table
	for _, unitType := range property.UnitTypes {
		result, err = tx.Exec(`
        INSERT INTO unit_types (listing_id, unit_type_key, floor_number, unit_number, unit_bedrooms_total, unit_bathrooms_total, unit_total_rent, unit_security_deposit)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (listing_id, unit_type_key) DO UPDATE SET 
            floor_number = EXCLUDED.floor_number,
            unit_number = EXCLUDED.unit_number,
            unit_bedrooms_total = EXCLUDED.unit_bedrooms_total,
            unit_bathrooms_total = EXCLUDED.unit_bathrooms_total,
            unit_total_rent = EXCLUDED.unit_total_rent,
            unit_security_deposit = EXCLUDED.unit_security_deposit
    `, property.ListingId, unitType.UnitTypeKey, unitType.FloorNumber, unitType.UnitNumber, unitType.UnitBedroomsTotal, unitType.UnitBathroomsTotal, unitType.UnitTotalRent, unitType.UnitSecurityDeposit)
		if err != nil {
			logEvent("error", "Error on line 716")
			return err, "line 716"
		}
		rowsAffected, _ := result.RowsAffected()
		logEvent("info", fmt.Sprintf("Rows Affected in unit types: %d", rowsAffected))
	}

	// Insert into the medias table
	for _, media := range property.Media {
		result, err = tx.Exec(`
        INSERT INTO medias (listing_id, media_key, media_url)
        VALUES ($1, $2, $3)
        ON CONFLICT (listing_id, media_key) DO UPDATE SET 
            media_url = EXCLUDED.media_url
    `, property.ListingId, media.MediaKey, media.MediaURL)
		if err != nil {
			logEvent("error", "Error on line 732")
			return err, "line 732"
		}
		rowsAffected, _ := result.RowsAffected()
		logEvent("info", fmt.Sprintf("Rows Affected in medias: %d", rowsAffected))
	}

	// Commit the transaction
	logEvent("info", "Committing transaction to database")
	err = tx.Commit()
	if err != nil {
		logEvent("error", "Failed to commit transaction: "+err.Error())
		return err, "commit line"
	} else {
		logEvent("info", "Transaction committed successfully")
	}
	return nil, ""
}

func deleteProperty(property Property) error {
	// Start a transaction
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Delete from the child tables first to respect foreign key constraints
	_, err = tx.Exec(`DELETE FROM medias WHERE listing_id = $1`, property.ListingId)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`DELETE FROM unit_types WHERE listing_id = $1`, property.ListingId)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`DELETE FROM rooms WHERE listing_id = $1`, property.ListingId)
	if err != nil {
		return err
	}

	// Delete from the properties table
	_, err = tx.Exec(`DELETE FROM properties WHERE listing_id = $1`, property.ListingId)
	if err != nil {
		return err
	}

	// Commit the transaction
	return tx.Commit()
}

func processData(data []Property) {
	for _, property := range data {
		if property.MlgCanView {
			// Insert or update in the database
			err, line := insertOrUpdateProperty(property)
			if err != nil {
				logEvent("trace", "Trace on : "+line+" :"+err.Error())
			}
		} else {
			// Delete from the database
			err := deleteProperty(property)
			if err != nil {
				logEvent("trace", "Trace: "+err.Error())
			}
		}
	}
}

func getLastModificationTimestamp() (time.Time, error) {
	query := "SELECT MAX(modification_timestamp) at time zone 'utc' FROM properties"

	var timestamp time.Time
	err := db.QueryRow(query).Scan(&timestamp)
	if err != nil {
		return time.Time{}, err
	}
	return timestamp, nil
}

func constructUpdateURL(lastTimestamp time.Time) string {
	baseURL := "https://api.mlsgrid.com/v2/Property?$filter=OriginatingSystemName%20eq%20%27mred%27%20and%20MlgCanView%20eq%20true%20and%20ModificationTimestamp%20gt%20"
	timestampStr := lastTimestamp.Format("2006-01-02T15:04:05.999Z")
	return baseURL + timestampStr + "&$expand=Rooms%2CUnitTypes%2CMedia&$top=1000"
}

func makeRequestAndUpdateCounters(url string) (ApiResponse, error) {
	resp, err := makeRequest2(url)

	requestsThisHour++
	requestsToday++

	downloadedGB := float64(dataDownloaded) / float64(1024*1024*1024) // Convert bytes to GB
	logEvent("info", "Able to make a request within rate limits")
	logEvent("info", fmt.Sprintf("Requests this hour: %d. Requests today: %d", requestsThisHour, requestsToday))
	logEvent("info", fmt.Sprintf("Downloaded %.3fGB this hour.", downloadedGB))
	return resp, err
}

func makeRequest2(url string) (ApiResponse, error) {

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		logEvent("error", fmt.Sprintf("Failed to make request to %s. Error: %s", url, err.Error()))
		return ApiResponse{}, err
	}

	// Add Bearer token for authentication
	APIBearerToken := os.Getenv("API_BEARER_TOKEN")
	req.Header.Add("Authorization", "Bearer "+APIBearerToken)

	resp, err := httpClient.Do(req)
	if err != nil {
		return ApiResponse{}, err
	}

	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			logEvent("trace", "Trace: "+err.Error())
		}
	}(resp.Body)

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		logEvent("error", fmt.Sprintf("Received non-200 response from %s. Status: %d. Body: %s", url, resp.StatusCode, string(bodyBytes)))
		return ApiResponse{}, fmt.Errorf("received non-200 response status: %d", resp.StatusCode)
	}

	var bytesRead int64
	cr := &countingReader{r: resp.Body, n: &bytesRead}

	var apiResp ApiResponse

	dec := json.NewDecoder(cr)
	err = dec.Decode(&apiResp)
	if err != nil {
		return ApiResponse{}, err
	}
	dataDownloaded += bytesRead
	return apiResp, nil
}

func main() {
	initialize()
	setupSignalHandlers()
	startTickers()

	handleApiErrors(func() error {
		var nextUrl string = initialUrl
		//timestamp, err := getLastModificationTimestamp()
		//if err != nil {
		//	logEvent("info", "Couldn't get last modification timestamp ")
		//}
		//var nextUrl string := constructUpdateURL(timestamp)

		for {
			if nextUrl == "" {
				logEvent("warn", "Initial import complete!")
				break
			}

			if canMakeRequest() {
				err := withRetry(3, 2*time.Second, func() error {
					resp, err := makeRequestAndUpdateCounters(nextUrl)
					nextUrl = resp.NextLink
					if err != nil {
						logEvent("error", "Error: "+err.Error())
						return err
					}
					// Acquire Semaphore token to process data
					processDataSem <- struct{}{}
					logEvent("info", "Process data worker token acquired")
					// Processing logic, 5 threads max
					go func(response ApiResponse) {
						processData(response.Data)
						// Release the semaphore token once processData completes
						<-processDataSem
						logEvent("info", "Process data worker finished. Releasing token.")
					}(resp)
					return nil
				})
				if err != nil {
					logEvent("error", fmt.Sprintf("Failed after multiple retries: %s", err.Error()))
					// Decide how you want to handle persistent errors here.
					// You might choose to break out of the loop, wait for a longer duration, or alert someone.
				}
			} else {
				timestamp, err := getLastModificationTimestamp()
				if err != nil {
					logEvent("info", "Couldn't get last modification timestamp ")
				}
				nextUrl = constructUpdateURL(timestamp)
				logEvent("info", "Sleeping for a minute. Will check if can make a request after.")
				time.Sleep(1 * time.Minute)
			}
		}
		return nil
	})
}

// https://api.mlsgrid.com/v2/Property?$filter=OriginatingSystemName%20eq%20%27mred%27%20and%20MlgCanView%20eq%20true%20and%20ModificationTimestamp%20gt%202023-10-05T16:47:03.961Z&$expand=Rooms%2CUnitTypes%2CMedia&$top=1000
