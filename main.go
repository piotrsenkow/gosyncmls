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
	ListingId                    string     `json:"ListingId"`
	PropertyType                 string     `json:"PropertyType"`
	MRDType                      string     `json:"MRD_TYP"`
	MLSStatus                    string     `json:"MlsStatus"`
	OriginalListPrice            float64    `json:"OriginalListPrice"`
	ListPrice                    float64    `json:"ListPrice"`
	ClosePrice                   float64    `json:"ClosePrice"`
	AssociationFee               float64    `json:"AssociationFee"`
	TaxAnnualAmount              float64    `json:"TaxAnnualAmount"`
	TaxYear                      IntValue   `json:"TaxYear"`
	DaysOnMarket                 IntValue   `json:"DaysOnMarket"`
	MlgCanView                   bool       `json:"MlgCanView"`
	MlgCanUse                    []string   `json:"MlgCanUse"`
	StreetNumber                 string     `json:"StreetNumber"`
	StreetName                   string     `json:"StreetName"`
	City                         string     `json:"City"`
	PostalCode                   string     `json:"PostalCode"`
	CountyOrParish               string     `json:"CountyOrParish"`
	Township                     string     `json:"Township"`
	RoomsTotal                   int        `json:"RoomsTotal"`
	BedroomsTotal                IntValue   `json:"BedroomsTotal"`
	BathroomsFull                IntValue   `json:"BathroomsFull"`
	BathroomsHalf                IntValue   `json:"BathroomsHalf"`
	GarageSpaces                 float64    `json:"GarageSpaces"`
	LotSizeAcres                 float64    `json:"LotSizeAcres"`
	LotSizeDimensions            string     `json:"LotSizeDimensions"`
	LivingArea                   float64    `json:"LivingArea"`
	MrdAge                       string     `json:"MRD_AGE"`
	YearBuilt                    IntValue   `json:"YearBuilt"`
	PublicRemarks                string     `json:"PublicRemarks"`
	ModificationTimestamp        time.Time  `json:"ModificationTimestamp"`
	ElementarySchool             string     `json:"ElementarySchool"`
	MiddleOrJuniorSchool         string     `json:"MiddleOrJuniorSchool"`
	HighSchool                   string     `json:"HighSchool"`
	ElementarySchoolDistrict     string     `json:"ElementarySchoolDistrict"`
	MiddleOrJuniorSchoolDistrict string     `json:"MiddleOrJuniorSchoolDistrict"`
	HighSchoolDistrict           string     `json:"HighSchoolDistrict"`
	ListingAgreement             string     `json:"ListingAgreement"`
	WaterfrontYN                 bool       `json:"WaterfrontYN"`
	Model                        string     `json:"Model"`
	UnitTypes                    []UnitType `json:"UnitTypes"`
	Rooms                        []Room     `json:"Rooms"`
	Media                        []Media    `json:"Media"`
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
            listing_agreement, waterfront_yn, model
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, 
            $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, 
            $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40
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
            model = EXCLUDED.model
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
		property.HighSchoolDistrict, property.ListingAgreement, property.WaterfrontYN,
		property.Model,
	)
	if err != nil {
		logEvent("error", "Error on line 328")
		return err, "line 328"
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
			logEvent("error", "Error on line 343")
			return err, "line 343"
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
			logEvent("error", "Error on line 361")
			return err, "line 361"
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
			logEvent("error", "Error on line 374")
			return err, "line 374"
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
		//var nextUrl string = initialUrl
		timestamp, err := getLastModificationTimestamp()
		if err != nil {
			logEvent("info", "Couldn't get last modification timestamp ")
		}
		var nextUrl string = constructUpdateURL(timestamp)

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
