package models

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"
)

// IntValue is a custom type that allows us to parse the MLSGrid integer format
type IntValue int

// CustomTime is a custom type that allows us to parse the MLSGrid timestamp format
type CustomTime time.Time

// Property is the struct that represents the MLSGrid Property object
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
	RoomsTotal                   IntValue  `json:"RoomsTotal"`
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

	MLSAreaMajor           string     `json:"MLSAreaMajor"`
	MRD_ACTUALSTATUS       string     `json:"MRD_ACTUALSTATUS"`
	MRD_ACTV_DATE          CustomTime `json:"MRD_ACTV_DATE"`
	AssociationFeeIncludes []string   `json:"AssociationFeeIncludes"`
	MRD_ASQ                string     `json:"MRD_ASQ"`
	MRD_ASSESSOR_SQFT      string     `json:"MRD_ASSESSOR_SQFT"`
	MRD_BB                 string     `json:"MRD_BB"`
	MRD_BLDG_ON_LAND       string     `json:"MRD_BLDG_ON_LAND"`
	MRD_BMD                string     `json:"MRD_BMD"`
	MRD_BRBELOW            string     `json:"MRD_BRBELOW"`
	MRD_CAN_OWNER_RENT     string     `json:"MRD_CAN_OWNER_RENT"`
	MRD_CURRENTLYLEASED    string     `json:"MRD_CURRENTLYLEASED"`
	MRD_DEED_GARAGE_COST   string     `json:"MRD_DEED_GARAGE_COST"`
	MRD_DIN                string     `json:"MRD_DIN"`
	MRD_DISABILITY_ACCESS  string     `json:"MRD_DISABILITY_ACCESS"`
	MRD_EXT                string     `json:"MRD_EXT"`
	MRD_FIREPLACE_LOCATION string     `json:"MRD_FIREPLACE_LOCATION"`
	MRD_FULL_BATHS_BLDG    string     `json:"MRD_FULL_BATHS_BLDG"`
	MRD_GARAGE_ONSITE      string     `json:"MRD_GARAGE_ONSITE"`
	MRD_GARAGE_OWNERSHIP   string     `json:"MRD_GARAGE_OWNERSHIP"`
	MRD_GARAGE_TYPE        string     `json:"MRD_GARAGE_TYPE"`
	MRD_SP_INCL_PARKING    string     `json:"MRD_SP_INCL_PARKING"`
	MRD_HALF_BATHS_BLDG    string     `json:"MRD_HALF_BATHS_BLDG"`
	MRD_IDX                string     `json:"MRD_IDX"`
	MRD_LSZ                string     `json:"MRD_LSZ"`
	MRD_MAF                string     `json:"MRD_MAF"`

	GrossIncome          IntValue `json:"GrossIncome"`
	AdditionalParcelsYN  bool     `json:"AdditionalParcelsYN"`
	ParcelNumber         string   `json:"ParcelNumber"`
	ExpirationDate       string   `json:"ExpirationDate"`
	MRD_MASTER_ASSOC_FEE string   `json:"MRD_MASTER_ASSOC_FEE"`
	MRD_MAIN_SQFT        string   `json:"MRD_MAIN_SQFT"`
	MRD_UNIT_SQFT        string   `json:"MRD_UNIT_SQFT"`
	MRD_UPPER_SQFT       string   `json:"MRD_UPPER_SQFT"`
	MRD_LOWER_SQFT       string   `json:"MRD_LOWER_SQFT"`
	Ownership            string   `json:"Ownership"`
	SubdivisionName      string   `json:"SubdivisionName"`
	MRD_MGT              string   `json:"MRD_MGT"`
	MRD_MIN              string   `json:"MRD_MIN"`
	MRD_MIN_LP           string   `json:"MRD_MIN_LP"`
	MRD_MAX_LP           string   `json:"MRD_MAX_LP"`
	MRD_MIN_RP           string   `json:"MRD_MIN_RP"`
	MRD_MAX_RP           string   `json:"MRD_MAX_RP"`

	CumulativeDaysOnMarket IntValue `json:"CumulativeDaysOnMarket"`
	LeaseTerm              string   `json:"LeaseTerm"`
	MRD_NEW_CONSTR_YN      string   `json:"MRD_NEW_CONSTR_YN"`
	MRD_ORP                string   `json:"MRD_ORP"`
	MRD_AON                string   `json:"MRD_AON"`
	MRD_B78                string   `json:"MRD_B78"`
	MRD_BAS                string   `json:"MRD_BAS"`
	MRD_BD3                string   `json:"MRD_BD3"`
	CloseDate              string   `json:"CloseDate"`
	FrontageLength         string   `json:"FrontageLength"`

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

	NetOperatingIncome     IntValue   `json:"NetOperatingIncome"`
	NewConstructionYN      bool       `json:"NewConstructionYN"`
	OffMarketDate          string     `json:"OffMarketDate"`
	OperatingExpense       IntValue   `json:"OperatingExpense"`
	OriginalEntryTimestamp CustomTime `json:"OriginalEntryTimestamp"`
	OtherEquipment         []string   `json:"OtherEquipment"`
	OtherStructures        []string   `json:"OtherStructures"`
	ParkingTotal           IntValue   `json:"ParkingTotal"`
	PostalCodePlus4        string     `json:"PostalCodePlus4"`
	PreviousListPrice      IntValue   `json:"PreviousListPrice"`
	PurchaseContractDate   string     `json:"PurchaseContractDate"`
	RentIncludes           []string   `json:"RentIncludes"`
	StandardStatus         string     `json:"StandardStatus"`
	StateOrProvince        string     `json:"StateOrProvince"`
	StatusChangeTimestamp  CustomTime `json:"StatusChangeTimestamp"`
	StreetDirPrefix        string     `json:"StreetDirPrefix"`
	StreetSuffix           string     `json:"StreetSuffix"`
	TotalActualRent        IntValue   `json:"TotalActualRent"`
	TrashExpense           IntValue   `json:"TrashExpense"`
	WaterSewerExpense      IntValue   `json:"WaterSewerExpense"`
	Zoning                 string     `json:"Zoning"`
	ListAgentEmail         string     `json:"ListAgentEmail"`
	ListAgentFirstName     string     `json:"ListAgentFirstName"`
	ListAgentLastName      string     `json:"ListAgentLastName"`
	ListAgentFullName      string     `json:"ListAgentFullName"`
	ListAgentMlsId         string     `json:"ListAgentMlsId"`
	ListAgentMobilePhone   string     `json:"ListAgentMobilePhone"`
	ListAgentKey           string     `json:"ListAgentKey"`
	ListOfficeMlsId        string     `json:"ListOfficeMlsId"`
	ListOfficeName         string     `json:"ListOfficeName"`
	ListOfficePhone        string     `json:"ListOfficePhone"`
	ListingContractDate    string     `json:"ListingContractDate"`

	UnitTypes []UnitType `json:"UnitTypes"`
	Rooms     []Room     `json:"Rooms"`
	Media     []Media    `json:"Media"`
}

// Room is the struct that represents the MLSGrid Room object
type Room struct {
	MrdFlooring    string `json:"MRD_Flooring"`
	RoomLevel      string `json:"RoomLevel"`
	RoomDimensions string `json:"RoomDimensions"`
	RoomType       string `json:"RoomType"`
	RoomKey        string `json:"RoomKey"`
}

// UnitType is the struct that represents the MLSGrid UnitType object
type UnitType struct {
	UnitTypeKey         string   `json:"UnitTypeKey"`
	FloorNumber         string   `json:"MRD_FloorNumber"`
	UnitNumber          string   `json:"UnitTypeType"`
	UnitBedroomsTotal   IntValue `json:"UnitTypeBedsTotal"`
	UnitBathroomsTotal  IntValue `json:"UnitTypeBathsTotal"`
	UnitTotalRent       IntValue `json:"UnitTypeActualRent"`
	UnitSecurityDeposit string   `json:"MRD_SecurityDeposit"`
}

// Media is the struct that represents the MLSGrid Media object
type Media struct {
	MediaKey string `json:"MediaKey"`
	MediaURL string `json:"MediaURL"`
}

// ApiResponse is the struct that represents the MLSGrid API response
type ApiResponse struct {
	Data     []Property `json:"value"`
	NextLink string     `json:"@odata.nextLink"`
}

// UnmarshalJSON is a custom unmarshaler for the IntValue type
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

// UnmarshalJSON is a custom unmarshaler for the CustomTime type
func (ct *CustomTime) UnmarshalJSON(b []byte) error {
	var strTime string
	if err := json.Unmarshal(b, &strTime); err != nil {
		return err
	}

	parsedTime, err := time.Parse(time.RFC3339, strTime)
	if err != nil {
		// Try parsing without timezone information
		parsedTime, err = time.Parse("2006-01-02T15:04:05", strTime)
		if err != nil {
			return err
		}
	}

	*ct = CustomTime(parsedTime)
	return nil
}

// Value is a driver.Value interface method for CustomTime
func (ct CustomTime) Value() (driver.Value, error) {
	// Convert CustomTime to time.Time, then to driver.Value (which is just interface{})
	t := time.Time(ct)
	if t.IsZero() {
		// Return NULL if zero time
		return nil, nil
	}
	return t, nil
}
