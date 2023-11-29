package services

import (
	"github.com/piotrsenkow/gosyncmls/utils"
	"sync"
)

// RateTracker keeps track of the number of requests made in the last hour and the last day.
type RateTracker struct {
	RequestsThisHour int
	RequestsToday    int
	DataDownloaded   int64
	mu               sync.Mutex
	// Add other necessary fields and methods
}

// GlobalRateTracker is the global instance of RateTracker.
var GlobalRateTracker = NewRateTracker()

// NewRateTracker returns a new instance of RateTracker.
func NewRateTracker() *RateTracker {
	return &RateTracker{}
}

// IncrementRequestsToday increments the number of requests made today.
func (rt *RateTracker) IncrementRequestsToday() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.RequestsToday++
}

// IncrementRequestsThisHour increments the number of requests made in the last hour.
func (rt *RateTracker) IncrementRequestsThisHour() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.RequestsThisHour++
}

// AddDataDownloaded increments the number of bytes downloaded.
func (rt *RateTracker) AddDataDownloaded(size int64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.DataDownloaded += size
}

// ResetHourlyCounters resets the number of requests made in the last hour and the number of bytes downloaded.
func (rt *RateTracker) ResetHourlyCounters() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.RequestsThisHour = 0
	rt.DataDownloaded = 0
	utils.LogEvent("info", "Hourly counter reset")
}

// ResetDailyCounters resets the number of requests made today.
func (rt *RateTracker) ResetDailyCounters() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.RequestsToday = 0
	utils.LogEvent("info", "Daily counter reset")
}
