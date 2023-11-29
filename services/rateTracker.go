package services

import (
	"github.com/piotrsenkow/gosyncmls/utils"
	"sync"
)

type RateTracker struct {
	RequestsThisHour int
	RequestsToday    int
	DataDownloaded   int64
	mu               sync.Mutex
	// Add other necessary fields and methods
}

var GlobalRateTracker = NewRateTracker()

func NewRateTracker() *RateTracker {
	return &RateTracker{}
}

func (rt *RateTracker) IncrementRequestsToday() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.RequestsToday++
}

func (rt *RateTracker) IncrementRequestsThisHour() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.RequestsThisHour++
}

func (rt *RateTracker) AddDataDownloaded(size int64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.DataDownloaded += size
}

func (rt *RateTracker) ResetHourlyCounters() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.RequestsThisHour = 0
	rt.DataDownloaded = 0
	utils.LogEvent("info", "Hourly counter reset")
}

func (rt *RateTracker) ResetDailyCounters() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.RequestsToday = 0
	utils.LogEvent("info", "Daily counter reset")
}
