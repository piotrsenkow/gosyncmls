package services

import (
	"fmt"
	"github.com/piotrsenkow/gosyncmls/utils"
	"golang.org/x/time/rate"
	"time"
)

const (
	MaxRequestsPerHour = 7200
	MaxRequestsPerDay  = 40000
	MaxDownloadPerHour = 4 * 1024 * 1024 * 1024 // 4GB in bytes
)

var (
	perSecondLimiter *rate.Limiter
	perHourLimiter   *rate.Limiter
	perDayLimiter    *rate.Limiter
)

// InitializeRateLimiter initializes the rate limiters
func InitializeRateLimiter() {
	perSecondLimiter = rate.NewLimiter(1.95, 2)
	perHourLimiter = rate.NewLimiter(rate.Limit(MaxRequestsPerHour)/3600, MaxRequestsPerHour)
	perDayLimiter = rate.NewLimiter(rate.Limit(MaxRequestsPerDay)/86400, MaxRequestsPerDay)
}

// CanMakeRequest checks if the program can make a request to the MLSGrid API
func CanMakeRequest() bool {
	if !isWithinRateLimit(perDayLimiter, "day") {
		return false
	}
	if !isWithinRateLimit(perHourLimiter, "hour") {
		return false
	}
	if !isWithinRateLimit(perSecondLimiter, "second") {
		return false
	}
	if GlobalRateTracker.DataDownloaded >= MaxDownloadPerHour {
		utils.LogEvent("warn", "4 GB hourly download limit reached!")
	}
	return GlobalRateTracker.RequestsThisHour <= MaxRequestsPerHour &&
		GlobalRateTracker.RequestsToday <= MaxRequestsPerDay && GlobalRateTracker.DataDownloaded <= MaxDownloadPerHour
}

// isWithinRateLimit checks if the program is within the rate limit for a given period
func isWithinRateLimit(limiter *rate.Limiter, period string) bool {
	reserve := limiter.Reserve()
	if !reserve.OK() {
		delay := reserve.Delay()
		utils.LogEvent("warn", fmt.Sprintf("Rate limited per %s, waiting for %v", period, delay))
		time.Sleep(delay)
		return false
	}
	return true
}
