package services

import (
	"github.com/piotrsenkow/gosyncmls/utils"
	"sync"
	"time"
)

var (
	hourTicker        *time.Ticker
	dayTicker         *time.Ticker
	tickersStarted    bool
	startTickersMutex sync.Mutex
)

func StartTickers() {
	startTickersMutex.Lock()
	defer startTickersMutex.Unlock()

	if tickersStarted {
		return
	}

	hourTicker = time.NewTicker(1 * time.Hour)
	dayTicker = time.NewTicker(24 * time.Hour)

	// Anonymous function running on a GoThread that handles resetting of hourly / daily requests using time tickers
	go func() {
		for {
			select {
			case <-hourTicker.C:
				GlobalRateTracker.ResetHourlyCounters()
				utils.LogEvent("info", "Hourly counter reset")
			case <-dayTicker.C:
				GlobalRateTracker.ResetDailyCounters()
				utils.LogEvent("info", "Daily counter reset")
			}
		}
	}()

	tickersStarted = true
}
