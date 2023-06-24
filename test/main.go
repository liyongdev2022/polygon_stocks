package main

import (
	"context"
	"log"
	"time"

	polygon "github.com/polygon-io/client-go/rest"
	"github.com/polygon-io/client-go/rest/models"
)

func main() {
	// init client
	c := polygon.New("A8e0FPkqfm5oaKJ_YDBPI_ZYMWMyS8y5")

	// set params
	params := models.ListAggsParams{
		Ticker:     "AAPL",
		Multiplier: 5,
		Timespan:   "minute",
		From:       models.Millis(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)),
		To:         models.Millis(time.Date(2023, 3, 9, 0, 0, 0, 0, time.UTC)),
	}.WithOrder(models.Desc).WithLimit(50000).WithAdjusted(true)

	// make request
	iter := c.ListAggs(context.Background(), params)

	// do something with the result
	for iter.Next() {
		log.Print(iter.Item())
	}
	if iter.Err() != nil {
		log.Fatal(iter.Err())
	}
}
