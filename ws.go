package bittrex

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/shopspring/decimal"
	"github.com/thebotguys/signalr"
)

type SummaryState struct {
	MarketName     string
	High           decimal.Decimal
	Low            decimal.Decimal
	Last           decimal.Decimal
	Volume         decimal.Decimal
	BaseVolume     decimal.Decimal
	Bid            decimal.Decimal
	Ask            decimal.Decimal
	OpenBuyOrders  int
	OpenSellOrders int
	TimeStamp      string
	Created        string
}

type ExchangeDelta struct {
	Nounce uint64
	Deltas []SummaryState
}

// doAsyncTimeout runs f in a different goroutine
//	if f returns before timeout elapses, doAsyncTimeout returns the result of f().
//	otherwise it returns "operation timeout" error, and calls tmFunc after f returns.
func doAsyncTimeout(f func() error, tmFunc func(error), timeout time.Duration) error {
	errs := make(chan error)
	go func() {
		err := f()
		select {
		case errs <- err:
		default:
			if tmFunc != nil {
				tmFunc(err)
			}
		}
	}()
	select {
	case err := <-errs:
		return err
	case <-time.After(timeout):
		return errors.New("operation timeout")
	}
}

func subForSummary(client *signalr.Client) ([]SummaryState, error) {
	var result []SummaryState

	_, err := client.CallHub("CoreHub", "SubscribeToSummaryDeltas")
	if err != nil {
		return nil, err
	}
	return result, nil
}

func parseDeltas(messages []json.RawMessage, dataCh chan<- SummaryState) error {

	for _, msg := range messages {
		var d ExchangeDelta
		if err := json.Unmarshal(msg, &d); err != nil {
			return err
		}

		for _, v := range d.Deltas {
			dataCh <- v
		}
	}

	return nil
}

func SubscribeSummaryUpdate(dataCh chan<- SummaryState, stop <-chan bool) error {
	const timeout = 5 * time.Second
	client := signalr.NewWebsocketClient()
	client.OnClientMethod = func(hub string, method string, messages []json.RawMessage) {
		if hub != "CoreHub" || method != "updateSummaryState" {
			return
		}

		parseDeltas(messages, dataCh)
	}

	connect := func() error { return client.Connect("https", "socket.bittrex.com", []string{"CoreHub"}) }
	handleErr := func(err error) {
		if err == nil {
			client.Close()
		} else {
			//should maybe panic or something here?
			fmt.Println(err.Error())
		}
	}

	if err := doAsyncTimeout(connect, handleErr, timeout); err != nil {
		return err
	}
	defer client.Close()
	var initStates []SummaryState

	subForStates := func() error {
		var err error
		initStates, err = subForSummary(client)
		return err
	}

	err := doAsyncTimeout(subForStates, nil, timeout)
	if err != nil {
		return err
	}

	for _, st := range initStates {
		dataCh <- st
	}

	select {
	case <-stop:
	case <-client.DisconnectedChannel:
	}

	return nil
}
