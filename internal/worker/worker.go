package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/flyertea/idlemesh-ai/internal/api"
)

type Config struct {
	ID           string
	Engine       string
	Model        string
	PollInterval time.Duration
	JobTimeout   time.Duration
	Logger       *log.Logger
}

// Run claims one job at a time until ctx is canceled.
func Run(ctx context.Context, client *api.Client, runner Runner, cfg Config) error {
	if client == nil || runner == nil || !api.ValidWorkerID(cfg.ID) || !api.ValidCapability(cfg.Engine, cfg.Model) ||
		cfg.PollInterval <= 0 || cfg.JobTimeout <= 0 {
		return errors.New("worker requires a client, runner, ID, engine, model, positive poll interval and job timeout")
	}
	if cfg.Logger == nil {
		cfg.Logger = log.Default()
	}
	request := api.ClaimRequest{WorkerID: cfg.ID, Engine: cfg.Engine, Model: cfg.Model}
	var retry time.Duration
	for ctx.Err() == nil {
		claimStarted := time.Now()
		lease, err := client.Claim(ctx, request)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if permanent(err) {
				return fmt.Errorf("claim job: %s", coordinatorError(err))
			}
			retry = nextBackoff(retry, cfg.PollInterval)
			cfg.Logger.Printf("claim failed (%s), retrying in %s", coordinatorError(err), retry)
			wait(ctx, retry)
			continue
		}
		retry = 0
		if lease == nil {
			wait(ctx, cfg.PollInterval)
			continue
		}
		ttl := leaseTTL(lease)
		if ttl <= 0 || lease.ID == "" || lease.Token == "" || lease.Model != cfg.Model ||
			lease.Engine != cfg.Engine || lease.MaxTokens < 1 {
			return errors.New("coordinator returned an invalid or mismatched job lease")
		}
		if err := runLease(ctx, client, runner, cfg, lease, claimStarted, ttl); err != nil {
			return err
		}
	}
	return nil
}

func runLease(ctx context.Context, client *api.Client, runner Runner, cfg Config, lease *api.Lease, claimedAt time.Time, ttl time.Duration) error {
	if !time.Now().Before(claimedAt.Add(ttl)) {
		cfg.Logger.Printf("job %s lease arrived too late", lease.ID)
		return nil
	}
	leaseCtx, cancelLease := context.WithCancel(ctx)
	renewed := make(chan struct{})
	go func() {
		defer close(renewed)
		defer cancelLease()
		renew(leaseCtx, client, cfg, lease, claimedAt, ttl)
	}()
	defer func() {
		cancelLease()
		<-renewed
	}()

	cfg.Logger.Printf("job %s claimed (attempt %d)", lease.ID, lease.Attempt)
	jobCtx, cancelJob := context.WithTimeout(leaseCtx, cfg.JobTimeout)
	output, runErr := runner(jobCtx, *lease)
	if jobCtx.Err() != nil {
		runErr = jobCtx.Err()
	}
	cancelJob()
	if leaseCtx.Err() != nil {
		return nil
	}
	if runErr == nil && len(output) > api.MaxOutputBytes {
		runErr = fmt.Errorf("inference output exceeds the %d-byte result limit", api.MaxOutputBytes)
	}
	result := api.FinishRequest{Token: lease.Token, Output: output}
	if runErr != nil {
		result.Output = ""
		result.Error = shortError(runErr)
	}
	for attempt := 1; attempt <= 3; attempt++ {
		finishCtx, cancelFinish := context.WithTimeout(leaseCtx, min(ttl, 5*time.Second))
		err := client.Finish(finishCtx, lease.ID, result)
		cancelFinish()
		if err == nil {
			cfg.Logger.Printf("job %s outcome submitted", lease.ID)
			return nil
		}
		if leaseCtx.Err() != nil || stale(err) {
			return nil
		}
		if permanent(err) {
			return fmt.Errorf("finish job %s: %s", lease.ID, coordinatorError(err))
		}
		cfg.Logger.Printf("job %s outcome submission failed: %s", lease.ID, coordinatorError(err))
		if attempt < 3 && !wait(leaseCtx, min(250*time.Millisecond, ttl/3)) {
			return nil
		}
	}
	return nil
}

func renew(ctx context.Context, client *api.Client, cfg Config, lease *api.Lease, grantedAt time.Time, ttl time.Duration) {
	for ctx.Err() == nil {
		expires := grantedAt.Add(ttl)
		delay := time.Until(grantedAt.Add(ttl / 3))
		for wait(ctx, min(delay, time.Until(expires))) {
			started := time.Now()
			if !started.Before(expires) {
				cfg.Logger.Printf("job %s lease expired", lease.ID)
				return
			}
			renewCtx, cancel := context.WithDeadline(ctx, expires)
			update, err := client.Renew(renewCtx, lease.ID, api.LeaseRequest{Token: lease.Token})
			cancel()
			if err == nil {
				ttl = leaseTTL(update)
				if ttl <= 0 || update.ID != lease.ID {
					cfg.Logger.Printf("job %s received an invalid renewal", lease.ID)
					return
				}
				grantedAt = started
				break
			}
			if ctx.Err() != nil {
				return
			}
			if permanent(err) {
				cfg.Logger.Printf("job %s lease lost: %s", lease.ID, coordinatorError(err))
				return
			}
			delay = min(ttl/3, time.Second)
		}
	}
}

func leaseTTL(lease *api.Lease) time.Duration {
	if lease == nil || lease.TTLMillis <= 0 || lease.TTLMillis > math.MaxInt64/int64(time.Millisecond) {
		return 0
	}
	return time.Duration(lease.TTLMillis) * time.Millisecond
}

func stale(err error) bool {
	return api.IsStatus(err, http.StatusNotFound) || api.IsStatus(err, http.StatusConflict) ||
		api.IsStatus(err, http.StatusGone)
}

func permanent(err error) bool {
	code := api.StatusCode(err)
	return code >= 300 && code < 500 && code != http.StatusTooManyRequests && code != http.StatusRequestTimeout
}

func nextBackoff(previous, initial time.Duration) time.Duration {
	const limit = 30 * time.Second
	if previous == 0 {
		return min(initial, limit)
	}
	return min(previous*2, limit)
}

func wait(ctx context.Context, duration time.Duration) bool {
	if ctx.Err() != nil {
		return false
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func shortError(err error) string {
	message := strings.Join(strings.Fields(err.Error()), " ")
	if message == "" {
		return "inference failed"
	}
	const limit = 240
	if len(message) <= limit {
		return message
	}
	end := limit - len("...")
	for end > 0 && !utf8.RuneStart(message[end]) {
		end--
	}
	return message[:end] + "..."
}

func coordinatorError(err error) string {
	if code := api.StatusCode(err); code != 0 {
		return fmt.Sprintf("coordinator returned HTTP %d", code)
	}
	return shortError(err)
}
