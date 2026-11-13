// PHOSPHOR PATCH-FOR-THE-PATCH (2026-11-13): downstream follow-up to
// Dagger 1.0.0-beta.23 telemetry lifecycle changes (f0a3d728), on otel-go
// 132b1dd82eb2. During an upstream conflict, preserve the caller deadline,
// independent remote metric forwarding, immutable bounded snapshots, and
// command output/verdict separation. See PHOSPHOR_PATCHES.md before dropping
// or replacing this follow-up; its failure probes are the acceptance check.

package otel

import (
	"context"
	"errors"
	"fmt"
)

// CloseContext flushes and shuts down the providers created by Init. The caller
// controls the entire shutdown budget, including SDKs which do not promptly
// return from Shutdown after their context is cancelled. Providers are stopped
// concurrently so a failing signal cannot starve the others of that budget.
//
// A deadline can abandon queued telemetry. This is best-effort shutdown, not an
// acknowledgement that all records reached durable storage. Use Close for the
// existing 30-second shutdown policy.
func CloseContext(ctx context.Context) error {
	var providers []namedShutdown
	if tracerProvider != nil {
		providers = append(providers, namedShutdown{"traces", tracerProvider.Shutdown})
	}
	if logs := LoggerProvider(closeCtx); logs != nil {
		providers = append(providers, namedShutdown{"logs", logs.Shutdown})
	}
	if metrics := MeterProvider(closeCtx); metrics != nil {
		providers = append(providers, namedShutdown{"metrics", metrics.Shutdown})
	}
	return shutdownWithin(ctx, providers)
}

type namedShutdown struct {
	name     string
	shutdown func(context.Context) error
}

func shutdownWithin(ctx context.Context, providers []namedShutdown) error {
	results := make(chan error, len(providers))
	for _, provider := range providers {
		go func() {
			err := provider.shutdown(ctx)
			if err != nil {
				err = fmt.Errorf("%s: %w", provider.name, err)
			}
			results <- err
		}()
	}
	var errs []error
	for range providers {
		select {
		case err := <-results:
			errs = append(errs, err)
		case <-ctx.Done():
			return errors.Join(append(errs, ctx.Err())...)
		}
	}
	return errors.Join(errs...)
}
