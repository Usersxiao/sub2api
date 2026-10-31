//go:build unit

package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

type ordinaryDeliveryAdapterUnsupported struct {
	UserPlatformQuotaRepository
	legacyCalls int
}

func (p *ordinaryDeliveryAdapterUnsupported) IncrementUsageWithReset(context.Context, int64, string, float64, time.Time) error {
	p.legacyCalls++
	return errors.New("unexpected legacy writer")
}

type ordinaryDeliveryAdapterSupported struct {
	ordinaryDeliveryAdapterUnsupported
	cmd     *service.UsageBillingCommand
	receipt *service.UsageBillingOrdinaryPlatformReceipt
	result  service.UsageBillingOrdinaryPlatformDeliveryResult
	err     error
}

func (p *ordinaryDeliveryAdapterSupported) ConsumeOrdinaryPlatformQuotaUsage(_ context.Context, cmd *service.UsageBillingCommand, receipt *service.UsageBillingOrdinaryPlatformReceipt) (service.UsageBillingOrdinaryPlatformDeliveryResult, error) {
	p.cmd, p.receipt = cmd, receipt
	return p.result, p.err
}

func (p *ordinaryDeliveryAdapterSupported) GetOrdinaryPlatformQuotaUsageDelivery(_ context.Context, cmd *service.UsageBillingCommand, receipt *service.UsageBillingOrdinaryPlatformReceipt) (*service.UsageBillingOrdinaryPlatformDeliveryResult, error) {
	p.cmd, p.receipt = cmd, receipt
	return &p.result, p.err
}

func TestOrdinaryPlatformQuotaDeliveryAdapterOptionalPortForwardsOrFailsWithoutFallback(t *testing.T) {
	cmd := &service.UsageBillingCommand{RequestID: "adapter-source"}
	receipt := &service.UsageBillingOrdinaryPlatformReceipt{DeliveryMode: service.UsageBillingOrdinaryPlatformDeliveryDBV1}
	unsupported := &ordinaryDeliveryAdapterUnsupported{}
	writer, ok := NewUserPlatformQuotaServiceAdapter(unsupported).(service.UserPlatformQuotaOrdinaryDeliveryRepository)
	require.True(t, ok)
	_, err := writer.ConsumeOrdinaryPlatformQuotaUsage(context.Background(), cmd, receipt)
	require.Error(t, err)
	_, err = writer.GetOrdinaryPlatformQuotaUsageDelivery(context.Background(), cmd, receipt)
	require.Error(t, err)
	require.Zero(t, unsupported.legacyCalls)
	probeErr := errors.New("optional delivery probe")
	probe := &ordinaryDeliveryAdapterSupported{result: service.UsageBillingOrdinaryPlatformDeliveryResult{State: service.OrdinaryPlatformDeliveryCompleted,
		SourceFingerprint: "source", Replayed: true, Outcome: service.UserPlatformQuotaBoundIncrementResult{Status: service.QuotaIncrementSuperseded}}, err: probeErr}
	writer, ok = NewUserPlatformQuotaServiceAdapter(probe).(service.UserPlatformQuotaOrdinaryDeliveryRepository)
	require.True(t, ok)
	result, err := writer.ConsumeOrdinaryPlatformQuotaUsage(context.Background(), cmd, receipt)
	require.ErrorIs(t, err, probeErr)
	require.Equal(t, probe.result, result)
	require.Same(t, cmd, probe.cmd)
	require.Same(t, receipt, probe.receipt)
	read, err := writer.GetOrdinaryPlatformQuotaUsageDelivery(context.Background(), cmd, receipt)
	require.ErrorIs(t, err, probeErr)
	require.Same(t, &probe.result, read)
	require.Zero(t, probe.legacyCalls)
}
