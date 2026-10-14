//go:build rhlive

package service

// Live end-to-end test against the real RunningHub API. Costs real credits.
// Needs RH_TEST_DSN plus RH_LIVE_KEY (and RH_LIVE_TIER: consumer|enterprise),
// RH_LIVE_APP, RH_LIVE_IMAGE, RH_LIVE_AUDIO. Uses a throwaway schema like the
// integration tests.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRHLive_FullFlow(t *testing.T) {
	key := os.Getenv("RH_LIVE_KEY")
	if key == "" {
		t.Skip("RH_LIVE_KEY not set")
	}
	tier := os.Getenv("RH_LIVE_TIER")
	if tier == "" {
		tier = RunningHubTierConsumer
	}
	e := newRHEnv(t)
	real := NewRunningHubService(nil)
	e.s.baseURL, e.s.httpClient = real.baseURL, real.httpClient
	e.settings(tier == RunningHubTierConsumer, tier == RunningHubTierEnterprise, 10)
	e.user(1, 100000)
	e.account("live", tier, 2)
	_, err := e.db.ExecContext(e.ctx, `UPDATE runninghub_accounts SET api_key=$1`, key)
	require.NoError(t, err)

	w, err := e.s.CreateWorkflow(e.ctx, RunningHubWorkflowCreate{WebappID: os.Getenv("RH_LIVE_APP"), InstanceType: "plus"})
	require.NoError(t, err)
	t.Logf("工作流节点数: %d", len(w.NodeInfoList))
	on := true
	_, err = e.s.UpdateWorkflow(e.ctx, w.ID, RunningHubWorkflowUpdate{Enabled: &on})
	require.NoError(t, err)

	var overrides []RunningHubNodeOverride
	for _, n := range w.NodeInfoList {
		ft, _ := n["fieldType"].(string)
		var path string
		switch ft {
		case "IMAGE":
			path = os.Getenv("RH_LIVE_IMAGE")
		case "AUDIO":
			path = os.Getenv("RH_LIVE_AUDIO")
		default:
			continue
		}
		f, err := os.Open(path)
		require.NoError(t, err)
		m, err := e.s.StageMedia(e.ctx, 1, false, f, filepath.Base(path), "")
		_ = f.Close()
		require.NoError(t, err)
		overrides = append(overrides, RunningHubNodeOverride{NodeID: n["nodeId"].(string), FieldName: n["fieldName"].(string), FieldValue: m.Ref})
	}
	require.NotEmpty(t, overrides)
	overrides = append(overrides, RunningHubNodeOverride{NodeID: "312", FieldName: "value", FieldValue: "832"})

	task, err := e.s.Submit(e.ctx, 1, w.WebappID, RunningHubSubmitRequest{Nodes: overrides, RateMultiplier: 1})
	require.NoError(t, err)
	t.Logf("提交成功 task=%s 上游=%s 预扣=%.2f", task.ID, e.upstreamID(task.ID), task.ReservedRH)

	start := time.Now()
	for {
		time.Sleep(20 * time.Second)
		got, err := e.s.Query(e.ctx, 1, task.ID)
		require.NoError(t, err)
		if got.SettledAt != nil {
			b, _ := json.MarshalIndent(got, "", "  ")
			t.Logf("耗时 %s 最终结果:\n%s", time.Since(start).Round(time.Second), b)
			t.Logf("用户余额: %.4f (初始 100000)", e.credits(1))
			require.Equal(t, rhStatusSuccess, got.Status)
			return
		}
		if time.Since(start) > 45*time.Minute {
			t.Fatalf("超时，最后状态 %s", got.Status)
		}
		t.Logf("%s 状态=%s", time.Since(start).Round(time.Second), got.Status)
	}
}
