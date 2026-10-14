package service

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	RunningHubTierConsumer   = "consumer"
	RunningHubTierEnterprise = "enterprise"
)

var (
	ErrRunningHubNoDB         = errors.New("RunningHub requires a database")
	ErrRunningHubPoolBusy     = errors.New("all RunningHub accounts are busy, please retry later")
	ErrRunningHubInsufficient = errors.New("insufficient RH credits")
	ErrRunningHubNotFound     = errors.New("not found")
)

// RunningHubAccount is the public view of a pool entry (no API key).
type RunningHubAccount struct {
	ID             int64  `json:"id"`
	Name           string `json:"name"`
	Tier           string `json:"tier"`
	MaxConcurrency int    `json:"max_concurrency"`
	Enabled        bool   `json:"enabled"`
	InFlight       int    `json:"in_flight"`
}

type RunningHubSettings struct {
	ConsumerEnabled   bool    `json:"consumer_enabled"`
	EnterpriseEnabled bool    `json:"enterprise_enabled"`
	YuanToCoin        float64 `json:"yuan_to_coin"`
}

type RunningHubCredits struct {
	RH float64 `json:"rh_credits"`
}

// internal pool entry – includes the key, never serialised to clients.
type rhAccountRow struct {
	RunningHubAccount
	APIKey string
}

type RunningHubService struct {
	db         *sql.DB
	baseURL    string
	mediaDir   string
	httpClient *http.Client

	stopOnce sync.Once
	stop     chan struct{}
}

func NewRunningHubService(db *sql.DB) *RunningHubService {
	base := strings.TrimRight(os.Getenv("RUNNINGHUB_BASE_URL"), "/")
	if base == "" {
		base = "https://www.runninghub.cn"
	}
	mediaDir := os.Getenv("RUNNINGHUB_MEDIA_DIR")
	if mediaDir == "" {
		dataDir := os.Getenv("DATA_DIR")
		if dataDir == "" {
			dataDir = "data"
		}
		mediaDir = filepath.Join(dataDir, "runninghub-media")
	}
	return &RunningHubService{
		db:         db,
		baseURL:    base,
		mediaDir:   mediaDir,
		httpClient: &http.Client{Timeout: 5 * time.Minute},
		stop:       make(chan struct{}),
	}
}

// ProvideRunningHubService builds the service, bootstraps env accounts and starts
// the background settler.
func ProvideRunningHubService(db *sql.DB) *RunningHubService {
	s := NewRunningHubService(db)
	if db != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		s.bootstrapEnvAccounts(ctx)
		s.reconcileInFlight(ctx)
		cancel()
		go s.runSettler()
	}
	return s
}

func (s *RunningHubService) Stop() {
	s.stopOnce.Do(func() { close(s.stop) })
}

// Optional bootstrap via env: RUNNINGHUB_ACCOUNTS=name:tier:key[:concurrency],...
func (s *RunningHubService) bootstrapEnvAccounts(ctx context.Context) {
	for _, raw := range strings.Split(os.Getenv("RUNNINGHUB_ACCOUNTS"), ",") {
		p := strings.SplitN(strings.TrimSpace(raw), ":", 4)
		if len(p) < 3 || p[2] == "" {
			continue
		}
		maxConc := 3
		if len(p) == 4 {
			if n, err := strconv.Atoi(p[3]); err == nil && n > 0 {
				maxConc = n
			}
		}
		_, _ = s.db.ExecContext(ctx,
			`INSERT INTO runninghub_accounts(name,tier,api_key,max_concurrency)
			 SELECT $1,$2,$3,$4
			 WHERE NOT EXISTS (SELECT 1 FROM runninghub_accounts WHERE name=$1 AND tier=$2)`,
			p[0], p[1], p[2], maxConc)
	}
}

// reconcileInFlight rebuilds in_flight from unsettled tasks, fixing drift left by a crash.
func (s *RunningHubService) reconcileInFlight(ctx context.Context) {
	_, _ = s.db.ExecContext(ctx,
		`UPDATE runninghub_accounts a
		 SET in_flight = (SELECT COUNT(*) FROM runninghub_tasks t
		                  WHERE t.account_id = a.id AND t.settled_at IS NULL)`)
}

// ── Settings ──────────────────────────────────────────────────────────────────

func (s *RunningHubService) GetSettings(ctx context.Context) (RunningHubSettings, error) {
	var st RunningHubSettings
	if s.db == nil {
		return st, ErrRunningHubNoDB
	}
	err := s.db.QueryRowContext(ctx,
		`SELECT consumer_enabled, enterprise_enabled, yuan_to_coin FROM runninghub_settings WHERE id=1`).
		Scan(&st.ConsumerEnabled, &st.EnterpriseEnabled, &st.YuanToCoin)
	if errors.Is(err, sql.ErrNoRows) {
		return RunningHubSettings{ConsumerEnabled: true, YuanToCoin: 1}, nil
	}
	return st, err
}

func (s *RunningHubService) UpdateSettings(ctx context.Context, st RunningHubSettings) (RunningHubSettings, error) {
	if s.db == nil {
		return st, ErrRunningHubNoDB
	}
	if st.YuanToCoin <= 0 {
		return st, errors.New("yuan_to_coin must be positive")
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO runninghub_settings(id, consumer_enabled, enterprise_enabled, yuan_to_coin)
		 VALUES (1,$1,$2,$3)
		 ON CONFLICT (id) DO UPDATE SET consumer_enabled=$1, enterprise_enabled=$2,
		     yuan_to_coin=$3, updated_at=NOW()`,
		st.ConsumerEnabled, st.EnterpriseEnabled, st.YuanToCoin)
	return st, err
}
