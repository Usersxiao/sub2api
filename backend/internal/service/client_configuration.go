package service

// Publication and invocation resolution share one permission evaluation. This
// service never turns panel JWTs into credentials or enables execution features.
import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"
)

const ClientConfigurationSettingKey = "autodesign_client_configuration_v1"
const DefaultClientInstanceID = "sub-0bc3c2f13ccd1516eaa02280"

var clientIdentifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$`)
var clientVersion = regexp.MustCompile(`^(0|[1-9][0-9]{0,8})\.(0|[1-9][0-9]{0,8})\.(0|[1-9][0-9]{0,8})$`)
var clientPurposes = strings.Fields("chat agent design research review automation editing memory search image cover video speech transcription music sound embeddings")
var clientOperations = strings.Fields("text image video speech transcription music sound embeddings search")
var clientProtocols = strings.Fields("responses messages chat-completions images videos speech transcriptions embeddings music sound web-search x-search gemini")

type ClientParameterOption struct {
	Value string `json:"value"`
	Label string `json:"label"`
}

type ClientModelParameter struct {
	Name        string                  `json:"name"`
	Label       string                  `json:"label"`
	Description string                  `json:"description,omitempty"`
	Editable    bool                    `json:"editable"`
	Type        string                  `json:"type"`
	Default     json.RawMessage         `json:"default"`
	MinLength   *int                    `json:"minLength,omitempty"`
	MaxLength   *int                    `json:"maxLength,omitempty"`
	Minimum     *float64                `json:"minimum,omitempty"`
	Maximum     *float64                `json:"maximum,omitempty"`
	Step        *float64                `json:"step,omitempty"`
	Options     []ClientParameterOption `json:"options,omitempty"`
}

type ClientModelRoute struct {
	RouteID string `json:"routeId"`
	Target  struct {
		Kind    string `json:"kind"`
		ModelID string `json:"modelId"`
	} `json:"target"`
	DisplayName     string   `json:"displayName"`
	ProviderDisplay string   `json:"providerDisplay"`
	Purposes        []string `json:"purposes"`
	Operation       string   `json:"operation"`
	Availability    struct {
		Status string `json:"status"`
		Reason string `json:"reason,omitempty"`
	} `json:"availability"`
	Protocol     string `json:"protocol"`
	Transport    string `json:"transport"`
	Capabilities struct {
		Streaming        bool `json:"streaming"`
		Tools            bool `json:"tools"`
		StructuredOutput bool `json:"structuredOutput"`
		Vision           bool `json:"vision"`
		Uploads          bool `json:"uploads"`
		Asynchronous     bool `json:"asynchronous"`
		Cancellation     bool `json:"cancellation"`
	} `json:"capabilities"`
	InputKinds  []string               `json:"inputKinds"`
	OutputKinds []string               `json:"outputKinds"`
	Parameters  []ClientModelParameter `json:"parameters"`
	Limits      struct {
		ContextTokens   *int64   `json:"contextTokens,omitempty"`
		OutputTokens    *int64   `json:"outputTokens,omitempty"`
		FileBytes       *int64   `json:"fileBytes,omitempty"`
		ReferenceImages *int64   `json:"referenceImages,omitempty"`
		DurationSeconds *float64 `json:"durationSeconds,omitempty"`
	} `json:"limits"`
	// Effective pricing is not wired in P2a; only an empty array is accepted.
	Prices []json.RawMessage `json:"prices"`
}

type ClientConfiguredRoute struct {
	GroupIDs []int64          `json:"groupIds"`
	Route    ClientModelRoute `json:"route"`
}

type ClientConfiguration struct {
	SchemaVersion    int                     `json:"schemaVersion"`
	InstanceID       string                  `json:"instanceId"`
	MinClientVersion string                  `json:"minClientVersion"`
	Defaults         map[string]string       `json:"defaults"`
	Routes           []ClientConfiguredRoute `json:"routes"`
}

type ClientConfigurationScope struct {
	SchemaVersion      int    `json:"schemaVersion"`
	InstanceID         string `json:"instanceId"`
	ConfigVersion      string `json:"configVersion"`
	EntitlementVersion string `json:"entitlementVersion"`
	MinClientVersion   string `json:"minClientVersion"`
}

type ClientConfigurationBootstrap struct {
	ClientConfigurationScope
	CatalogRefresh struct {
		PollAfterSeconds int    `json:"pollAfterSeconds"`
		ExpiresAt        string `json:"expiresAt"`
	} `json:"catalogRefresh"`
	Features struct {
		Chat            bool `json:"chat"`
		Editing         bool `json:"editing"`
		Media           bool `json:"media"`
		Runninghub      bool `json:"runninghub"`
		BackgroundTasks bool `json:"backgroundTasks"`
	} `json:"features"`
	Quotas []ClientConfigurationQuota `json:"quotas"`
}

type ClientConfigurationQuota struct {
	Unit      string `json:"unit"`
	Remaining string `json:"remaining"`
	Label     string `json:"label"`
}

type ClientConfigurationCatalog struct {
	ClientConfigurationScope
	GeneratedAt string             `json:"generatedAt"`
	ExpiresAt   string             `json:"expiresAt"`
	ETag        string             `json:"etag"`
	Defaults    map[string]string  `json:"defaults"`
	Routes      []ClientModelRoute `json:"routes"`
}

type ClientConfigurationSnapshot struct {
	SchemaVersion  int                          `json:"schemaVersion"`
	InstanceID     string                       `json:"instanceId"`
	UpstreamUserID string                       `json:"upstreamUserId"`
	Bootstrap      ClientConfigurationBootstrap `json:"bootstrap"`
	Catalog        ClientConfigurationCatalog   `json:"catalog"`
}

type ClientConfigurationSettings interface {
	GetValue(context.Context, string) (string, error)
	Set(context.Context, string, string) error
}
type ClientConfigurationUsers interface {
	GetByID(context.Context, int64) (*User, error)
}
type ClientConfigurationGroups interface {
	GetAvailableGroups(context.Context, int64) ([]Group, error)
}
type ClientConfigurationGroupReader interface {
	GetByID(context.Context, int64) (*Group, error)
}
type ClientConfigurationChannels interface {
	ListAll(context.Context) ([]Channel, error)
}
type ClientConfigurationAccounts interface {
	ListModelAvailabilityCandidates(context.Context, *int64, []string, bool) ([]Account, error)
}
type clientConfigurationModelSupport interface {
	isModelSupportedByAccountWithContext(context.Context, *Account, string) bool
}

type ClientConfigurationService struct {
	settings     ClientConfigurationSettings
	users        ClientConfigurationUsers
	groups       ClientConfigurationGroups
	groupReader  ClientConfigurationGroupReader
	channels     ClientConfigurationChannels
	accounts     ClientConfigurationAccounts
	modelSupport clientConfigurationModelSupport
	composite    *CompositeRouteResolver
	now          func() time.Time
}

func NewClientConfigurationService(settings ClientConfigurationSettings, users ClientConfigurationUsers, groups ClientConfigurationGroups, groupReader ClientConfigurationGroupReader, channels ClientConfigurationChannels, accounts ClientConfigurationAccounts, gateway *GatewayService, composite *CompositeRouteResolver) *ClientConfigurationService {
	return &ClientConfigurationService{settings: settings, users: users, groups: groups, groupReader: groupReader, channels: channels, accounts: accounts, modelSupport: gateway, composite: composite, now: time.Now}
}

func DefaultClientConfiguration() ClientConfiguration {
	return ClientConfiguration{SchemaVersion: 1, InstanceID: DefaultClientInstanceID, MinClientVersion: "0.25.1", Defaults: map[string]string{}, Routes: []ClientConfiguredRoute{}}
}

func ParseClientConfiguration(raw []byte) (*ClientConfiguration, error) {
	if len(raw) > 1024*1024 {
		return nil, fmt.Errorf("configuration exceeds 1 MiB")
	}
	var cfg ClientConfiguration
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("invalid configuration JSON")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("expected one configuration object")
	}
	if err := validateClientConfiguration(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (s *ClientConfigurationService) Get(ctx context.Context) (*ClientConfiguration, error) {
	raw, err := s.settings.GetValue(ctx, ClientConfigurationSettingKey)
	if errors.Is(err, ErrSettingNotFound) || (err == nil && strings.TrimSpace(raw) == "") {
		cfg := DefaultClientConfiguration()
		return &cfg, nil
	}
	if err != nil {
		return nil, err
	}
	return ParseClientConfiguration([]byte(raw))
}

func (s *ClientConfigurationService) Save(ctx context.Context, cfg *ClientConfiguration) error {
	if err := validateClientConfiguration(cfg); err != nil {
		return err
	}
	for _, published := range cfg.Routes {
		for _, id := range published.GroupIDs {
			if _, err := s.groupReader.GetByID(ctx, id); err != nil {
				return fmt.Errorf("route %s references an unavailable group", published.Route.RouteID)
			}
		}
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	if len(raw) > 1024*1024 {
		return fmt.Errorf("configuration exceeds 1 MiB")
	}
	return s.settings.Set(ctx, ClientConfigurationSettingKey, string(raw))
}

// ClientInvocationResolution is internal routing evidence, never a public DTO.
// ModelID remains the published model name; existing gateway mapping and billing
// resolve its channel alias without accepting a client-provided upstream model.
type ClientInvocationResolution struct {
	Scope             ClientConfigurationScope    `json:"-"`
	GroupID           int64                       `json:"-"`
	ModelID           string                      `json:"-"`
	Platform          string                      `json:"-"`
	Route             ClientModelRoute            `json:"-"`
	ChannelMapping    *clientChannelModelBinding  `json:"-"`
	AccountMappings   []clientAccountModelBinding `json:"-"`
	CompositeDecision *CompositeRouteDecision     `json:"-"`
}

type clientRouteCandidate struct {
	GroupID         int64
	Platform        string
	GroupPlatform   string
	ChannelMapping  *clientChannelModelBinding
	AccountMappings []clientAccountModelBinding
	Composite       *clientCompositeRouteBinding
	compositeRoute  *CompositeRouteDecision `json:"-"`
}

type clientChannelModelBinding struct {
	ChannelID   int64
	Platform    string
	SourceModel string
	MappedModel string
}

type clientAccountModelBinding struct {
	AccountID       int64
	AccountPlatform string
	InputModel      string
	UpstreamModel   string
}

type clientCompositeRouteBinding struct {
	Source           string
	GroupID          int64
	PublicModel      string
	TargetPlatform   string
	UpstreamModel    string
	Endpoint         string
	RouteID          int64
	RouteMatchType   string
	RouteTargetModel string
}

type clientRouteEntitlement struct {
	RouteID    string
	Candidates []clientRouteCandidate
}

func (s *ClientConfigurationService) Snapshot(ctx context.Context, userID int64) (*ClientConfigurationSnapshot, error) {
	snapshot, _, err := s.evaluate(ctx, userID)
	return snapshot, err
}

// ResolveInvocation verifies the submitted snapshot versions and returns the
// first eligible group in publication groupIds order. Multiple supported
// platforms in that group require explicit routing before execution; channel
// repository row order never selects a provider or a different billing group.
func (s *ClientConfigurationService) ResolveInvocation(ctx context.Context, userID int64, routeID, configVersion, entitlementVersion string) (*ClientInvocationResolution, error) {
	if !clientIdentifier.MatchString(routeID) || !clientIdentifier.MatchString(configVersion) || !clientIdentifier.MatchString(entitlementVersion) {
		return nil, ErrClientGatewayScope
	}
	snapshot, candidates, err := s.evaluate(ctx, userID)
	if err != nil {
		return nil, err
	}
	if snapshot.Catalog.ConfigVersion != configVersion || snapshot.Catalog.EntitlementVersion != entitlementVersion {
		return nil, ErrClientGatewayChanged
	}
	for _, route := range snapshot.Catalog.Routes {
		if route.RouteID != routeID || route.Availability.Status != "available" {
			continue
		}
		eligible := candidates[routeID]
		if len(eligible) == 0 || (eligible[0].Composite == nil && len(eligible) > 1 && eligible[0].GroupID == eligible[1].GroupID) {
			return nil, ErrClientGatewayRevoked
		}
		selected := eligible[0]
		return &ClientInvocationResolution{Scope: snapshot.Catalog.ClientConfigurationScope,
			GroupID: selected.GroupID, ModelID: route.Target.ModelID, Platform: selected.Platform, Route: route,
			ChannelMapping: selected.ChannelMapping, AccountMappings: selected.AccountMappings, CompositeDecision: selected.compositeRoute}, nil
	}
	return nil, ErrClientGatewayRevoked
}

// evaluate is the sole directory/route permission filter. Its private candidate
// set binds a displayed route to the groups and platforms that actually support
// it, so losing the selected group invalidates earlier grants even when the
// visible model remains available through another group.
func (s *ClientConfigurationService) evaluate(ctx context.Context, userID int64) (*ClientConfigurationSnapshot, map[string][]clientRouteCandidate, error) {
	cfg, err := s.Get(ctx)
	if err != nil {
		return nil, nil, err
	}
	user, err := s.users.GetByID(ctx, userID)
	if err != nil {
		return nil, nil, err
	}
	if user == nil || user.ID != userID || !user.IsActive() || user.DeletedAt != nil {
		return nil, nil, ErrUserNotFound
	}
	groups, err := s.groups.GetAvailableGroups(ctx, userID)
	if err != nil {
		return nil, nil, err
	}
	allowed := make(map[int64]Group, len(groups))
	for _, group := range groups {
		if group.Status == StatusActive {
			allowed[group.ID] = group
		}
	}

	routes := make([]ClientModelRoute, 0)
	entitlements := make([]clientRouteEntitlement, 0)
	candidates := make(map[string][]clientRouteCandidate)
	availableDefaults := make(map[string]ClientModelRoute)
	channels, err := s.channels.ListAll(ctx)
	if err != nil {
		return nil, nil, err
	}
	accountCache := make(map[string][]Account)
	for _, published := range cfg.Routes {
		eligible, err := s.routeCandidates(ctx, published, allowed, channels, accountCache)
		if err != nil {
			return nil, nil, err
		}
		if len(eligible) == 0 {
			continue
		}
		routes = append(routes, published.Route)
		candidates[published.Route.RouteID] = eligible
		entitlements = append(entitlements, clientRouteEntitlement{published.Route.RouteID, eligible})
		if published.Route.Availability.Status == "available" {
			availableDefaults[published.Route.RouteID] = published.Route
		}
	}
	defaults := make(map[string]string)
	for purpose, id := range cfg.Defaults {
		if _, ok := availableDefaults[id]; ok {
			defaults[purpose] = id
		}
	}
	configVersion := clientConfigurationDigest("cfg-", cfg)
	// Role admin deliberately has no shortcut. Sort the membership set, while
	// route candidates preserve administrator-published group priority.
	groupIDs := make([]int64, 0, len(allowed))
	for id := range allowed {
		groupIDs = append(groupIDs, id)
	}
	sort.Slice(groupIDs, func(i, j int) bool { return groupIDs[i] < groupIDs[j] })
	entitlementVersion := clientConfigurationDigest("ent-", struct {
		UserID     int64
		Groups     []int64
		Routes     []ClientModelRoute
		Candidates []clientRouteEntitlement
	}{userID, groupIDs, routes, entitlements})
	scope := ClientConfigurationScope{1, cfg.InstanceID, configVersion, entitlementVersion, cfg.MinClientVersion}
	now := s.now().UTC()
	expires := now.Add(time.Minute).Format(time.RFC3339Nano)
	bootstrap := ClientConfigurationBootstrap{ClientConfigurationScope: scope, Quotas: []ClientConfigurationQuota{}}
	bootstrap.CatalogRefresh.PollAfterSeconds = 30
	bootstrap.CatalogRefresh.ExpiresAt = expires
	if math.IsNaN(user.Balance) || math.IsInf(user.Balance, 0) {
		return nil, nil, fmt.Errorf("invalid account balance")
	}
	bootstrap.Quotas = append(bootstrap.Quotas, ClientConfigurationQuota{"USD", strconv.FormatFloat(user.Balance, 'f', -1, 64), "USD"})
	catalog := ClientConfigurationCatalog{ClientConfigurationScope: scope, GeneratedAt: now.Format(time.RFC3339Nano), ExpiresAt: expires,
		ETag: `"` + clientConfigurationDigest("client-", []string{configVersion, entitlementVersion}) + `"`, Defaults: defaults, Routes: routes}
	return &ClientConfigurationSnapshot{1, cfg.InstanceID, strconv.FormatInt(userID, 10), bootstrap, catalog}, candidates, nil
}

func (s *ClientConfigurationService) routeCandidates(ctx context.Context, published ClientConfiguredRoute, allowed map[int64]Group, channels []Channel, accountCache map[string][]Account) ([]clientRouteCandidate, error) {
	eligible := make([]clientRouteCandidate, 0)
	for _, id := range published.GroupIDs {
		group, ok := allowed[id]
		if !ok || (group.ModelAllowlistEnabled() && !group.ModelAllowlist.Allows(published.Route.Target.ModelID)) {
			continue
		}
		var compositeDecision *CompositeRouteDecision
		var compositeBinding *clientCompositeRouteBinding
		if group.Platform == PlatformComposite {
			endpoint, supported := clientCompositeEndpointForProtocol(published.Route.Protocol)
			if !supported || s.composite == nil {
				continue
			}
			decision, err := s.composite.Resolve(ctx, id, published.Route.Target.ModelID, endpoint)
			if err != nil {
				return nil, err
			}
			if !decision.Matched || decision.GroupID != id || decision.PublicModel != published.Route.Target.ModelID ||
				decision.Endpoint != endpoint || !isConcreteRequestPlatform(decision.TargetPlatform) || !clientLabel(decision.UpstreamModel) {
				continue
			}
			compositeDecision = &decision
			compositeBinding = clientCompositeBinding(decision)
		}
		modelSupportCtx := ctx
		if compositeDecision != nil {
			modelSupportCtx = WithCompositeRouteDecision(ctx, *compositeDecision)
		}
		platforms := make(map[string]clientChannelModelBinding)
		ambiguousPlatforms := make(map[string]bool)
		accountMappingsByPlatform := make(map[string]map[int64]clientAccountModelBinding)
		ambiguousAccountMappings := make(map[string]bool)
		for _, channel := range channels {
			if channel.Status != StatusActive || !clientHasGroup(channel.GroupIDs, id) {
				continue
			}
			for _, model := range channel.SupportedModels() {
				if group.Platform == PlatformComposite {
					if model.Platform != compositeDecision.TargetPlatform ||
						(model.Name != published.Route.Target.ModelID && model.Name != compositeDecision.UpstreamModel) {
						continue
					}
				} else if model.Name != published.Route.Target.ModelID || group.Platform != model.Platform {
					continue
				}
				// Resolve the channel alias before testing account support; group
				// allowlists and the invocation use the published public name.
				mappingCache := newEmptyChannelCache()
				expandMappingToCache(mappingCache, &channel, id, model.Platform)
				mappingSource := model.Name
				if compositeDecision != nil {
					mappingSource = compositeDecision.UpstreamModel
				}
				accountModel := lookupMappingAcrossPlatforms(mappingCache, id, model.Platform, strings.ToLower(mappingSource))
				if accountModel == "" {
					accountModel = mappingSource
				}
				cacheKey := fmt.Sprintf("%d/%s", id, model.Platform)
				accounts, cached := accountCache[cacheKey]
				if !cached {
					accountPlatforms := []string{model.Platform}
					if model.Platform == PlatformAnthropic || model.Platform == PlatformGemini {
						accountPlatforms = append(accountPlatforms, PlatformAntigravity)
					}
					var err error
					accounts, err = s.accounts.ListModelAvailabilityCandidates(ctx, &id, accountPlatforms, false)
					if err != nil {
						return nil, err
					}
					accountCache[cacheKey] = accounts
				}
				for i := range accounts {
					account := &accounts[i]
					mixed := account.Platform == PlatformAntigravity && (model.Platform == PlatformAnthropic || model.Platform == PlatformGemini) && account.IsMixedSchedulingEnabled()
					if !account.IsActive() || !account.Schedulable || (account.Platform != model.Platform && !mixed) {
						continue
					}
					if s.modelSupport.isModelSupportedByAccountWithContext(modelSupportCtx, account, accountModel) {
						binding := clientChannelModelBinding{ChannelID: channel.ID, Platform: model.Platform,
							SourceModel: mappingSource, MappedModel: accountModel}
						if previous, exists := platforms[model.Platform]; exists && previous != binding {
							ambiguousPlatforms[model.Platform] = true
						} else {
							platforms[model.Platform] = binding
						}
						upstreamModel := resolveAccountUpstreamModel(account, accountModel)
						if !clientLabel(upstreamModel) {
							ambiguousAccountMappings[model.Platform] = true
							break
						}
						candidates := accountMappingsByPlatform[model.Platform]
						if candidates == nil {
							candidates = make(map[int64]clientAccountModelBinding)
							accountMappingsByPlatform[model.Platform] = candidates
						}
						accountBinding := clientAccountModelBinding{AccountID: account.ID, AccountPlatform: account.Platform,
							InputModel: accountModel, UpstreamModel: upstreamModel}
						for _, previous := range candidates {
							if previous.UpstreamModel != upstreamModel {
								ambiguousAccountMappings[model.Platform] = true
								break
							}
						}
						candidates[account.ID] = accountBinding
					}
				}
			}
		}
		ordered := make([]string, 0, len(platforms))
		for platform := range platforms {
			if !ambiguousPlatforms[platform] && !ambiguousAccountMappings[platform] {
				ordered = append(ordered, platform)
			}
		}
		sort.Strings(ordered)
		for _, platform := range ordered {
			binding := platforms[platform]
			accountMappings := make([]clientAccountModelBinding, 0, len(accountMappingsByPlatform[platform]))
			for _, accountMapping := range accountMappingsByPlatform[platform] {
				accountMappings = append(accountMappings, accountMapping)
			}
			sort.Slice(accountMappings, func(i, j int) bool {
				if accountMappings[i].AccountID != accountMappings[j].AccountID {
					return accountMappings[i].AccountID < accountMappings[j].AccountID
				}
				return accountMappings[i].AccountPlatform < accountMappings[j].AccountPlatform
			})
			eligible = append(eligible, clientRouteCandidate{GroupID: id, Platform: platform, GroupPlatform: group.Platform,
				ChannelMapping: &binding, AccountMappings: accountMappings, Composite: compositeBinding, compositeRoute: compositeDecision})
		}
	}
	return eligible, nil
}

func clientCompositeEndpointForProtocol(protocol string) (string, bool) {
	switch protocol {
	case "messages":
		return CompositeRouteEndpointMessages, true
	case "responses":
		return CompositeRouteEndpointResponses, true
	case "chat-completions":
		return CompositeRouteEndpointChatCompletions, true
	default:
		return "", false
	}
}

func clientCompositeBinding(decision CompositeRouteDecision) *clientCompositeRouteBinding {
	binding := &clientCompositeRouteBinding{Source: decision.Source, GroupID: decision.GroupID,
		PublicModel: decision.PublicModel, TargetPlatform: decision.TargetPlatform,
		UpstreamModel: decision.UpstreamModel, Endpoint: decision.Endpoint}
	if decision.Route != nil {
		binding.RouteID = decision.Route.ID
		binding.RouteMatchType = decision.Route.MatchType
		binding.RouteTargetModel = decision.Route.UpstreamModel
	}
	return binding
}

func clientConfigurationDigest(prefix string, value any) string {
	raw, _ := json.Marshal(value)
	sum := sha256.Sum256(raw)
	return prefix + hex.EncodeToString(sum[:16])
}

func clientContains(values []string, value string) bool {
	for _, candidate := range values {
		if value == candidate {
			return true
		}
	}
	return false
}
func clientLabel(value string) bool {
	return strings.TrimSpace(value) != "" && len(utf16.Encode([]rune(value))) <= 500
}
func clientHasGroup(ids []int64, id int64) bool {
	for _, candidate := range ids {
		if candidate == id {
			return true
		}
	}
	return false
}

func validateClientConfiguration(cfg *ClientConfiguration) error {
	if cfg == nil || cfg.SchemaVersion != 1 || !clientIdentifier.MatchString(cfg.InstanceID) || !clientVersion.MatchString(cfg.MinClientVersion) || cfg.Defaults == nil || cfg.Routes == nil || len(cfg.Routes) > 200 {
		return fmt.Errorf("invalid schemaVersion, instanceId, minClientVersion, defaults or routes")
	}
	routes := make(map[string]ClientModelRoute)
	for _, published := range cfg.Routes {
		route := published.Route
		if _, duplicate := routes[route.RouteID]; duplicate {
			return fmt.Errorf("duplicate routeId")
		}
		if !clientIdentifier.MatchString(route.RouteID) || route.Target.Kind != "model" || !clientLabel(route.Target.ModelID) || !clientLabel(route.DisplayName) || !clientLabel(route.ProviderDisplay) {
			return fmt.Errorf("invalid route identity or display labels")
		}
		if len(published.GroupIDs) == 0 || len(published.GroupIDs) > 1000 {
			return fmt.Errorf("each route requires groupIds")
		}
		seenGroups := make(map[int64]bool)
		for _, id := range published.GroupIDs {
			if id <= 0 || seenGroups[id] {
				return fmt.Errorf("invalid or duplicate groupIds")
			}
			seenGroups[id] = true
		}
		if !clientContains(clientOperations, route.Operation) || !clientContains(clientProtocols, route.Protocol) || !clientContains([]string{"json", "multipart", "websocket"}, route.Transport) {
			return fmt.Errorf("unsupported operation, protocol or transport; RunningHub is outside this publication")
		}
		if err := clientUniqueSubset(route.Purposes, clientPurposes); err != nil {
			return fmt.Errorf("invalid route purposes")
		}
		if err := clientUniqueSubset(route.InputKinds, strings.Fields("text image audio video file")); err != nil {
			return fmt.Errorf("invalid inputKinds")
		}
		if err := clientUniqueSubset(route.OutputKinds, strings.Fields("text image audio video embedding file")); err != nil {
			return fmt.Errorf("invalid outputKinds")
		}
		if route.Availability.Status == "available" {
			if route.Availability.Reason != "" {
				return fmt.Errorf("available routes omit reason")
			}
		} else if !clientContains([]string{"disabled", "withdrawn", "maintenance"}, route.Availability.Status) || !clientLabel(route.Availability.Reason) {
			return fmt.Errorf("invalid availability")
		}
		if route.Parameters == nil || len(route.Parameters) > 100 || route.Prices == nil || len(route.Prices) != 0 {
			return fmt.Errorf("parameters and prices must be arrays; P2a prices must be empty")
		}
		seenParams := make(map[string]bool)
		for _, parameter := range route.Parameters {
			if seenParams[parameter.Name] {
				return fmt.Errorf("duplicate parameter name")
			}
			seenParams[parameter.Name] = true
			if err := validateClientParameter(parameter); err != nil {
				return fmt.Errorf("route %s parameter %s: %w", route.RouteID, parameter.Name, err)
			}
		}
		for _, limit := range []*int64{route.Limits.ContextTokens, route.Limits.OutputTokens, route.Limits.FileBytes} {
			if limit != nil && (*limit <= 0 || *limit > 9007199254740991) {
				return fmt.Errorf("invalid positive route limit")
			}
		}
		if limit := route.Limits.ReferenceImages; limit != nil && (*limit < 0 || *limit > 9007199254740991) {
			return fmt.Errorf("invalid referenceImages limit")
		}
		if limit := route.Limits.DurationSeconds; limit != nil && (*limit <= 0 || math.IsInf(*limit, 0) || math.IsNaN(*limit)) {
			return fmt.Errorf("invalid durationSeconds limit")
		}
		routes[route.RouteID] = route
	}
	for purpose, id := range cfg.Defaults {
		route, ok := routes[id]
		if !clientContains(clientPurposes, purpose) || !ok || route.Availability.Status != "available" || !clientContains(route.Purposes, purpose) {
			return fmt.Errorf("default must reference an available route for its purpose")
		}
	}
	return nil
}

func clientUniqueSubset(values, allowed []string) error {
	if len(values) == 0 {
		return fmt.Errorf("empty set")
	}
	seen := make(map[string]bool)
	for _, value := range values {
		if seen[value] || !clientContains(allowed, value) {
			return fmt.Errorf("invalid set")
		}
		seen[value] = true
	}
	return nil
}

func validateClientParameter(parameter ClientModelParameter) error {
	name := strings.NewReplacer("-", "", "_", "", ".", "", ":", "").Replace(strings.ToLower(parameter.Name))
	if !clientIdentifier.MatchString(parameter.Name) || clientContains(strings.Fields("apikey accesstoken refreshtoken authorization baseurl endpoint secret password provider"), name) || !clientLabel(parameter.Label) || (parameter.Description != "" && !clientLabel(parameter.Description)) {
		return fmt.Errorf("invalid or reserved parameter name/label")
	}
	if len(parameter.Default) == 0 || !json.Valid(parameter.Default) || bytes.Equal(bytes.TrimSpace(parameter.Default), []byte("null")) {
		return fmt.Errorf("default is required")
	}
	switch parameter.Type {
	case "boolean":
		var value bool
		if json.Unmarshal(parameter.Default, &value) != nil || parameter.Minimum != nil || parameter.Maximum != nil || parameter.Step != nil || parameter.MinLength != nil || parameter.MaxLength != nil || parameter.Options != nil {
			return fmt.Errorf("invalid boolean declaration")
		}
	case "string":
		var value string
		if json.Unmarshal(parameter.Default, &value) != nil || parameter.MinLength == nil || parameter.MaxLength == nil || *parameter.MinLength < 0 || *parameter.MaxLength < *parameter.MinLength || len(utf16.Encode([]rune(value))) < *parameter.MinLength || len(utf16.Encode([]rune(value))) > *parameter.MaxLength || parameter.Minimum != nil || parameter.Maximum != nil || parameter.Step != nil || parameter.Options != nil {
			return fmt.Errorf("invalid string bounds/default")
		}
	case "number", "integer":
		var value float64
		if json.Unmarshal(parameter.Default, &value) != nil || parameter.Minimum == nil || parameter.Maximum == nil || value < *parameter.Minimum || value > *parameter.Maximum || *parameter.Minimum > *parameter.Maximum || parameter.MinLength != nil || parameter.MaxLength != nil || parameter.Options != nil {
			return fmt.Errorf("invalid numeric bounds/default")
		}
		for _, number := range []float64{value, *parameter.Minimum, *parameter.Maximum} {
			if math.IsInf(number, 0) || math.IsNaN(number) || (parameter.Type == "integer" && (math.Trunc(number) != number || math.Abs(number) > 9007199254740991)) {
				return fmt.Errorf("invalid numeric value")
			}
		}
		if parameter.Step != nil {
			steps := (value - *parameter.Minimum) / *parameter.Step
			if *parameter.Step <= 0 || math.IsInf(*parameter.Step, 0) || math.IsNaN(*parameter.Step) || math.IsNaN(steps) || math.IsInf(steps, 0) || math.Abs(steps-math.Round(steps)) > 1e-8 || (parameter.Type == "integer" && math.Trunc(*parameter.Step) != *parameter.Step) {
				return fmt.Errorf("invalid numeric step")
			}
		}
	case "enum":
		var value string
		if json.Unmarshal(parameter.Default, &value) != nil || len(parameter.Options) == 0 || len(parameter.Options) > 100 || parameter.MinLength != nil || parameter.MaxLength != nil || parameter.Minimum != nil || parameter.Maximum != nil || parameter.Step != nil {
			return fmt.Errorf("invalid enum declaration")
		}
		seen := make(map[string]bool)
		found := false
		for _, option := range parameter.Options {
			if seen[option.Value] || !clientLabel(option.Label) {
				return fmt.Errorf("invalid enum option")
			}
			seen[option.Value] = true
			found = found || option.Value == value
		}
		if !found {
			return fmt.Errorf("enum default is not an option")
		}
	default:
		return fmt.Errorf("unsupported parameter type")
	}
	return nil
}
