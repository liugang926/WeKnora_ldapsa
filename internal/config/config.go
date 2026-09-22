package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/go-ldap/ldap/v3"
	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

// Config 应用程序总配置
type Config struct {
	Conversation    *ConversationConfig    `yaml:"conversation"     json:"conversation"`
	Server          *ServerConfig          `yaml:"server"           json:"server"`
	KnowledgeBase   *KnowledgeBaseConfig   `yaml:"knowledge_base"   json:"knowledge_base"`
	Tenant          *TenantConfig          `yaml:"tenant"           json:"tenant"`
	Auth            *AuthConfig            `yaml:"auth"             json:"auth"`
	Audit           *AuditConfig           `yaml:"audit"            json:"audit"`
	OIDCAuth        *OIDCAuthConfig        `yaml:"oidc_auth"        json:"oidc_auth"`
	Directory       *DirectoryConfig       `yaml:"directory"        json:"directory"`
	Models          []ModelConfig          `yaml:"models"           json:"models"`
	VectorDatabase  *VectorDatabaseConfig  `yaml:"vector_database"  json:"vector_database"`
	DocReader       *DocReaderConfig       `yaml:"docreader"        json:"docreader"`
	StreamManager   *StreamManagerConfig   `yaml:"stream_manager"   json:"stream_manager"`
	ExtractManager  *ExtractManagerConfig  `yaml:"extract"          json:"extract"`
	WebSearch       *WebSearchConfig       `yaml:"web_search"       json:"web_search"`
	PromptTemplates *PromptTemplatesConfig `yaml:"prompt_templates" json:"prompt_templates"`
	IM              *IMConfig              `yaml:"im"               json:"im"`
	Agent           *AgentConfig           `yaml:"agent"            json:"agent"`
	// FrontendBaseURL is the externally-visible origin of the SPA, used
	// to compose absolute share-link URLs. Empty falls back to a host-
	// relative URL ("/register?token=…") which the SPA then resolves
	// against window.location.origin — fine for typical single-origin
	// deployments. Sourced from FRONTEND_BASE_URL env at startup.
	FrontendBaseURL string `yaml:"frontend_base_url" json:"frontend_base_url"`
}

// AgentConfig represents the global agent settings.
type AgentConfig struct {
	// LLMCallTimeout is the default timeout for a single LLM call in seconds.
	// Default: 120 (standard agents) or 300 (can be overridden by Env).
	LLMCallTimeout int `yaml:"llm_call_timeout" json:"llm_call_timeout"`
	// ToolApprovalTimeoutSeconds is how long the agent waits for human approval on a flagged MCP tool.
	// 0 means default 600 (10 minutes).
	ToolApprovalTimeoutSeconds int `yaml:"tool_approval_timeout_seconds" json:"tool_approval_timeout_seconds"`
}

// IMConfig configures the IM integration service.
// All fields are optional — zero values fall back to built-in defaults so
// existing deployments need no config changes.
type IMConfig struct {
	// Workers is the number of concurrent QA worker goroutines per instance.
	// Default: 5.
	Workers int `yaml:"workers" json:"workers"`
	// GlobalMaxWorkers is the maximum number of QA requests that can execute
	// concurrently across ALL instances. Enforced via a Redis counter; when the
	// global limit is reached, local workers wait until a slot opens.
	// Requires Redis — ignored in single-instance mode.
	// 0 (default) means no global limit.
	GlobalMaxWorkers int `yaml:"global_max_workers" json:"global_max_workers"`
	// MaxQueueSize is the maximum number of pending QA requests per instance.
	// Default: 50.
	MaxQueueSize int `yaml:"max_queue_size" json:"max_queue_size"`
	// MaxPerUser limits how many requests a single user can have queued globally.
	// Default: 3.
	MaxPerUser int `yaml:"max_per_user" json:"max_per_user"`
	// RateLimitWindow is the sliding window duration for per-user rate limiting.
	// Default: 60s.
	RateLimitWindow time.Duration `yaml:"rate_limit_window" json:"rate_limit_window"`
	// RateLimitMax is the maximum number of requests allowed per window per user.
	// Default: 10.
	RateLimitMax int `yaml:"rate_limit_max" json:"rate_limit_max"`
}

// DocReaderConfig configures the document parser client (gRPC or HTTP).
type DocReaderConfig struct {
	// Addr: for gRPC it is the server address (e.g. "localhost:50051"); for HTTP it is the base URL (e.g. "http://localhost:8080").
	Addr string `yaml:"addr" json:"addr"`
	// Transport: "grpc" (default) or "http"
	Transport string `yaml:"transport" json:"transport"`
}

type VectorDatabaseConfig struct {
	Driver string `yaml:"driver" json:"driver"`
}

// ConversationConfig 对话服务配置
type ConversationConfig struct {
	MaxRounds            int            `yaml:"max_rounds"                       json:"max_rounds"`
	KeywordThreshold     float64        `yaml:"keyword_threshold"                json:"keyword_threshold"`
	EmbeddingTopK        int            `yaml:"embedding_top_k"                  json:"embedding_top_k"`
	VectorThreshold      float64        `yaml:"vector_threshold"                 json:"vector_threshold"`
	RerankTopK           int            `yaml:"rerank_top_k"                     json:"rerank_top_k"`
	RerankThreshold      float64        `yaml:"rerank_threshold"                 json:"rerank_threshold"`
	FallbackStrategy     string         `yaml:"fallback_strategy"                json:"fallback_strategy"`
	FallbackResponse     string         `yaml:"fallback_response"                json:"fallback_response"`
	EnableRewrite        bool           `yaml:"enable_rewrite"                   json:"enable_rewrite"`
	EnableQueryExpansion bool           `yaml:"enable_query_expansion"           json:"enable_query_expansion"`
	EnableRerank         bool           `yaml:"enable_rerank"                    json:"enable_rerank"`
	Summary              *SummaryConfig `yaml:"summary"                          json:"summary"`

	// Prompt template ID fields — resolved to text by backfillConversationDefaults
	FallbackPromptID             string `yaml:"fallback_prompt_id"                json:"fallback_prompt_id"`
	RewritePromptID              string `yaml:"rewrite_prompt_id"                 json:"rewrite_prompt_id"`
	GenerateSessionTitlePromptID string `yaml:"generate_session_title_prompt_id"  json:"generate_session_title_prompt_id"`
	GenerateSummaryPromptID      string `yaml:"generate_summary_prompt_id"        json:"generate_summary_prompt_id"`
	ExtractEntitiesPromptID      string `yaml:"extract_entities_prompt_id"        json:"extract_entities_prompt_id"`
	ExtractRelationshipsPromptID string `yaml:"extract_relationships_prompt_id"   json:"extract_relationships_prompt_id"`
	GenerateQuestionsPromptID    string `yaml:"generate_questions_prompt_id"      json:"generate_questions_prompt_id"`

	// GenerateKBDescriptionPromptID selects the knowledge-base description template.
	GenerateKBDescriptionPromptID string `yaml:"generate_kb_description_prompt_id" json:"generate_kb_description_prompt_id"` //nolint:lll // one-line struct tag

	// Resolved prompt text fields (populated by backfill, not from YAML)
	FallbackPrompt             string `yaml:"-" json:"fallback_prompt"`
	RewritePromptSystem        string `yaml:"-" json:"rewrite_prompt_system"`
	RewritePromptUser          string `yaml:"-" json:"rewrite_prompt_user"`
	GenerateSessionTitlePrompt string `yaml:"-" json:"generate_session_title_prompt"`
	GenerateSummaryPrompt      string `yaml:"-" json:"generate_summary_prompt"`
	ExtractEntitiesPrompt      string `yaml:"-" json:"extract_entities_prompt"`
	ExtractRelationshipsPrompt string `yaml:"-" json:"extract_relationships_prompt"`
	GenerateQuestionsPrompt    string `yaml:"-" json:"generate_questions_prompt"`

	// GenerateKBDescriptionPrompt is the resolved knowledge-base description template text.
	GenerateKBDescriptionPrompt string `yaml:"-" json:"generate_kb_description_prompt"`

	// IntentSystemPrompts maps intent values (e.g. "greeting", "chitchat") to
	// system prompt text. Populated by backfill from IntentPrompts templates.
	IntentSystemPrompts map[string]string `yaml:"-" json:"-"`
}

// SummaryConfig 摘要配置
type SummaryConfig struct {
	MaxInputChars       int     `yaml:"max_input_chars"       json:"max_input_chars"` // Max input characters for summary generation (default: 16384)
	MaxTokens           int     `yaml:"max_tokens"            json:"max_tokens"`
	RepeatPenalty       float64 `yaml:"repeat_penalty"        json:"repeat_penalty"`
	TopK                int     `yaml:"top_k"                 json:"top_k"`
	TopP                float64 `yaml:"top_p"                 json:"top_p"`
	FrequencyPenalty    float64 `yaml:"frequency_penalty"     json:"frequency_penalty"`
	PresencePenalty     float64 `yaml:"presence_penalty"      json:"presence_penalty"`
	Temperature         float64 `yaml:"temperature"           json:"temperature"`
	Seed                int     `yaml:"seed"                  json:"seed"`
	MaxCompletionTokens int     `yaml:"max_completion_tokens" json:"max_completion_tokens"`
	NoMatchPrefix       string  `yaml:"no_match_prefix"       json:"no_match_prefix"`
	Thinking            *bool   `yaml:"thinking"              json:"thinking"`

	// Prompt template ID fields — resolved to text by backfillConversationDefaults
	PromptID          string `yaml:"prompt_id"           json:"prompt_id"`
	ContextTemplateID string `yaml:"context_template_id" json:"context_template_id"`

	// Resolved prompt text fields (populated by backfill, not from YAML)
	Prompt          string `yaml:"-" json:"prompt"`
	ContextTemplate string `yaml:"-" json:"context_template"`
}

// ServerConfig 服务器配置
type ServerConfig struct {
	Port            int           `yaml:"port"             json:"port"`
	Host            string        `yaml:"host"             json:"host"`
	LogPath         string        `yaml:"log_path"         json:"log_path"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout" json:"shutdown_timeout" default:"30s"`
}

// KnowledgeBaseConfig 知识库配置
type KnowledgeBaseConfig struct {
	ChunkSize              int                    `yaml:"chunk_size"       json:"chunk_size"`
	ChunkOverlap           int                    `yaml:"chunk_overlap"    json:"chunk_overlap"`
	SplitMarkers           []string               `yaml:"split_markers"    json:"split_markers"`
	KeepSeparator          bool                   `yaml:"keep_separator"   json:"keep_separator"`
	ImageProcessing        *ImageProcessingConfig `yaml:"image_processing" json:"image_processing"`
	DocumentProcessTimeout time.Duration          `yaml:"document_process_timeout"  json:"document_process_timeout"`
	// DocReaderCallTimeout caps a single DocReader RPC. Without this the
	// gRPC call inherits the asynq task context (whole DocumentProcessTimeout,
	// default 2h+), so a hung docreader would block a worker for hours and
	// leave knowledge in "processing". Default 30 minutes is generous enough
	// for OCR-heavy large PDFs while ensuring forward progress.
	DocReaderCallTimeout time.Duration `yaml:"docreader_call_timeout"   json:"docreader_call_timeout"`
}

// DefaultDocumentProcessTimeout is the ceiling for a single document:process
// Asynq task when document_process_timeout is unset or non-positive.
const DefaultDocumentProcessTimeout = 2 * time.Hour

// DocumentProcessTimeout returns the effective document-process task timeout.
// Partial configs (e.g. unit tests) receive the default when unset.
func DocumentProcessTimeout(cfg *Config) time.Duration {
	if cfg != nil && cfg.KnowledgeBase != nil && cfg.KnowledgeBase.DocumentProcessTimeout > 0 {
		return cfg.KnowledgeBase.DocumentProcessTimeout
	}
	return DefaultDocumentProcessTimeout
}

// ImageProcessingConfig 图像处理配置
type ImageProcessingConfig struct {
	EnableMultimodal bool `yaml:"enable_multimodal" json:"enable_multimodal"`
}

// TenantConfig 空间配置
type TenantConfig struct {
	DefaultSessionName        string `yaml:"default_session_name"        json:"default_session_name"`
	DefaultSessionTitle       string `yaml:"default_session_title"       json:"default_session_title"`
	DefaultSessionDescription string `yaml:"default_session_description" json:"default_session_description"`
	// EnableCrossTenantAccess enables cross-tenant access for users with permission
	EnableCrossTenantAccess bool `yaml:"enable_cross_tenant_access" json:"enable_cross_tenant_access"`
	// EnableRBAC turns on tenant-level role enforcement (issue #1303).
	// Pointer so we can distinguish "unset" from "explicit false":
	//   nil           — fall back to the built-in default (true) applied
	//                   by applyAuthAndTenantDefaults.
	//   pointer false — operators opted into the logging-only rollout
	//                   window (set via config.yaml `enable_rbac: false`
	//                   or env `WEKNORA_TENANT_ENABLE_RBAC=false`).
	//   pointer true  — enforcement on (the new default).
	// Read through IsRBACEnforced so callers stay nil-safe.
	EnableRBAC *bool `yaml:"enable_rbac" json:"enable_rbac"`
	// MaxOwnedPerUser caps how many tenants a single non-superuser can
	// create (and Own) via self-service POST /tenants. Counts only Owner
	// memberships so being invited as Admin/Editor/Viewer in another
	// tenant doesn't burn quota. Cross-tenant superusers
	// (CanAccessAllTenants) are exempt.
	//   > 0 — enforce the cap (handler returns 429 when reached).
	//   = 0 — fall back to defaultMaxOwnedTenantsPerUser in the handler.
	//   < 0 — disable the cap entirely (not recommended in shared deployments).
	//
	// Env override: WEKNORA_TENANT_MAX_OWNED_PER_USER (integer). When set
	// and parseable it always wins over config.yaml so operators can
	// loosen / tighten the quota without a redeploy. See
	// applyAuthAndTenantDefaults for the semantics of <0 / 0 / >0.
	MaxOwnedPerUser int `yaml:"max_owned_per_user" json:"max_owned_per_user" mapstructure:"max_owned_per_user"`
	// SelfServiceCreationEnabled controls whether ordinary authenticated
	// users may create a workspace for themselves. Nil preserves the
	// historical default (enabled); cross-tenant superusers are exempt.
	SelfServiceCreationEnabled *bool `yaml:"self_service_creation_enabled" json:"self_service_creation_enabled" mapstructure:"self_service_creation_enabled"`
}

// IsRBACEnforced reports whether tenant-level role enforcement is
// active. Nil receiver or nil EnableRBAC pointer means "operator did
// not opt out", which after applyAuthAndTenantDefaults is the new
// default (true). Callers that need to treat a nil *Config as
// fail-open (legacy behaviour) should keep their own `cfg != nil`
// short-circuit before invoking this helper.
func (t *TenantConfig) IsRBACEnforced() bool {
	if t == nil || t.EnableRBAC == nil {
		return true
	}
	return *t.EnableRBAC
}

// IsSelfServiceCreationEnabled reports whether ordinary users may create
// tenants. Nil keeps the historical behaviour enabled.
func (t *TenantConfig) IsSelfServiceCreationEnabled() bool {
	return t == nil || t.SelfServiceCreationEnabled == nil || *t.SelfServiceCreationEnabled
}

// AuditConfig governs durable audit log behaviour. Writes happen on
// every member-management mutation and on RBAC denials (when
// EnableRBAC is true); the table grows monotonically unless this
// section turns on retention.
type AuditConfig struct {
	// RetentionDays is how many days of audit history to keep. Older
	// rows are deleted by a daily background sweep.
	//   > 0 — purge rows whose created_at < NOW() - retention_days.
	//   = 0 — disable purge entirely (the pre-rollout default).
	//   < 0 — invalid; ValidateConfig rejects it.
	// Default: 90 (set by applyAuditDefaults when the section is omitted).
	RetentionDays int `yaml:"retention_days" json:"retention_days"`
}

// AuthConfig governs the user authentication entry points.
type AuthConfig struct {
	// RegistrationMode controls who may call POST /auth/register.
	//   "self_serve" (default) — anyone may register; a new tenant is
	//                            auto-created and the registrant becomes
	//                            its Owner. Preserves existing behaviour.
	//   "invite_only"          — public registration is rejected; new
	//                            users only enter through the invitation
	//                            flow added in PR 3.
	RegistrationMode string `yaml:"registration_mode" json:"registration_mode"`
	// DefaultTenantMode controls public password-registration provisioning.
	// create_personal preserves the historical one-user-one-workspace default;
	// tenantless creates only the identity and waits for an invitation or an
	// explicit self-service tenant creation.
	DefaultTenantMode      string `yaml:"default_tenant_mode" json:"default_tenant_mode"`
	ComplexPasswordEnabled bool   `yaml:"complex_password_enabled" json:"complex_password_enabled"`
}

// AuthRegistrationMode constants used by handlers and middleware.
const (
	AuthRegistrationModeSelfServe       = "self_serve"
	AuthRegistrationModeInviteOnly      = "invite_only"
	AuthDefaultTenantModeCreatePersonal = "create_personal"
	AuthDefaultTenantModeTenantless     = "tenantless"
)

// IsInviteOnly returns true when registration is gated behind invitations.
// Treats nil receiver and empty/unknown values as "not invite-only" so the
// default keeps current behaviour even if the section is missing from the
// config file.
func (c *AuthConfig) IsInviteOnly() bool {
	if c == nil {
		return false
	}
	return c.RegistrationMode == AuthRegistrationModeInviteOnly
}

type OIDCUserInfoMapping struct {
	Username string `yaml:"username" json:"username"`
	Email    string `yaml:"email"    json:"email"`
}

type OIDCAuthConfig struct {
	Enable                bool                 `yaml:"enable"                 json:"enable"`
	IssuerURL             string               `yaml:"issuer_url"             json:"issuer_url"`
	DiscoveryURL          string               `yaml:"discovery_url"          json:"discovery_url"`
	ProviderDisplayName   string               `yaml:"provider_display_name"  json:"provider_display_name"`
	ClientID              string               `yaml:"client_id"              json:"client_id"`
	ClientSecret          string               `yaml:"client_secret"          json:"-"`
	AuthorizationEndpoint string               `yaml:"authorization_endpoint" json:"authorization_endpoint"`
	TokenEndpoint         string               `yaml:"token_endpoint"         json:"token_endpoint"`
	UserInfoEndpoint      string               `yaml:"user_info_endpoint"     json:"user_info_endpoint"`
	JwksURI               string               `yaml:"jwks_uri"               json:"jwks_uri"`
	Scopes                []string             `yaml:"scopes"                 json:"scopes"`
	UserInfoMapping       *OIDCUserInfoMapping `yaml:"user_info_mapping"      json:"user_info_mapping"`
}

// DirectoryConfig controls the optional LDAP / Active Directory module.
//
// The module is deliberately disabled by default.  A deployment can manage
// the connection in one of two places:
//   - file: YAML / environment variables are authoritative and the admin UI
//     exposes the fields read-only;
//   - database: a system administrator stores the connection through the UI
//     (the bind password is AES-GCM encrypted before it is persisted).
//
// Keeping that choice explicit prevents a value saved in the UI from silently
// overriding a secret mounted by the operator.
type DirectoryConfig struct {
	Enabled             bool                    `yaml:"enabled" json:"enabled"`
	ManagementSource    string                  `yaml:"management_source" json:"management_source"`
	ID                  string                  `yaml:"id" json:"id"`
	ProviderDisplayName string                  `yaml:"provider_display_name" json:"provider_display_name"`
	Servers             []DirectoryServerConfig `yaml:"servers" json:"servers"`
	BindDN              string                  `yaml:"bind_dn" json:"bind_dn"`
	BindPassword        string                  `yaml:"bind_password" json:"-"`
	BindPasswordFile    string                  `yaml:"bind_password_file" json:"-"`
	BaseDN              string                  `yaml:"base_dn" json:"base_dn"`
	UserBaseDN          string                  `yaml:"user_base_dn" json:"user_base_dn"`
	GroupBaseDN         string                  `yaml:"group_base_dn" json:"group_base_dn"`
	UserFilter          string                  `yaml:"user_filter" json:"user_filter"`
	GroupFilter         string                  `yaml:"group_filter" json:"group_filter"`
	LoginFilter         string                  `yaml:"login_filter" json:"login_filter"`
	AllowedLoginFilter  string                  `yaml:"allowed_login_filter" json:"allowed_login_filter"`
	CAFile              string                  `yaml:"ca_file" json:"ca_file"`
	ConnectTimeout      time.Duration           `yaml:"connect_timeout" json:"connect_timeout"`
	QueryTimeout        time.Duration           `yaml:"query_timeout" json:"query_timeout"`
	PageSize            uint32                  `yaml:"page_size" json:"page_size"`
	ResultLimit         int                     `yaml:"result_limit" json:"result_limit"`
	SyncInterval        time.Duration           `yaml:"sync_interval" json:"sync_interval"`
	StaleAfter          time.Duration           `yaml:"stale_after" json:"stale_after"`
	// ConfiguredFromEnvironment is computed at load time and is not accepted
	// from YAML.  The management API uses it to explain why fields are locked.
	ConfiguredFromEnvironment bool `yaml:"-" json:"configured_from_environment"`
}

type DirectoryServerConfig struct {
	URL        string `yaml:"url" json:"url"`
	TLSMode    string `yaml:"tls_mode" json:"tls_mode"`
	ServerName string `yaml:"server_name" json:"server_name"`
}

const (
	DirectoryManagementFile     = "file"
	DirectoryManagementDatabase = "database"
	DirectoryTLSLDAPS           = "ldaps"
	DirectoryTLSStartTLS        = "starttls"
)

// PromptTemplateI18n holds localized name and description for a prompt template.
type PromptTemplateI18n struct {
	Name        string `yaml:"name"        json:"name"`
	Description string `yaml:"description" json:"description"`
}

// PromptTemplate 提示词模板
//
// 字段设计：每个模板最多由两部分组成 —— 系统侧 (content) 和用户侧 (user)。
//   - content: 主要内容 / 系统 Prompt（所有模板都使用此字段）
//   - user:    用户侧 Prompt（仅在需要 system+user 配对的模板中使用，如 rewrite、keywords_extraction）
//   - i18n:    多语言 name/description，键为 locale（如 "zh-CN"、"en-US"、"ko-KR"），后端根据请求语言替换 Name/Description 再返回
type PromptTemplate struct {
	ID               string                        `yaml:"id"                 json:"id"`
	Name             string                        `yaml:"name"               json:"name"`
	Description      string                        `yaml:"description"        json:"description"`
	Content          string                        `yaml:"content"            json:"content"`
	User             string                        `yaml:"user"               json:"user,omitempty"`
	HasKnowledgeBase bool                          `yaml:"has_knowledge_base" json:"has_knowledge_base,omitempty"`
	HasWebSearch     bool                          `yaml:"has_web_search"     json:"has_web_search,omitempty"`
	Default          bool                          `yaml:"default"            json:"default,omitempty"`
	Mode             string                        `yaml:"mode"               json:"mode,omitempty"`
	I18n             map[string]PromptTemplateI18n `yaml:"i18n"               json:"-"`
}

// PromptTemplatesConfig 提示词模板配置
//
// 每种 Prompt 类型对应一个 YAML 文件，所有模板都在同一个字段（文件）中管理。
// 每个模板使用 content (system prompt) + user (user prompt) 两个字段。
type PromptTemplatesConfig struct {
	SystemPrompt    []PromptTemplate `yaml:"system_prompt"    json:"system_prompt"`
	ContextTemplate []PromptTemplate `yaml:"context_template" json:"context_template"`
	// Rewrite 合并了前端可选模板和运行时默认模板，每个模板同时包含 content + user
	Rewrite []PromptTemplate `yaml:"rewrite" json:"rewrite"`
	// Fallback 合并了固定回复模板和模型兜底 prompt（通过 mode:"model" 区分）
	Fallback []PromptTemplate `yaml:"fallback" json:"fallback"`

	GenerateSessionTitle []PromptTemplate `yaml:"generate_session_title" json:"generate_session_title,omitempty"`
	GenerateSummary      []PromptTemplate `yaml:"generate_summary"       json:"generate_summary,omitempty"`
	// GenerateKBDescription writes the knowledge-base gist from the document profile aggregate.
	GenerateKBDescription []PromptTemplate `yaml:"generate_kb_description" json:"generate_kb_description,omitempty"` //nolint:lll // one-line struct tag
	KeywordsExtraction    []PromptTemplate `yaml:"keywords_extraction"    json:"keywords_extraction,omitempty"`
	AgentSystemPrompt     []PromptTemplate `yaml:"agent_system_prompt"    json:"agent_system_prompt,omitempty"`
	GraphExtraction       []PromptTemplate `yaml:"graph_extraction"       json:"graph_extraction,omitempty"`
	GenerateQuestions     []PromptTemplate `yaml:"generate_questions"     json:"generate_questions,omitempty"`
	// IntentPrompts holds per-intent system prompt overrides (template ID = intent value).
	IntentPrompts []PromptTemplate `yaml:"intent_prompts" json:"intent_prompts,omitempty"`
}

// DefaultTemplate returns the first template marked as default in the list,
// or the first template if none is marked, or nil if the list is empty.
func DefaultTemplate(templates []PromptTemplate) *PromptTemplate {
	for i := range templates {
		if templates[i].Default {
			return &templates[i]
		}
	}
	if len(templates) > 0 {
		return &templates[0]
	}
	return nil
}

// DefaultTemplateByMode returns the default template filtered by mode.
func DefaultTemplateByMode(templates []PromptTemplate, mode string) *PromptTemplate {
	for i := range templates {
		if templates[i].Mode == mode && templates[i].Default {
			return &templates[i]
		}
	}
	for i := range templates {
		if templates[i].Mode == mode {
			return &templates[i]
		}
	}
	return DefaultTemplate(templates)
}

// LocalizeTemplates returns a deep copy of the template list with Name and
// Description replaced according to the given locale.  Fallback chain:
//
//	locale → primary language (e.g. "zh" from "zh-CN") → original Name/Description.
//
// The returned slice is safe to serialise directly; it never mutates the original.
func LocalizeTemplates(templates []PromptTemplate, locale string) []PromptTemplate {
	if len(templates) == 0 {
		return templates
	}
	out := make([]PromptTemplate, len(templates))
	copy(out, templates)
	for i := range out {
		if len(out[i].I18n) == 0 {
			continue
		}
		// Try exact match first (e.g. "zh-CN"), then primary subtag (e.g. "zh")
		l10n, ok := out[i].I18n[locale]
		if !ok {
			if idx := strings.IndexByte(locale, '-'); idx > 0 {
				l10n, ok = out[i].I18n[locale[:idx]]
			}
		}
		if !ok {
			continue
		}
		if l10n.Name != "" {
			out[i].Name = l10n.Name
		}
		if l10n.Description != "" {
			out[i].Description = l10n.Description
		}
	}
	return out
}

// ModelConfig 模型配置
type ModelConfig struct {
	Type       string                 `yaml:"type"       json:"type"`
	Source     string                 `yaml:"source"     json:"source"`
	ModelName  string                 `yaml:"model_name" json:"model_name"`
	Parameters map[string]interface{} `yaml:"parameters" json:"parameters"`
}

// StreamManagerConfig 流管理器配置
type StreamManagerConfig struct {
	Type           string        `yaml:"type"            json:"type"`            // 类型: "memory" 或 "redis"
	Redis          RedisConfig   `yaml:"redis"           json:"redis"`           // Redis配置
	CleanupTimeout time.Duration `yaml:"cleanup_timeout" json:"cleanup_timeout"` // 清理超时，单位秒
}

// RedisConfig Redis配置
type RedisConfig struct {
	Address  string        `yaml:"address"  json:"address"`  // Redis地址
	Username string        `yaml:"username" json:"username"` // Redis用户名
	Password string        `yaml:"password" json:"password"` // Redis密码
	DB       int           `yaml:"db"       json:"db"`       // Redis数据库
	Prefix   string        `yaml:"prefix"   json:"prefix"`   // 键前缀
	TTL      time.Duration `yaml:"ttl"      json:"ttl"`      // 过期时间(小时)
}

// ExtractManagerConfig 抽取管理器配置
type ExtractManagerConfig struct {
	ExtractGraph  *types.PromptTemplateStructured `yaml:"extract_graph"  json:"extract_graph"`
	ExtractEntity *types.PromptTemplateStructured `yaml:"extract_entity" json:"extract_entity"`
	FabriText     *FebriText                      `yaml:"fabri_text"     json:"fabri_text"`
}

type FebriText struct {
	WithTag   string `yaml:"with_tag"    json:"with_tag"`
	WithNoTag string `yaml:"with_no_tag" json:"with_no_tag"`
}

// resolvedConfigDir holds the directory of the loaded config file. Populated by
// LoadConfig and read by ConfigDir(); empty until LoadConfig has run.
var resolvedConfigDir string

// ConfigDir returns the directory containing the loaded config.yaml. Other
// startup code (e.g. builtin model loader) uses this to locate sibling config
// files like builtin_models.yaml without re-implementing viper search rules.
// Falls back to "./config" when LoadConfig has not been called yet.
func ConfigDir() string {
	if resolvedConfigDir != "" {
		return resolvedConfigDir
	}
	if f := viper.ConfigFileUsed(); f != "" {
		return filepath.Dir(f)
	}
	return "./config"
}

// LoadConfig 从配置文件加载配置
func LoadConfig() (*Config, error) {
	// 设置配置文件名和路径
	viper.SetConfigName("config")         // 配置文件名称(不带扩展名)
	viper.SetConfigType("yaml")           // 配置文件类型
	viper.AddConfigPath(".")              // 当前目录
	viper.AddConfigPath("./config")       // config子目录
	viper.AddConfigPath("$HOME/.appname") // 用户目录
	viper.AddConfigPath("/etc/appname/")  // etc目录

	// 启用环境变量替换
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// 读取配置文件
	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	// 替换配置中的环境变量引用
	configFileContent, err := os.ReadFile(viper.ConfigFileUsed())
	if err != nil {
		return nil, fmt.Errorf("error reading config file content: %w", err)
	}

	// 替换${ENV_VAR}格式的环境变量引用
	re := regexp.MustCompile(`\${([^}]+)}`)
	result := re.ReplaceAllStringFunc(string(configFileContent), func(match string) string {
		// 提取环境变量名称（去掉${}部分）
		envVar := match[2 : len(match)-1]
		// 获取环境变量值，如果不存在则保持原样
		if value := os.Getenv(envVar); value != "" {
			return value
		}
		return match
	})

	// 使用处理后的配置内容
	viper.ReadConfig(strings.NewReader(result))

	// 解析配置到结构体
	var cfg Config
	if err := viper.Unmarshal(&cfg, func(dc *mapstructure.DecoderConfig) {
		dc.TagName = "yaml"
	}); err != nil {
		return nil, fmt.Errorf("unable to decode config into struct: %w", err)
	}
	fmt.Printf("Using configuration file: %s\n", viper.ConfigFileUsed())

	// 加载提示词模板（从目录或配置文件）
	configDir := filepath.Dir(viper.ConfigFileUsed())
	resolvedConfigDir = configDir
	promptTemplates, err := loadPromptTemplates(configDir)
	if err != nil {
		fmt.Printf("Warning: failed to load prompt templates from directory: %v\n", err)
		// 如果目录加载失败，使用配置文件中的模板（如果有）
	} else if promptTemplates != nil {
		cfg.PromptTemplates = promptTemplates
	}

	// Back-fill conversation config from prompt templates defaults
	// (so config.yaml can omit large prompt blocks and rely on template files)
	if cfg.PromptTemplates != nil && cfg.Conversation != nil {
		backfillConversationDefaults(&cfg)
	}

	// Load built-in agent definitions (i18n-aware) from builtin_agents.yaml
	if err := types.LoadBuiltinAgentsConfig(configDir); err != nil {
		fmt.Printf("Warning: failed to load builtin agents config: %v\n", err)
	}

	// Load smart-reasoning agent type presets (rag-qa / wiki-qa / hybrid / custom).
	if err := types.LoadAgentTypePresetsConfig(configDir); err != nil {
		fmt.Printf("Warning: failed to load agent type presets: %v\n", err)
	}

	// Resolve prompt template ID references in builtin agent configs
	// (e.g. system_prompt_id -> actual content from agent_system_prompt.yaml)
	if cfg.PromptTemplates != nil {
		resolveBuiltinAgentPromptIDs(cfg.PromptTemplates)
		// Validate that every preset references an existing prompt template.
		types.ResolveAgentTypePresetPromptRefs(func(id string) string {
			if t := FindTemplateByID(cfg.PromptTemplates, id); t != nil {
				return t.Content
			}
			return ""
		})
	}

	// Validate configuration values
	applyOIDCEnvOverrides(&cfg)
	if err := applyDirectoryEnvOverrides(&cfg); err != nil {
		return nil, err
	}
	applyAgentEnvOverrides(&cfg)
	applyKnowledgeBaseEnvOverrides(&cfg)
	applyAuthAndTenantDefaults(&cfg)
	applyAuditDefaults(&cfg)

	if err := ValidateConfig(&cfg); err != nil {
		return nil, err
	}

	// Surface RBAC enforcement state at startup. air's hot-reload only
	// rebuilds the binary on Go-source changes; it does NOT re-source
	// .env, so a `WEKNORA_TENANT_ENABLE_RBAC=true` flip while the dev
	// loop is already running silently has no effect until the dev
	// script restarts. Logging this once at startup makes the
	// "I edited .env but the gates still aren't firing" trap obvious
	// from the first console line. Printf rather than logger because
	// LoadConfig runs before the logger sink is wired in the dig graph.
	rbacOn := cfg.Tenant.IsRBACEnforced()
	xtAccess := cfg.Tenant != nil && cfg.Tenant.EnableCrossTenantAccess
	fmt.Printf(
		"[config] tenant RBAC enforcement: enable_rbac=%v cross_tenant_access=%v "+
			"(env: WEKNORA_TENANT_ENABLE_RBAC=%q WEKNORA_TENANT_ENABLE_CROSS_TENANT_ACCESS=%q)\n",
		rbacOn, xtAccess,
		os.Getenv("WEKNORA_TENANT_ENABLE_RBAC"),
		os.Getenv("WEKNORA_TENANT_ENABLE_CROSS_TENANT_ACCESS"),
	)

	return &cfg, nil
}

// ValidateConfig performs basic validation of the loaded configuration.
// It checks for obviously invalid or missing values that would cause runtime failures.
func ValidateConfig(cfg *Config) error {
	var errs []string

	if cfg.OIDCAuth != nil && cfg.OIDCAuth.Enable {
		if strings.TrimSpace(cfg.OIDCAuth.ClientID) == "" {
			errs = append(errs, "oidc_auth.client_id is required when OIDC is enabled")
		}
		if strings.TrimSpace(cfg.OIDCAuth.ClientSecret) == "" {
			errs = append(errs, "oidc_auth.client_secret is required when OIDC is enabled")
		}
		if strings.TrimSpace(cfg.OIDCAuth.DiscoveryURL) == "" &&
			(strings.TrimSpace(cfg.OIDCAuth.AuthorizationEndpoint) == "" || strings.TrimSpace(cfg.OIDCAuth.TokenEndpoint) == "") {
			errs = append(errs, "oidc_auth.discovery_url or both oidc_auth.authorization_endpoint and oidc_auth.token_endpoint are required when OIDC is enabled")
		}
	}

	if cfg.Directory != nil && cfg.Directory.Enabled {
		d := cfg.Directory
		switch d.ManagementSource {
		case DirectoryManagementFile:
			if len(d.Servers) == 0 {
				errs = append(errs, "directory.servers is required when the file-managed directory module is enabled")
			}
			if strings.TrimSpace(d.BindDN) == "" {
				errs = append(errs, "directory.bind_dn is required when the file-managed directory module is enabled")
			}
			if d.BindPassword == "" {
				errs = append(errs, "directory bind password is required when the file-managed directory module is enabled")
			}
			if strings.TrimSpace(d.BaseDN) == "" {
				errs = append(errs, "directory.base_dn is required when the file-managed directory module is enabled")
			}
			if !strings.Contains(d.LoginFilter, "{login}") {
				errs = append(errs, "directory.login_filter must contain {login}")
			} else {
				probe := strings.ReplaceAll(d.LoginFilter, "{login}", ldap.EscapeFilter("login-probe"))
				if _, err := ldap.CompileFilter(probe); err != nil {
					errs = append(errs, fmt.Sprintf("directory.login_filter is invalid: %v", err))
				}
			}
		case DirectoryManagementDatabase:
			// The persisted connection is validated when it is saved and loaded.
		default:
			errs = append(errs, fmt.Sprintf("directory.management_source must be %q or %q, got %q",
				DirectoryManagementFile, DirectoryManagementDatabase, d.ManagementSource))
		}
		for i, server := range d.Servers {
			urlValue := strings.TrimSpace(server.URL)
			switch server.TLSMode {
			case DirectoryTLSLDAPS:
				if !strings.HasPrefix(strings.ToLower(urlValue), "ldaps://") {
					errs = append(errs, fmt.Sprintf("directory.servers[%d].url must use ldaps:// for tls_mode=ldaps", i))
				}
			case DirectoryTLSStartTLS:
				if !strings.HasPrefix(strings.ToLower(urlValue), "ldap://") {
					errs = append(errs, fmt.Sprintf("directory.servers[%d].url must use ldap:// for tls_mode=starttls", i))
				}
			default:
				errs = append(errs, fmt.Sprintf("directory.servers[%d].tls_mode must be ldaps or starttls", i))
			}
		}
		if d.ConnectTimeout <= 0 || d.QueryTimeout <= 0 {
			errs = append(errs, "directory connect_timeout and query_timeout must be positive")
		}
		if d.PageSize == 0 || d.ResultLimit <= 0 || int(d.PageSize) > d.ResultLimit {
			errs = append(errs, "directory page_size must be positive and no greater than result_limit")
		}
		if d.SyncInterval <= 0 || d.StaleAfter <= 0 {
			errs = append(errs, "directory sync_interval and stale_after must be positive")
		}
	}

	if cfg.Auth != nil {
		mode := strings.TrimSpace(cfg.Auth.RegistrationMode)
		if mode != "" && mode != AuthRegistrationModeSelfServe && mode != AuthRegistrationModeInviteOnly {
			errs = append(errs, fmt.Sprintf("auth.registration_mode must be %q or %q, got %q",
				AuthRegistrationModeSelfServe, AuthRegistrationModeInviteOnly, mode))
		}

		tenantMode := strings.TrimSpace(cfg.Auth.DefaultTenantMode)
		if tenantMode != "" && tenantMode != AuthDefaultTenantModeCreatePersonal && tenantMode != AuthDefaultTenantModeTenantless {
			errs = append(errs, fmt.Sprintf("auth.default_tenant_mode must be %q or %q, got %q",
				AuthDefaultTenantModeCreatePersonal, AuthDefaultTenantModeTenantless, tenantMode))
		}
	}

	if cfg.Audit != nil && cfg.Audit.RetentionDays < 0 {
		errs = append(errs, fmt.Sprintf("audit.retention_days must be >= 0 (got %d); use 0 to disable purge",
			cfg.Audit.RetentionDays))
	}

	if cfg.Conversation != nil {
		if cfg.Conversation.EmbeddingTopK < 0 {
			errs = append(errs, "conversation.embedding_top_k must be >= 0")
		}
		if cfg.Conversation.RerankTopK < 0 {
			errs = append(errs, "conversation.rerank_top_k must be >= 0")
		}
		if cfg.Conversation.VectorThreshold < 0 || cfg.Conversation.VectorThreshold > 1 {
			errs = append(errs, "conversation.vector_threshold must be between 0 and 1")
		}
		if cfg.Conversation.RerankThreshold < -10 || cfg.Conversation.RerankThreshold > 10 {
			errs = append(errs, "conversation.rerank_threshold must be between -10 and 10")
		}
	}

	if cfg.KnowledgeBase != nil {
		if cfg.KnowledgeBase.ChunkSize <= 0 {
			errs = append(errs, "knowledge_base.chunk_size must be > 0")
		}
		if cfg.KnowledgeBase.ChunkOverlap < 0 {
			errs = append(errs, "knowledge_base.chunk_overlap must be >= 0")
		}
		if cfg.KnowledgeBase.ChunkOverlap >= cfg.KnowledgeBase.ChunkSize {
			errs = append(errs, "knowledge_base.chunk_overlap must be less than chunk_size")
		}
	}

	if cfg.Server != nil {
		if cfg.Server.Port <= 0 || cfg.Server.Port > 65535 {
			errs = append(errs, "server.port must be between 1 and 65535")
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("config validation errors: %s", strings.Join(errs, "; "))
	}
	return nil
}

func applyOIDCEnvOverrides(cfg *Config) {
	if cfg.OIDCAuth == nil {
		cfg.OIDCAuth = &OIDCAuthConfig{}
	}
	if cfg.OIDCAuth.UserInfoMapping == nil {
		cfg.OIDCAuth.UserInfoMapping = &OIDCUserInfoMapping{}
	}

	if value := strings.TrimSpace(os.Getenv("OIDC_AUTH_ENABLE")); value != "" {
		cfg.OIDCAuth.Enable = strings.EqualFold(value, "true")
	}
	if value := strings.TrimSpace(os.Getenv("OIDC_AUTH_ISSUER_URL")); value != "" {
		cfg.OIDCAuth.IssuerURL = value
	}
	if value := strings.TrimSpace(os.Getenv("OIDC_AUTH_DISCOVERY_URL")); value != "" {
		cfg.OIDCAuth.DiscoveryURL = value
	}
	if value := strings.TrimSpace(os.Getenv("OIDC_AUTH_PROVIDER_DISPLAY_NAME")); value != "" {
		cfg.OIDCAuth.ProviderDisplayName = value
	}
	if value := strings.TrimSpace(os.Getenv("OIDC_AUTH_CLIENT_ID")); value != "" {
		cfg.OIDCAuth.ClientID = value
	}
	if value := strings.TrimSpace(os.Getenv("OIDC_AUTH_CLIENT_SECRET")); value != "" {
		cfg.OIDCAuth.ClientSecret = value
	}
	if value := strings.TrimSpace(os.Getenv("OIDC_AUTH_AUTHORIZATION_ENDPOINT")); value != "" {
		cfg.OIDCAuth.AuthorizationEndpoint = value
	}
	if value := strings.TrimSpace(os.Getenv("OIDC_AUTH_TOKEN_ENDPOINT")); value != "" {
		cfg.OIDCAuth.TokenEndpoint = value
	}
	if value := strings.TrimSpace(os.Getenv("OIDC_AUTH_USER_INFO_ENDPOINT")); value != "" {
		cfg.OIDCAuth.UserInfoEndpoint = value
	}
	if value := strings.TrimSpace(os.Getenv("OIDC_AUTH_JWKS_URI")); value != "" {
		cfg.OIDCAuth.JwksURI = value
	}
	if value := strings.TrimSpace(os.Getenv("OIDC_AUTH_SCOPES")); value != "" {
		cfg.OIDCAuth.Scopes = strings.Fields(strings.ReplaceAll(value, ",", " "))
	}
	if value := strings.TrimSpace(os.Getenv("OIDC_USER_INFO_MAPPING_USER_NAME")); value != "" {
		cfg.OIDCAuth.UserInfoMapping.Username = value
	}
	if value := strings.TrimSpace(os.Getenv("OIDC_USER_INFO_MAPPING_EMAIL")); value != "" {
		cfg.OIDCAuth.UserInfoMapping.Email = value
	}

	if cfg.OIDCAuth.ProviderDisplayName == "" {
		cfg.OIDCAuth.ProviderDisplayName = "OIDC"
	}
	if len(cfg.OIDCAuth.Scopes) == 0 {
		cfg.OIDCAuth.Scopes = []string{"openid", "profile", "email"}
	}
	if cfg.OIDCAuth.UserInfoMapping.Username == "" {
		cfg.OIDCAuth.UserInfoMapping.Username = "name"
	}
	if cfg.OIDCAuth.UserInfoMapping.Email == "" {
		cfg.OIDCAuth.UserInfoMapping.Email = "email"
	}
	if cfg.OIDCAuth.DiscoveryURL == "" && cfg.OIDCAuth.IssuerURL != "" {
		cfg.OIDCAuth.DiscoveryURL = strings.TrimRight(cfg.OIDCAuth.IssuerURL, "/") + "/.well-known/openid-configuration"
	}
}

// applyDirectoryEnvOverrides resolves the deploy-time LDAP/AD configuration.
// Secret-file support is intentionally explicit instead of a generic Viper
// hook: an accidentally unreadable password file must fail startup, not turn
// into an empty-password bind or fall back to a plaintext value.
func applyDirectoryEnvOverrides(cfg *Config) error {
	if cfg.Directory == nil {
		cfg.Directory = &DirectoryConfig{}
	}
	d := cfg.Directory
	if strings.TrimSpace(d.ManagementSource) == "" {
		d.ManagementSource = DirectoryManagementFile
	}
	if strings.TrimSpace(d.ID) == "" {
		d.ID = "default-ad"
	}
	if strings.TrimSpace(d.ProviderDisplayName) == "" {
		d.ProviderDisplayName = "Corporate directory"
	}

	envSeen := false
	setString := func(name string, dst *string) {
		if value, ok := os.LookupEnv(name); ok {
			envSeen = true
			*dst = strings.TrimSpace(value)
		}
	}
	if value, ok := os.LookupEnv("LDAP_ENABLED"); ok {
		envSeen = true
		parsed, err := strconv.ParseBool(strings.TrimSpace(value))
		if err != nil {
			return fmt.Errorf("LDAP_ENABLED must be a boolean: %w", err)
		}
		d.Enabled = parsed
	}
	setString("LDAP_CONFIG_SOURCE", &d.ManagementSource)
	setString("LDAP_DIRECTORY_ID", &d.ID)
	setString("LDAP_PROVIDER_DISPLAY_NAME", &d.ProviderDisplayName)
	setString("LDAP_BIND_DN", &d.BindDN)
	// LDAP passwords are opaque octet strings. Preserve leading/trailing
	// spaces from the environment instead of applying the whitespace
	// normalization used for addresses, DNs, and filters.
	if value, ok := os.LookupEnv("LDAP_BIND_PASSWORD"); ok {
		envSeen = true
		d.BindPassword = value
	}
	setString("LDAP_BIND_PASSWORD_FILE", &d.BindPasswordFile)
	setString("LDAP_BASE_DN", &d.BaseDN)
	setString("LDAP_USER_BASE_DN", &d.UserBaseDN)
	setString("LDAP_GROUP_BASE_DN", &d.GroupBaseDN)
	setString("LDAP_USER_FILTER", &d.UserFilter)
	setString("LDAP_GROUP_FILTER", &d.GroupFilter)
	setString("LDAP_LOGIN_FILTER", &d.LoginFilter)
	setString("LDAP_ALLOWED_LOGIN_FILTER", &d.AllowedLoginFilter)
	setString("LDAP_CA_FILE", &d.CAFile)

	if value, ok := os.LookupEnv("LDAP_URLS"); ok {
		envSeen = true
		urls := splitNonEmpty(value)
		tlsMode := strings.TrimSpace(os.Getenv("LDAP_TLS_MODE"))
		if tlsMode == "" {
			tlsMode = DirectoryTLSLDAPS
		}
		names := splitNonEmpty(os.Getenv("LDAP_SERVER_NAMES"))
		d.Servers = make([]DirectoryServerConfig, 0, len(urls))
		for i, urlValue := range urls {
			serverName := ""
			if i < len(names) {
				serverName = names[i]
			}
			d.Servers = append(d.Servers, DirectoryServerConfig{
				URL: urlValue, TLSMode: strings.ToLower(tlsMode), ServerName: serverName,
			})
		}
	} else {
		if value, ok := os.LookupEnv("LDAP_TLS_MODE"); ok {
			envSeen = true
			for i := range d.Servers {
				d.Servers[i].TLSMode = strings.ToLower(strings.TrimSpace(value))
			}
		}
	}

	parseDurationEnv := func(name string, dst *time.Duration) error {
		value, ok := os.LookupEnv(name)
		if !ok {
			return nil
		}
		envSeen = true
		parsed, err := time.ParseDuration(strings.TrimSpace(value))
		if err != nil || parsed <= 0 {
			return fmt.Errorf("%s must be a positive Go duration", name)
		}
		*dst = parsed
		return nil
	}
	if err := parseDurationEnv("LDAP_CONNECT_TIMEOUT", &d.ConnectTimeout); err != nil {
		return err
	}
	if err := parseDurationEnv("LDAP_QUERY_TIMEOUT", &d.QueryTimeout); err != nil {
		return err
	}
	if err := parseDurationEnv("LDAP_SYNC_INTERVAL", &d.SyncInterval); err != nil {
		return err
	}
	if err := parseDurationEnv("LDAP_STALE_AFTER", &d.StaleAfter); err != nil {
		return err
	}
	parseIntEnv := func(name string, apply func(int) error) error {
		value, ok := os.LookupEnv(name)
		if !ok {
			return nil
		}
		envSeen = true
		parsed, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			return fmt.Errorf("%s must be an integer: %w", name, err)
		}
		return apply(parsed)
	}
	if err := parseIntEnv("LDAP_PAGE_SIZE", func(value int) error {
		if value <= 0 {
			return errors.New("LDAP_PAGE_SIZE must be positive")
		}
		d.PageSize = uint32(value)
		return nil
	}); err != nil {
		return err
	}
	if err := parseIntEnv("LDAP_RESULT_LIMIT", func(value int) error {
		if value <= 0 {
			return errors.New("LDAP_RESULT_LIMIT must be positive")
		}
		d.ResultLimit = value
		return nil
	}); err != nil {
		return err
	}

	if d.ConnectTimeout == 0 {
		d.ConnectTimeout = 5 * time.Second
	}
	if d.QueryTimeout == 0 {
		d.QueryTimeout = 10 * time.Second
	}
	if d.PageSize == 0 {
		d.PageSize = 500
	}
	if d.ResultLimit == 0 {
		d.ResultLimit = 10000
	}
	if d.SyncInterval == 0 {
		d.SyncInterval = 5 * time.Minute
	}
	if d.StaleAfter == 0 {
		d.StaleAfter = 15 * time.Minute
	}
	if d.UserBaseDN == "" {
		d.UserBaseDN = d.BaseDN
	}
	if d.GroupBaseDN == "" {
		d.GroupBaseDN = d.BaseDN
	}
	if d.UserFilter == "" {
		d.UserFilter = "(&(objectCategory=person)(objectClass=user))"
	}
	if d.GroupFilter == "" {
		d.GroupFilter = "(objectCategory=group)"
	}
	if d.LoginFilter == "" {
		d.LoginFilter = "(&(objectCategory=person)(objectClass=user)(|(sAMAccountName={login})(userPrincipalName={login})))"
	}
	for i := range d.Servers {
		if strings.TrimSpace(d.Servers[i].TLSMode) == "" {
			if strings.HasPrefix(strings.ToLower(strings.TrimSpace(d.Servers[i].URL)), "ldap://") {
				d.Servers[i].TLSMode = DirectoryTLSStartTLS
			} else {
				d.Servers[i].TLSMode = DirectoryTLSLDAPS
			}
		}
	}

	if d.BindPassword != "" && d.BindPasswordFile != "" {
		return errors.New("configure only one of directory.bind_password/LDAP_BIND_PASSWORD and directory.bind_password_file/LDAP_BIND_PASSWORD_FILE")
	}
	if d.Enabled && d.ManagementSource == DirectoryManagementFile && d.BindPasswordFile != "" {
		secret, err := readDirectorySecretFile(d.BindPasswordFile)
		if err != nil {
			return fmt.Errorf("read LDAP bind password file: %w", err)
		}
		d.BindPassword = secret
	}
	d.ConfiguredFromEnvironment = envSeen
	return nil
}

func splitNonEmpty(value string) []string {
	parts := strings.FieldsFunc(value, func(r rune) bool { return r == ',' || r == ';' || r == '\n' })
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			result = append(result, value)
		}
	}
	return result
}

func readDirectorySecretFile(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Size() > 1024*1024 {
		return "", errors.New("secret file must be a regular file no larger than 1 MiB")
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	secret := strings.TrimRight(string(contents), "\r\n")
	if secret == "" {
		return "", errors.New("secret file is empty")
	}
	return secret, nil
}

func applyKnowledgeBaseEnvOverrides(cfg *Config) {
	if cfg.KnowledgeBase == nil {
		cfg.KnowledgeBase = &KnowledgeBaseConfig{}
	}
	if cfg.KnowledgeBase.DocumentProcessTimeout <= 0 {
		cfg.KnowledgeBase.DocumentProcessTimeout = DefaultDocumentProcessTimeout
	}
	if value := strings.TrimSpace(os.Getenv("WEKNORA_DOCUMENT_PROCESS_TIMEOUT")); value != "" {
		if d, err := time.ParseDuration(value); err == nil {
			cfg.KnowledgeBase.DocumentProcessTimeout = d
		}
	}
	if cfg.KnowledgeBase.DocReaderCallTimeout <= 0 {
		cfg.KnowledgeBase.DocReaderCallTimeout = 30 * time.Minute
	}
	if value := strings.TrimSpace(os.Getenv("WEKNORA_DOCREADER_CALL_TIMEOUT")); value != "" {
		if d, err := time.ParseDuration(value); err == nil && d > 0 {
			cfg.KnowledgeBase.DocReaderCallTimeout = d
		}
	}
}

func applyAgentEnvOverrides(cfg *Config) {
	if cfg.Agent == nil {
		cfg.Agent = &AgentConfig{}
	}
	if value := strings.TrimSpace(os.Getenv("WEKNORA_AGENT_LLM_TIMEOUT")); value != "" {
		if timeout, err := time.ParseDuration(value); err == nil {
			cfg.Agent.LLMCallTimeout = int(timeout.Seconds())
		} else if sec, err := time.ParseDuration(value + "s"); err == nil {
			// Handle case where user just provides a number like "300"
			cfg.Agent.LLMCallTimeout = int(sec.Seconds())
		}
	}
	// MCP tool human-approval wait timeout (issue #1173). Accepts Go duration
	// (e.g. "10m", "30s") or a bare number interpreted as seconds.
	if value := strings.TrimSpace(os.Getenv("WEKNORA_AGENT_TOOL_APPROVAL_TIMEOUT")); value != "" {
		if d, err := time.ParseDuration(value); err == nil {
			cfg.Agent.ToolApprovalTimeoutSeconds = int(d.Seconds())
		} else if d, err := time.ParseDuration(value + "s"); err == nil {
			cfg.Agent.ToolApprovalTimeoutSeconds = int(d.Seconds())
		}
	}
}

// applyAuthAndTenantDefaults fills in defaults for the Auth and Tenant
// config sections and applies env-var overrides that operators commonly use
// to enable RBAC or switch registration mode without editing config.yaml.
//
// Defaults:
//   - auth.registration_mode  -> "self_serve" (preserves pre-RBAC behaviour)
//   - auth.default_tenant_mode -> "create_personal" (preserves the
//     historical registration behaviour)
//   - tenant.enable_rbac      -> true (enforce role checks unless an
//     operator explicitly opts into the logging-only rollout window via
//     config.yaml `enable_rbac: false` or `WEKNORA_TENANT_ENABLE_RBAC=false`).
//   - tenant.self_service_creation_enabled -> true (preserves ordinary
//     authenticated users' ability to create workspaces).
//
// Env overrides (when set and non-empty):
//   - WEKNORA_AUTH_DEFAULT_TENANT_MODE ("create_personal"/"tenantless")
//   - WEKNORA_AUTH_COMPLEX_PASSWORD_ENABLED (boolean)
//   - WEKNORA_TENANT_SELF_SERVICE_CREATION_ENABLED (boolean)
//   - WEKNORA_TENANT_ENABLE_RBAC      ("true"/"false", case-insensitive)
//   - WEKNORA_TENANT_ENABLE_CROSS_TENANT_ACCESS ("true"/"false", case-insensitive).
//     Read explicitly because viper.AutomaticEnv has no SetEnvPrefix, so the
//     WEKNORA_-prefixed var is not bound to the nested struct automatically.
//   - WEKNORA_TENANT_MAX_OWNED_PER_USER (integer; <0 disables the cap,
//     0 falls back to the handler default, >0 enforces that exact cap).
//     Unparseable / empty values are ignored so a stale shell variable
//     can't silently disable the quota for a future deployment.
//
// Note: auth.registration_mode has no dedicated env override. The
// long-standing DISABLE_REGISTRATION=true env var is the single env-layer
// knob and, when set, coerces registration_mode to invite_only here. That
// way both the API gate (handler) and the /auth/config-driven UI gate
// (frontend hides the register entry) stay consistent — without needing
// two parallel env vars.
func applyAuthAndTenantDefaults(cfg *Config) {
	if cfg.Auth == nil {
		cfg.Auth = &AuthConfig{}
	}
	if cfg.Tenant == nil {
		cfg.Tenant = &TenantConfig{}
	}

	if legacy := strings.TrimSpace(os.Getenv("DISABLE_REGISTRATION")); strings.EqualFold(legacy, "true") {
		prev := strings.TrimSpace(cfg.Auth.RegistrationMode)
		cfg.Auth.RegistrationMode = AuthRegistrationModeInviteOnly
		if prev != "" && prev != AuthRegistrationModeInviteOnly {
			fmt.Printf(
				"[config] DISABLE_REGISTRATION=true overrides auth.registration_mode=%q -> %q\n",
				prev, AuthRegistrationModeInviteOnly,
			)
		}
	}

	if strings.TrimSpace(cfg.Auth.RegistrationMode) == "" {
		cfg.Auth.RegistrationMode = AuthRegistrationModeSelfServe
	}

	if value := strings.TrimSpace(os.Getenv("WEKNORA_AUTH_COMPLEX_PASSWORD_ENABLED")); value != "" {
		if parsed, err := strconv.ParseBool(value); err == nil {
			cfg.Auth.ComplexPasswordEnabled = parsed
		}
	}

	if value := strings.TrimSpace(os.Getenv("WEKNORA_AUTH_DEFAULT_TENANT_MODE")); value != "" {
		cfg.Auth.DefaultTenantMode = value
	}
	if strings.TrimSpace(cfg.Auth.DefaultTenantMode) == "" {
		cfg.Auth.DefaultTenantMode = AuthDefaultTenantModeCreatePersonal
	}

	if value := strings.TrimSpace(os.Getenv("WEKNORA_TENANT_ENABLE_RBAC")); value != "" {
		v := strings.EqualFold(value, "true")
		cfg.Tenant.EnableRBAC = &v
	}
	if cfg.Tenant.EnableRBAC == nil {
		// Default: enforce. Operators opt out of enforcement explicitly
		// via config.yaml `enable_rbac: false` or the env override.
		on := true
		cfg.Tenant.EnableRBAC = &on
	}

	// WEKNORA_TENANT_ENABLE_CROSS_TENANT_ACCESS mirrors the RBAC switch above.
	// It must be read explicitly: viper.AutomaticEnv has no SetEnvPrefix, so the
	// WEKNORA_-prefixed env var is never bound to the nested struct field —
	// without this block, only config.yaml's enable_cross_tenant_access takes
	// effect and the documented env override is silently ignored. The default
	// stays whatever config.yaml provides (false unless set there).
	if value := strings.TrimSpace(os.Getenv("WEKNORA_TENANT_ENABLE_CROSS_TENANT_ACCESS")); value != "" {
		cfg.Tenant.EnableCrossTenantAccess = strings.EqualFold(value, "true")
	}

	if value := strings.TrimSpace(os.Getenv("WEKNORA_TENANT_SELF_SERVICE_CREATION_ENABLED")); value != "" {
		if enabled, err := strconv.ParseBool(value); err == nil {
			cfg.Tenant.SelfServiceCreationEnabled = &enabled
		} else {
			fmt.Printf(
				"[config] WEKNORA_TENANT_SELF_SERVICE_CREATION_ENABLED=%q is not a boolean, ignoring\n",
				value,
			)
		}
	}
	if cfg.Tenant.SelfServiceCreationEnabled == nil {
		on := true
		cfg.Tenant.SelfServiceCreationEnabled = &on
	}

	if value := strings.TrimSpace(os.Getenv("WEKNORA_TENANT_MAX_OWNED_PER_USER")); value != "" {
		if n, err := strconv.Atoi(value); err == nil {
			cfg.Tenant.MaxOwnedPerUser = n
		} else {
			fmt.Printf(
				"[config] WEKNORA_TENANT_MAX_OWNED_PER_USER=%q is not an integer, ignoring\n",
				value,
			)
		}
	}
}

// applyAuditDefaults fills in defaults for the Audit config section
// and applies the env override commonly used to extend or disable
// retention without editing config.yaml.
//
// Defaults:
//   - When the `audit:` section is omitted entirely from YAML,
//     RetentionDays = 90 (purge rows older than 90 days).
//
// Operator intent is otherwise preserved: an explicit
// `audit.retention_days: 0` in YAML means "disable the purge", which
// is a supported posture for compliance use cases that handle archival
// off-database.
//
// Env overrides (when set and parseable; out-of-range is ignored):
//   - WEKNORA_AUDIT_RETENTION_DAYS (non-negative integer)
func applyAuditDefaults(cfg *Config) {
	// Section omitted entirely -> apply the default and no env wiring
	// is needed for the most common path.
	if cfg.Audit == nil {
		cfg.Audit = &AuditConfig{RetentionDays: 90}
	}

	// Env override always wins, but only when explicitly set so a
	// stale shell variable doesn't suddenly disable the purge for a
	// future deployment that committed a real value.
	if value := strings.TrimSpace(os.Getenv("WEKNORA_AUDIT_RETENTION_DAYS")); value != "" {
		if n, err := strconv.Atoi(value); err == nil && n >= 0 {
			cfg.Audit.RetentionDays = n
		}
	}
}

// into actual prompt text content. Only xxx_id fields are used;
// no fallback to default templates.
func backfillConversationDefaults(cfg *Config) {
	pt := cfg.PromptTemplates
	conv := cfg.Conversation

	if conv.FallbackPromptID != "" {
		if t := FindTemplateByID(pt, conv.FallbackPromptID); t != nil {
			conv.FallbackPrompt = t.Content
		} else {
			fmt.Printf("Warning: fallback_prompt_id %q not found\n", conv.FallbackPromptID)
		}
	}
	if conv.RewritePromptID != "" {
		if t := FindTemplateByID(pt, conv.RewritePromptID); t != nil {
			conv.RewritePromptSystem = t.Content
			conv.RewritePromptUser = t.User
		} else {
			fmt.Printf("Warning: rewrite_prompt_id %q not found\n", conv.RewritePromptID)
		}
	}
	if conv.GenerateSessionTitlePromptID != "" {
		if t := FindTemplateByID(pt, conv.GenerateSessionTitlePromptID); t != nil {
			conv.GenerateSessionTitlePrompt = t.Content
		} else {
			fmt.Printf("Warning: generate_session_title_prompt_id %q not found\n", conv.GenerateSessionTitlePromptID)
		}
	}
	if conv.GenerateSummaryPromptID != "" {
		if t := FindTemplateByID(pt, conv.GenerateSummaryPromptID); t != nil {
			conv.GenerateSummaryPrompt = t.Content
		} else {
			fmt.Printf("Warning: generate_summary_prompt_id %q not found\n", conv.GenerateSummaryPromptID)
		}
	}
	if conv.GenerateKBDescriptionPromptID != "" {
		if t := FindTemplateByID(pt, conv.GenerateKBDescriptionPromptID); t != nil {
			conv.GenerateKBDescriptionPrompt = t.Content
		} else {
			fmt.Printf("Warning: generate_kb_description_prompt_id %q not found\n", conv.GenerateKBDescriptionPromptID)
		}
	}
	if conv.ExtractEntitiesPromptID != "" {
		if t := FindTemplateByID(pt, conv.ExtractEntitiesPromptID); t != nil {
			conv.ExtractEntitiesPrompt = t.Content
		} else {
			fmt.Printf("Warning: extract_entities_prompt_id %q not found\n", conv.ExtractEntitiesPromptID)
		}
	}
	if conv.ExtractRelationshipsPromptID != "" {
		if t := FindTemplateByID(pt, conv.ExtractRelationshipsPromptID); t != nil {
			conv.ExtractRelationshipsPrompt = t.Content
		} else {
			fmt.Printf("Warning: extract_relationships_prompt_id %q not found\n", conv.ExtractRelationshipsPromptID)
		}
	}
	if conv.GenerateQuestionsPromptID != "" {
		if t := FindTemplateByID(pt, conv.GenerateQuestionsPromptID); t != nil {
			conv.GenerateQuestionsPrompt = t.Content
		} else {
			fmt.Printf("Warning: generate_questions_prompt_id %q not found\n", conv.GenerateQuestionsPromptID)
		}
	}
	if conv.Summary != nil {
		if conv.Summary.PromptID != "" {
			if t := FindTemplateByID(pt, conv.Summary.PromptID); t != nil {
				conv.Summary.Prompt = t.Content
			} else {
				fmt.Printf("Warning: summary.prompt_id %q not found\n", conv.Summary.PromptID)
			}
		}
		if conv.Summary.ContextTemplateID != "" {
			if t := FindTemplateByID(pt, conv.Summary.ContextTemplateID); t != nil {
				conv.Summary.ContextTemplate = t.Content
			} else {
				fmt.Printf("Warning: summary.context_template_id %q not found\n", conv.Summary.ContextTemplateID)
			}
		}
	}

	// Build intent→system-prompt map from IntentPrompts templates.
	// Template ID must equal the QueryIntent string value (e.g. "greeting").
	if len(pt.IntentPrompts) > 0 {
		conv.IntentSystemPrompts = make(map[string]string, len(pt.IntentPrompts))
		for _, t := range pt.IntentPrompts {
			if t.ID != "" && t.Content != "" {
				conv.IntentSystemPrompts[t.ID] = t.Content
			}
		}
	}
}

// FindTemplateByID searches across all template lists for a template with the given ID.
// It returns the template if found, or nil otherwise.
func FindTemplateByID(pt *PromptTemplatesConfig, id string) *PromptTemplate {
	if pt == nil || id == "" {
		return nil
	}
	// Search all template collections
	for _, list := range [][]PromptTemplate{
		pt.SystemPrompt,
		pt.ContextTemplate,
		pt.Rewrite,
		pt.Fallback,
		pt.GenerateSessionTitle,
		pt.GenerateSummary,
		pt.GenerateKBDescription,
		pt.KeywordsExtraction,
		pt.AgentSystemPrompt,
		pt.GraphExtraction,
		pt.GenerateQuestions,
		pt.IntentPrompts,
	} {
		for i := range list {
			if list[i].ID == id {
				return &list[i]
			}
		}
	}
	return nil
}

// resolveBuiltinAgentPromptIDs resolves system_prompt_id and context_template_id
// references in builtin agent configs by looking up the actual content from
// prompt template YAML files.
func resolveBuiltinAgentPromptIDs(pt *PromptTemplatesConfig) {
	types.ResolveBuiltinAgentPromptRefs(func(id string) string {
		if t := FindTemplateByID(pt, id); t != nil {
			return t.Content
		}
		return ""
	})
}

// promptTemplateFile 用于解析模板文件
type promptTemplateFile struct {
	Templates []PromptTemplate `yaml:"templates"`
}

// loadPromptTemplates 从目录加载提示词模板
func loadPromptTemplates(configDir string) (*PromptTemplatesConfig, error) {
	templatesDir := filepath.Join(configDir, "prompt_templates")

	// 检查目录是否存在
	if _, err := os.Stat(templatesDir); os.IsNotExist(err) {
		return nil, nil // 目录不存在，返回nil让调用者使用配置文件中的模板
	}

	config := &PromptTemplatesConfig{}

	// 定义模板文件映射
	templateFiles := map[string]*[]PromptTemplate{
		"system_prompt.yaml":           &config.SystemPrompt,
		"context_template.yaml":        &config.ContextTemplate,
		"rewrite.yaml":                 &config.Rewrite,
		"fallback.yaml":                &config.Fallback,
		"generate_session_title.yaml":  &config.GenerateSessionTitle,
		"generate_summary.yaml":        &config.GenerateSummary,
		"generate_kb_description.yaml": &config.GenerateKBDescription,
		"keywords_extraction.yaml":     &config.KeywordsExtraction,
		"agent_system_prompt.yaml":     &config.AgentSystemPrompt,
		"graph_extraction.yaml":        &config.GraphExtraction,
		"generate_questions.yaml":      &config.GenerateQuestions,
		"intent_prompts.yaml":          &config.IntentPrompts,
	}

	// 加载每个模板文件
	for filename, target := range templateFiles {
		filePath := filepath.Join(templatesDir, filename)
		if _, err := os.Stat(filePath); os.IsNotExist(err) {
			continue // 文件不存在，跳过
		}

		data, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to read %s: %w", filename, err)
		}

		var file promptTemplateFile
		if err := yaml.Unmarshal(data, &file); err != nil {
			return nil, fmt.Errorf("failed to parse %s: %w", filename, err)
		}

		*target = file.Templates
	}

	return config, nil
}

// WebSearchConfig represents the web search configuration
type WebSearchConfig struct {
	Timeout int `yaml:"timeout" json:"timeout"` // 超时时间（秒）
}
