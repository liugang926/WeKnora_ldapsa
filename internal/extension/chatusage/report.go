// Package chatusage reports usage of human-initiated WeKnora conversations
// across workspaces. It is a read-only projection of retained chat history.
package chatusage

import (
	"context"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
)

// Tokens contains provider counts and coverage of retained assistant replies.
type Tokens struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	TotalTokens      int64 `json:"total_tokens"`
	ReportedTurns    int64 `json:"reported_turns"`
	UnreportedTurns  int64 `json:"unreported_turns"`
}

// Questions counts persisted human submissions, independent of reply completion.
type Questions struct {
	QuestionCount         int64 `json:"question_count"`
	WebQuestionCount      int64 `json:"web_question_count"`
	DingTalkQuestionCount int64 `json:"dingtalk_question_count" gorm:"column:dingtalk_question_count"`
}

// Counts aggregates distinct conversations and their usage.
type Counts struct {
	SessionCount int64 `json:"session_count"`
	Questions
	Tokens
}

// Summary covers all participating users, independent of the search term.
type Summary struct {
	UserCount int64 `json:"user_count"`
	Counts
}

// Person is an internal account with at least one qualifying conversation.
type Person struct {
	UserID      string `json:"user_id"`
	Username    string `json:"username"`
	DisplayName string `json:"display_name"`
	Counts
}

// Session identifies a conversation and the workspace in which it was started.
type Session struct {
	SessionID     string    `json:"session_id"`
	StartedAt     time.Time `json:"started_at"`
	WorkspaceID   uint64    `json:"workspace_id"`
	WorkspaceName string    `json:"workspace_name"`
	Source        string    `json:"source"`
	Questions
	Tokens
}

type page struct {
	Page     int   `json:"page"`
	PageSize int   `json:"page_size"`
	Total    int64 `json:"total"`
}

type peopleReport struct {
	page
	Summary              Summary  `json:"summary"`
	UnattributedDingTalk Counts   `json:"unattributed_dingtalk"`
	Items                []Person `json:"items"`
}

type sessionsReport struct {
	page
	Items []Session `json:"items"`
}

// usageCounter returns NULL for absent, malformed or out-of-range counters.
// The three keys passed here are constants, never user input.
func usageCounter(dialect, key string) string {
	if dialect == "sqlite" {
		return fmt.Sprintf(`CASE WHEN json_type(usage, '$.%[1]s') = 'integer'
			AND typeof(usage ->> '%[1]s') = 'integer' AND (usage ->> '%[1]s') >= 0
			THEN usage ->> '%[1]s' END`, key)
	}
	// -> serializes strings with quotes, so a numeric-looking JSON string
	// doesn't pass. Guard the cast itself to keep even malformed history readable.
	return fmt.Sprintf(`CASE WHEN CAST(usage -> '%[1]s' AS text) ~ '^[0-9]{1,19}$' THEN
		CASE WHEN CAST(usage ->> '%[1]s' AS numeric) <= 9223372036854775807
		THEN CAST(usage ->> '%[1]s' AS bigint) END END`, key)
}

// Use the IM writer's verified sender snapshot for personal attribution. A
// legacy channel mapping can establish the platform, but its user_id is the
// first thread creator (and is not historically corp-qualified). Never use it
// to infer who asked a question, even if an account has a matching name today.
// Forks retain the original creation times of copied messages.
func sessionUsage(dialect string) string {
	payload := "CASE WHEN m.role = 'assistant' THEN m.usage END"
	reported := "usage -> 'provider_reported' = 'true'::jsonb"
	if dialect == "sqlite" {
		payload = "CASE WHEN m.role = 'assistant' AND json_valid(m.usage) THEN m.usage END"
		reported = "json_type(usage, '$.provider_reported') = 'true'"
	}
	return `WITH retained AS (
		SELECT 'm:' || m.id AS id, COALESCE(e.session_id, s.id) AS session_id, s.user_id AS web_user_id,
		       CASE WHEN e.event_key IS NOT NULL THEN e.actor_user_id
		         WHEN m.usage_source = 'dingtalk' THEN m.usage_actor_user_id END AS usage_actor_user_id,
		       CASE WHEN m.usage_source = 'dingtalk' THEN m.usage_event_key END AS usage_event_key,
		       m.role, m.created_at, ` + payload + ` AS usage,
		       CASE WHEN m.channel = 'im' AND (m.usage_source = 'dingtalk' OR (
		         COALESCE(m.usage_source,'') = '' AND EXISTS (
		           SELECT 1 FROM im_channel_sessions cs WHERE cs.session_id = s.id
		             AND cs.tenant_id = s.tenant_id AND cs.platform = 'dingtalk'))) THEN 'dingtalk'
		         WHEN s.owner_auth_source = 'web' AND COALESCE(m.channel,'') <> 'im' THEN 'web' END AS source
		FROM sessions s JOIN messages m ON m.session_id = s.id
		LEFT JOIN im_usage_submissions e ON m.channel = 'im' AND m.usage_source = 'dingtalk'
		  AND e.source = 'dingtalk' AND e.event_key = NULLIF(m.usage_event_key,'')
		WHERE s.kind IN ('', 'assistant') AND m.role IN ('user','assistant')
		  AND (COALESCE(s.parent_session_id,'') = '' OR m.created_at > s.created_at)
		  AND (m.role <> 'user' OR e.event_key IS NULL)
		UNION ALL
		SELECT 'e:' || e.event_key, s.id, '', e.actor_user_id, NULL, 'user', e.created_at, NULL, 'dingtalk'
		FROM im_usage_submissions e JOIN sessions s ON s.id = e.session_id
		WHERE e.source = 'dingtalk' AND s.kind IN ('', 'assistant')
	), identified AS (
		SELECT r.*, u.id AS user_id, u.username
		FROM retained r LEFT JOIN users u ON u.id =
		  CASE WHEN source = 'web' THEN web_user_id ELSE NULLIF(usage_actor_user_id,'') END
		WHERE source = 'dingtalk' OR (source = 'web' AND u.id IS NOT NULL)
	), delivery_ordinals AS (
		SELECT id, ROW_NUMBER() OVER (PARTITION BY usage_event_key ORDER BY created_at, id) AS ordinal
		FROM identified WHERE source = 'dingtalk' AND role = 'user' AND COALESCE(usage_event_key,'') <> ''
	), counters AS (
		SELECT session_id, user_id, username, source, role, created_at,
		       COALESCE(d.ordinal, 1) AS event_ordinal,
		       ` + reported + ` AS provider_reported,
			` + usageCounter(dialect, "prompt_tokens") + ` AS input_count,
			` + usageCounter(dialect, "completion_tokens") + ` AS output_count,
			` + usageCounter(dialect, "total_tokens") + ` AS provider_total
		FROM identified q LEFT JOIN delivery_ordinals d ON d.id = q.id
	), turn_usage AS (
		SELECT *, CASE WHEN role = 'user' AND event_ordinal = 1 THEN 1 ELSE 0 END AS question,
			CASE WHEN input_count > 0 OR output_count > 0 OR provider_total > 0
				OR (provider_reported AND COALESCE(input_count, output_count, provider_total) IS NOT NULL)
				THEN 1 ELSE 0 END AS reported
		FROM counters
	), session_usage AS (
		SELECT session_id, user_id, username, source,
		       MIN(CASE WHEN question = 1 THEN created_at END) AS first_question,
		       SUM(question) AS question_count,
		       SUM(CASE WHEN source = 'web' THEN question ELSE 0 END) AS web_question_count,
		       SUM(CASE WHEN source = 'dingtalk' THEN question ELSE 0 END) AS dingtalk_question_count,
		       COALESCE(SUM(input_count), 0) AS prompt_tokens, COALESCE(SUM(output_count), 0) AS completion_tokens,
		       SUM(CASE WHEN provider_total > 0 THEN provider_total
		                ELSE COALESCE(input_count, 0) + COALESCE(output_count, 0) END) AS total_tokens,
		       SUM(reported) AS reported_turns,
		       SUM(CASE WHEN role = 'assistant' THEN 1 - reported ELSE 0 END) AS unreported_turns
		FROM turn_usage
		GROUP BY session_id, user_id, username, source
		HAVING SUM(question) > 0 OR (source = 'dingtalk' AND COUNT(CASE WHEN role = 'assistant' THEN 1 END) > 0)
	)`
}

// The same projection is used for people, all sources combined, and unknown
// DingTalk senders. COUNT DISTINCT keeps shared group conversations unique.
const totals = `COUNT(DISTINCT CASE WHEN question_count > 0 THEN session_id END) AS session_count,
	COALESCE(SUM(question_count),0) AS question_count, COALESCE(SUM(web_question_count),0) AS web_question_count,
	COALESCE(SUM(dingtalk_question_count),0) AS dingtalk_question_count,
	COALESCE(SUM(prompt_tokens),0) AS prompt_tokens,
	COALESCE(SUM(completion_tokens),0) AS completion_tokens, COALESCE(SUM(total_tokens),0) AS total_tokens,
	COALESCE(SUM(reported_turns),0) AS reported_turns, COALESCE(SUM(unreported_turns),0) AS unreported_turns`

func listPeople(ctx context.Context, db *gorm.DB, p page, query string) (peopleReport, error) {
	result := peopleReport{page: p, Items: []Person{}}
	var rows []struct {
		Person
		RowType   int64
		UserCount int64
		Matched   int64
	}
	// The summary row also carries the matching count, even for an empty page.
	// Reading summary and page in the same statement gives a consistent snapshot.
	const people = `, person_usage AS (
		SELECT user_id, username, ` + totals + `
		FROM session_usage WHERE user_id IS NOT NULL GROUP BY user_id, username
	), people AS (
		SELECT p.*, COALESCE(
			(SELECT TRIM(i.display_name) FROM directory_identities i
			 WHERE i.user_id = p.user_id AND i.status = 'active' AND TRIM(i.display_name) <> ''
			 ORDER BY i.last_seen_at DESC NULLS LAST, i.id LIMIT 1),
			(SELECT TRIM(d.display_name) FROM dingtalk_identities d
			 WHERE d.user_id = p.user_id AND d.status = 'active' AND TRIM(d.display_name) <> ''
			 ORDER BY d.id LIMIT 1), '') AS display_name
		FROM person_usage p
	), matching AS (SELECT * FROM people `
	const report = `), page_users AS (
		SELECT * FROM matching ORDER BY question_count DESC, user_id LIMIT ? OFFSET ?
	)
	SELECT 0 AS row_type, '' AS user_id, '' AS username, '' AS display_name,
	       (SELECT COUNT(*) FROM people) AS user_count, (SELECT COUNT(*) FROM matching) AS matched, ` + totals + `
	FROM session_usage
	UNION ALL
	SELECT 1, '', '', '', 0, 0, ` + totals + ` FROM session_usage WHERE user_id IS NULL AND source='dingtalk'
	UNION ALL
	SELECT 2, user_id, username, display_name, 0, 0, session_count, question_count, web_question_count,
	       dingtalk_question_count, prompt_tokens, completion_tokens, total_tokens, reported_turns, unreported_turns
	FROM page_users ORDER BY row_type, question_count DESC, user_id`
	var args []any
	filter := ""
	if query != "" {
		filter = `WHERE LOWER(username) LIKE LOWER(?) ESCAPE '!' OR LOWER(user_id) LIKE LOWER(?) ESCAPE '!'
			OR LOWER(display_name) LIKE LOWER(?) ESCAPE '!'`
		term := "%" + strings.NewReplacer("!", "!!", "%", "!%", "_", "!_").Replace(query) + "%"
		args = append(args, term, term, term)
	}
	args = append(args, p.PageSize, int64(p.Page-1)*int64(p.PageSize))
	sql := sessionUsage(db.Name()) + people + filter + report
	if err := db.WithContext(ctx).Raw(sql, args...).Scan(&rows).Error; err != nil {
		return result, err
	}
	for _, row := range rows {
		switch row.RowType {
		case 0:
			result.Summary = Summary{UserCount: row.UserCount, Counts: row.Counts}
			result.Total = row.Matched
		case 1:
			result.UnattributedDingTalk = row.Counts
		default:
			result.Items = append(result.Items, row.Person)
		}
	}
	return result, nil
}

func listSessions(ctx context.Context, db *gorm.DB, p page, userID string) (sessionsReport, error) {
	result := sessionsReport{page: p, Items: []Session{}}
	var rows []struct {
		Session
		IntakeStartedAt *time.Time
		Matched         int64
	}
	const report = `, individual AS (
		SELECT * FROM session_usage WHERE user_id = ? AND question_count > 0
	), page_sessions AS (
		SELECT * FROM individual ORDER BY first_question DESC, session_id LIMIT ? OFFSET ?
	)
	SELECT totals.matched, p.session_id, m.created_at AS started_at, e.created_at AS intake_started_at,
	       s.tenant_id AS workspace_id,
	       COALESCE(t.name, '') AS workspace_name, p.prompt_tokens, p.completion_tokens, p.total_tokens,
	       p.reported_turns, p.unreported_turns, p.source, p.question_count,
	       p.web_question_count, p.dingtalk_question_count
	FROM (SELECT COUNT(*) AS matched FROM individual) totals
	LEFT JOIN page_sessions p ON TRUE
	LEFT JOIN sessions s ON s.id = p.session_id
	LEFT JOIN tenants t ON t.id = s.tenant_id
	LEFT JOIN messages m ON m.id = (SELECT MIN(id) FROM messages
		WHERE session_id = p.session_id AND role = 'user' AND created_at = p.first_question)
	LEFT JOIN im_usage_submissions e ON e.event_key = (SELECT MIN(event_key) FROM im_usage_submissions
		WHERE session_id = p.session_id AND source = 'dingtalk' AND created_at = p.first_question)
	ORDER BY p.first_question DESC, p.session_id`
	sql := sessionUsage(db.Name()) + report
	offset := int64(p.Page-1) * int64(p.PageSize)
	if err := db.WithContext(ctx).Raw(sql, userID, p.PageSize, offset).Scan(&rows).Error; err != nil {
		return result, err
	}
	for _, row := range rows {
		result.Total = row.Matched
		if row.SessionID != "" {
			if row.IntakeStartedAt != nil {
				row.StartedAt = *row.IntakeStartedAt
			}
			result.Items = append(result.Items, row.Session)
		}
	}
	return result, nil
}
