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

// Counts aggregates distinct conversations and their usage.
type Counts struct {
	SessionCount int64 `json:"session_count"`
	Tokens
}

// Summary covers all participating users, independent of the search term.
type Summary struct {
	UserCount int64 `json:"user_count"`
	Counts
}

// Person is an internal account with at least one qualifying conversation.
type Person struct {
	UserID   string `json:"user_id"`
	Username string `json:"username"`
	Counts
}

// Session identifies a conversation and the workspace in which it was started.
type Session struct {
	SessionID     string    `json:"session_id"`
	StartedAt     time.Time `json:"started_at"`
	WorkspaceID   uint64    `json:"workspace_id"`
	WorkspaceName string    `json:"workspace_name"`
	Tokens
}

type page struct {
	Page     int   `json:"page"`
	PageSize int   `json:"page_size"`
	Total    int64 `json:"total"`
}

type peopleReport struct {
	page
	Summary Summary  `json:"summary"`
	Items   []Person `json:"items"`
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

// Only web authentication establishes an internal person's identity. Legacy
// blank sources and externally supplied owner IDs cannot prove ownership.
// Soft deletion changes transcript visibility, not incurred usage.
// Forks copy messages with their original creation time, before the branch.
func sessionUsage(dialect string, personOnly bool) string {
	scope := ""
	if personOnly {
		scope = " AND u.id = ?"
	}
	payload := "CASE WHEN m.role = 'assistant' THEN m.usage END"
	reported := "usage -> 'provider_reported' = 'true'::jsonb"
	if dialect == "sqlite" {
		payload = "CASE WHEN m.role = 'assistant' AND json_valid(m.usage) THEN m.usage END"
		reported = "json_type(usage, '$.provider_reported') = 'true'"
	}
	return `WITH retained_messages AS (
		SELECT s.id AS session_id, u.id AS user_id, u.username, m.role, m.created_at, ` + payload + ` AS usage
		FROM users u
		JOIN sessions s ON s.user_id = u.id
		JOIN messages m ON m.session_id = s.id
		WHERE s.owner_auth_source = 'web' AND s.kind IN ('', 'assistant')
		  AND m.role IN ('user', 'assistant')
		  AND (COALESCE(s.parent_session_id, '') = '' OR m.created_at > s.created_at)` + scope + `
	), counters AS (
		SELECT session_id, user_id, username, role, created_at, ` + reported + ` AS provider_reported,
			` + usageCounter(dialect, "prompt_tokens") + ` AS input_count,
			` + usageCounter(dialect, "completion_tokens") + ` AS output_count,
			` + usageCounter(dialect, "total_tokens") + ` AS provider_total
		FROM retained_messages
	), turn_usage AS (
		SELECT *,
			CASE WHEN input_count > 0 OR output_count > 0 OR provider_total > 0
				OR (provider_reported AND COALESCE(input_count, output_count, provider_total) IS NOT NULL)
				THEN 1 ELSE 0 END AS reported
		FROM counters
	), session_usage AS (
		SELECT session_id, user_id, username,
		       MIN(CASE WHEN role = 'user' THEN created_at END) AS first_question,
		       COALESCE(SUM(input_count), 0) AS prompt_tokens, COALESCE(SUM(output_count), 0) AS completion_tokens,
		       SUM(CASE WHEN provider_total > 0 THEN provider_total
		                ELSE COALESCE(input_count, 0) + COALESCE(output_count, 0) END) AS total_tokens,
		       SUM(reported) AS reported_turns,
		       SUM(CASE WHEN role = 'assistant' THEN 1 - reported ELSE 0 END) AS unreported_turns
		FROM turn_usage
		GROUP BY session_id, user_id, username
		HAVING COUNT(CASE WHEN role = 'user' THEN 1 END) > 0
	)`
}

func listPeople(ctx context.Context, db *gorm.DB, p page, query string) (peopleReport, error) {
	result := peopleReport{page: p, Items: []Person{}}
	var rows []struct {
		Person
		UserCount int64
		Matched   int64
	}
	// The summary row also carries the matching count, even for an empty page.
	// Reading summary and page in the same statement gives a consistent snapshot.
	const people = `, people AS (
		SELECT user_id, username, COUNT(*) AS session_count,
		       SUM(prompt_tokens) AS prompt_tokens, SUM(completion_tokens) AS completion_tokens,
		       SUM(total_tokens) AS total_tokens, SUM(reported_turns) AS reported_turns,
		       SUM(unreported_turns) AS unreported_turns
		FROM session_usage GROUP BY user_id, username
	), matching AS (SELECT * FROM people `
	const report = `), page_users AS (
		SELECT * FROM matching ORDER BY session_count DESC, user_id LIMIT ? OFFSET ?
	)
	SELECT '' AS user_id, '' AS username, COUNT(*) AS user_count, (SELECT COUNT(*) FROM matching) AS matched,
	       COALESCE(SUM(session_count),0) AS session_count, COALESCE(SUM(prompt_tokens),0) AS prompt_tokens,
	       COALESCE(SUM(completion_tokens),0) AS completion_tokens, COALESCE(SUM(total_tokens),0) AS total_tokens,
	       COALESCE(SUM(reported_turns),0) AS reported_turns, COALESCE(SUM(unreported_turns),0) AS unreported_turns
	FROM people
	UNION ALL
	SELECT user_id, username, 0, 0, session_count, prompt_tokens, completion_tokens,
	       total_tokens, reported_turns, unreported_turns
	FROM page_users ORDER BY user_count DESC, session_count DESC, user_id`
	var args []any
	filter := ""
	if query != "" {
		filter = `WHERE LOWER(username) LIKE LOWER(?) ESCAPE '!' OR LOWER(user_id) LIKE LOWER(?) ESCAPE '!'`
		term := "%" + strings.NewReplacer("!", "!!", "%", "!%", "_", "!_").Replace(query) + "%"
		args = append(args, term, term)
	}
	args = append(args, p.PageSize, int64(p.Page-1)*int64(p.PageSize))
	sql := sessionUsage(db.Dialector.Name(), false) + people + filter + report
	if err := db.WithContext(ctx).Raw(sql, args...).Scan(&rows).Error; err != nil {
		return result, err
	}
	for _, row := range rows {
		if row.UserID == "" {
			result.Summary = Summary{UserCount: row.UserCount, Counts: row.Counts}
			result.Total = row.Matched
		} else {
			result.Items = append(result.Items, row.Person)
		}
	}
	return result, nil
}

func listSessions(ctx context.Context, db *gorm.DB, p page, userID string) (sessionsReport, error) {
	result := sessionsReport{page: p, Items: []Session{}}
	var rows []struct {
		Session
		Matched int64
	}
	const report = `, page_sessions AS (
		SELECT * FROM session_usage ORDER BY first_question DESC, session_id LIMIT ? OFFSET ?
	)
	SELECT totals.matched, p.session_id, m.created_at AS started_at, s.tenant_id AS workspace_id,
	       COALESCE(t.name, '') AS workspace_name, p.prompt_tokens, p.completion_tokens, p.total_tokens,
	       p.reported_turns, p.unreported_turns
	FROM (SELECT COUNT(*) AS matched FROM session_usage) totals
	LEFT JOIN page_sessions p ON TRUE
	LEFT JOIN sessions s ON s.id = p.session_id
	LEFT JOIN tenants t ON t.id = s.tenant_id
	LEFT JOIN messages m ON m.id = (SELECT MIN(id) FROM messages
		WHERE session_id = p.session_id AND role = 'user' AND created_at = p.first_question)
	ORDER BY p.first_question DESC, p.session_id`
	sql := sessionUsage(db.Dialector.Name(), true) + report
	offset := int64(p.Page-1) * int64(p.PageSize)
	if err := db.WithContext(ctx).Raw(sql, userID, p.PageSize, offset).Scan(&rows).Error; err != nil {
		return result, err
	}
	for _, row := range rows {
		result.Total = row.Matched
		if row.SessionID != "" {
			result.Items = append(result.Items, row.Session)
		}
	}
	return result, nil
}
