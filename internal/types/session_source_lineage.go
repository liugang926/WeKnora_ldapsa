package types

import "encoding/json"

// MarshalJSON encodes the public session projection without private lineage fields.
// Raw repository models cannot serialize unverified generated text. Service
// readers mark a field only after owner checks and current lineage authorization.
func (s Session) MarshalJSON() ([]byte, error) {
	type plainSession Session
	copied := s
	if !s.TitleAuthorized || s.TitleSourceLineage.RequireComplete() != nil {
		copied.Title = ""
	}
	if !s.DescriptionAuthorized || s.DescriptionSourceLineage.RequireComplete() != nil {
		copied.Description = ""
	}
	return json.Marshal(plainSession(copied))
}

// MarshalJSON encodes the public session projection without private lineage fields.
// An embedded Marshaler would otherwise discard the outer IM-origin fields.
func (s SessionListItem) MarshalJSON() ([]byte, error) {
	payload, err := s.Session.MarshalJSON()
	if err != nil {
		return nil, err
	}
	var fields map[string]any
	if err := json.Unmarshal(payload, &fields); err != nil {
		return nil, err
	}
	for key, value := range map[string]string{"im_platform": s.IMPlatform, "im_chat_id": s.IMChatID, "im_thread_id": s.IMThreadID, "im_user_id": s.IMUserID, "im_agent_id": s.IMAgentID, "im_channel_id": s.IMChannelID} {
		if value != "" {
			fields[key] = value
		}
	}
	return json.Marshal(fields)
}
