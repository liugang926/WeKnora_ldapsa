package repository

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Tencent/WeKnora/internal/types"
	"github.com/stretchr/testify/require"
)

func targetRecoverySigned(t *testing.T, p NextcloudRecoveryPlan, key []byte) []byte {
	t.Helper()
	raw, err := json.Marshal(p)
	require.NoError(t, err)
	mac := hmac.New(sha256.New, key)
	mac.Write(raw)
	envelope, err := json.Marshal(
		map[string]string{"plan": string(raw), "hmac_sha256": hex.EncodeToString(mac.Sum(nil))},
	)
	require.NoError(t, err)
	return envelope
}

func targetRecoveryPlan(t *testing.T) (NextcloudRecoveryPlan, NextcloudRecoveryTargetClosure) {
	t.Helper()
	now := time.Now().Unix()
	row := NextcloudTargetClosure{
		OperationID:       "7c0253d2-43bd-4fdb-b05f-048d52c2e766",
		TenantID:          7,
		KnowledgeBaseID:   "kb.with-dot",
		InstanceID:        "instance",
		BindingID:         "binding",
		OperationRevision: 2,
		Reason:            "knowledge_base_deleted",
		DeletionTimeMS:    (now - 10) * 1000,
	}
	row.ClosureID = targetClosureID(row)
	proof := NextcloudRecoveryTargetClosure{
		Target: types.NextcloudTargetObservation{
			Schema:            "weknora-nextcloud-target-v1",
			State:             "target_deleted",
			Reason:            row.Reason,
			OperationID:       row.OperationID,
			InstanceID:        row.InstanceID,
			BindingID:         row.BindingID,
			TenantID:          "7",
			KnowledgeBaseID:   row.KnowledgeBaseID,
			OperationRevision: "2",
			ClosureID:         row.ClosureID,
			DeletionTimeMS:    fmt.Sprint(row.DeletionTimeMS),
			ClosedAt:          fmt.Sprint(now - 9),
		},
		RootFileID: "77",
		RootKey:    recoverySHA([]byte("nextcloud-filecache-root-v1\x0077")),
	}
	previous := strings.Repeat("c", 64)
	event := NextcloudRecoveryJournalEvent{
		Sequence:       "6",
		BindingID:      "binding",
		Kind:           "target_deleted",
		SourceRevision: "2",
		CreatedAt:      now,
		TargetClosure:  &proof,
	}
	lines := []string{previous, "6", "binding", "-", "target_deleted", "2", fmt.Sprint(now)}
	chain := recoverySHA([]byte(strings.Join(append(lines, targetClosureJournalLines(proof)...), "\n")))
	p := NextcloudRecoveryPlan{
		SchemaVersion:                       1,
		InstanceID:                          "instance",
		StreamID:                            "53f8d222-2331-4731-b799-9e179638cfd0",
		CheckpointRecordSHA256:              strings.Repeat("a", 64),
		RecoveryRecordSHA256:                strings.Repeat("b", 64),
		CheckpointSequence:                  "5",
		ThroughSequence:                     "6",
		CheckpointDatabaseChainSHA256:       previous,
		ThroughDatabaseChainSHA256:          chain,
		DatabasePrefixHashes:                map[string]string{"5": previous, "6": chain},
		CloseBindings:                       []NextcloudRecoveryBinding{{"binding", "6"}},
		WithdrawFiles:                       []NextcloudRecoveryFile{},
		AuthoritativeReconciliationRequired: true,
		JournalEvents:                       []NextcloudRecoveryJournalEvent{event},
		TargetClosures:                      []NextcloudRecoveryTargetClosure{proof},
	}
	return p, proof
}

func TestNextcloudTargetRecoveryTypedTailAndRetainedClosureProof(t *testing.T) {
	p, proof := targetRecoveryPlan(t)
	key := []byte(strings.Repeat("k", 32))
	verified, err := VerifyNextcloudRecoveryPlan(targetRecoverySigned(t, p, key), key)
	require.NoError(t, err)
	require.Equal(t, proof, verified.TargetClosures[0])
	for _, name := range []string{
		"omit-history",
		"changed-scope",
		"changed-root",
		"decimal-alias",
		"wrong-state",
		"file-event",
		"changed-hash",
	} {
		t.Run(name, func(t *testing.T) {
			candidate, _ := targetRecoveryPlan(t)
			switch name {
			case "omit-history":
				candidate.TargetClosures = nil
			case "changed-scope":
				candidate.TargetClosures[0].Target.TenantID = "8"
			case "changed-root":
				candidate.TargetClosures[0].RootKey = strings.Repeat("a", 64)
			case "decimal-alias":
				candidate.TargetClosures[0].Target.OperationRevision = "02"
			case "wrong-state":
				candidate.TargetClosures[0].Target.State = "present"
			case "file-event":
				fid := "77"
				candidate.JournalEvents[0].FileID = &fid
			case "changed-hash":
				candidate.TargetClosures[0].Target.ClosureID = strings.Repeat("a", 64)
			}
			_, err := VerifyNextcloudRecoveryPlan(targetRecoverySigned(t, candidate, key), key)
			require.Error(t, err)
		})
	}
	// Permanent proof before the checkpoint still survives when there is no tail.
	p.JournalEvents = nil
	p.CheckpointSequence = "6"
	p.CheckpointDatabaseChainSHA256 = p.ThroughDatabaseChainSHA256
	p.DatabasePrefixHashes = map[string]string{"6": p.ThroughDatabaseChainSHA256}
	p.CloseBindings = []NextcloudRecoveryBinding{}
	verified, err = VerifyNextcloudRecoveryPlan(targetRecoverySigned(t, p, key), key)
	require.NoError(t, err)
	require.Equal(t, proof, verified.TargetClosures[0])
}
