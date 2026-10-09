package repository

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"gorm.io/gorm"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
)

type OriginalBodyJournalPaths struct{ Directory, KeyPath, PinPath string }
type originalBodyJournal struct{ paths OriginalBodyJournalPaths }
type originalBodyJournalEvent struct {
	Sequence                                                                  int64  `json:"sequence"`
	PreviousSHA256                                                            string `json:"previous_sha256"`
	BodyID, BodySHA256, ScopeSHA256                                           string
	TenantID                                                                  uint64
	Fields                                                                    []string
	BeforeBytes, BodyDeadlineMS, MetadataDeadlineMS, RecordedAtMS, PurgeEpoch int64
	Signature, HeadSHA256                                                     string
}
type originalBodyJournalPin struct {
	Sequence              int64
	HeadSHA256, Signature string
}
type originalBodyJournalHead struct {
	ID         int `gorm:"primaryKey"`
	Sequence   int64
	HeadSHA256 string
}

func (originalBodyJournalHead) TableName() string { return "original_body_journal_heads" }
func originalBodyJournalFromEnv() *originalBodyJournal {
	p := OriginalBodyJournalPaths{os.Getenv("WEKNORA_ORIGINAL_BODY_JOURNAL_DIRECTORY"), os.Getenv("WEKNORA_ORIGINAL_BODY_JOURNAL_KEY"), os.Getenv("WEKNORA_ORIGINAL_BODY_JOURNAL_PIN")}
	if p.Directory == "" && p.KeyPath == "" && p.PinPath == "" {
		return nil
	}
	return &originalBodyJournal{p}
}
func (s *OriginalBodyRetentionStore) ConfigureJournal(paths OriginalBodyJournalPaths) {
	s.journal = &originalBodyJournal{paths}
}
func bodyPrivateFile(path string, mode os.FileMode) ([]byte, error) {
	if !filepath.IsAbs(path) {
		return nil, ErrOriginalBodyUnavailable
	}
	parent, e := filepath.EvalSymlinks(filepath.Dir(path))
	if e != nil || parent != filepath.Dir(path) {
		return nil, ErrOriginalBodyUnavailable
	}
	st, e := os.Lstat(path)
	if e != nil {
		return nil, e
	}
	if !st.Mode().IsRegular() || st.Mode().Perm() != mode {
		return nil, ErrOriginalBodyUnavailable
	}
	if actual, ok := st.Sys().(*syscall.Stat_t); !ok || int(actual.Uid) != os.Geteuid() || actual.Nlink != 1 {
		return nil, ErrOriginalBodyUnavailable
	}
	if st.Size() > 64*1024*1024 {
		return nil, ErrOriginalBodyUnavailable
	}
	return os.ReadFile(path)
}
func (j *originalBodyJournal) key() ([]byte, error) {
	if j == nil {
		return nil, ErrOriginalBodyUnavailable
	}
	resolved, e := filepath.EvalSymlinks(j.paths.Directory)
	if e != nil || resolved != j.paths.Directory {
		return nil, ErrOriginalBodyUnavailable
	}
	st, e := os.Lstat(resolved)
	if e != nil || !st.IsDir() || st.Mode().Perm() != 0700 {
		return nil, ErrOriginalBodyUnavailable
	}
	if actual, ok := st.Sys().(*syscall.Stat_t); !ok || int(actual.Uid) != os.Geteuid() {
		return nil, ErrOriginalBodyUnavailable
	}
	key, e := bodyPrivateFile(j.paths.KeyPath, 0600)
	if e != nil || len(key) < 32 {
		return nil, ErrOriginalBodyUnavailable
	}
	return key, nil
}
func bodyMAC(key, data []byte) string {
	m := hmac.New(sha256.New, key)
	m.Write(data)
	return hex.EncodeToString(m.Sum(nil))
}
func bodyEventCore(event originalBodyJournalEvent) []byte {
	event.Signature = ""
	event.HeadSHA256 = ""
	b, _ := json.Marshal(event)
	return b
}
func bodyPinMAC(key []byte, p originalBodyJournalPin) string {
	return bodyMAC(key, []byte("body-pin-v1\n"+strconv.FormatInt(p.Sequence, 10)+"\n"+p.HeadSHA256))
}
func (j *originalBodyJournal) read() ([]originalBodyJournalEvent, originalBodyJournalPin, error) {
	var pin originalBodyJournalPin
	key, e := j.key()
	if e != nil {
		return nil, pin, e
	}
	raw, e := bodyPrivateFile(filepath.Join(j.paths.Directory, "purge-ledger.json"), 0600)
	if e != nil {
		return nil, pin, e
	}
	var events []originalBodyJournalEvent
	if json.Unmarshal(raw, &events) != nil {
		return nil, pin, ErrOriginalBodyUnavailable
	}
	head := ""
	for n, event := range events {
		if event.Sequence != int64(n+1) || event.PreviousSHA256 != head || !hmac.Equal([]byte(event.Signature), []byte(bodyMAC(key, bodyEventCore(event)))) {
			return nil, pin, ErrOriginalBodyUnavailable
		}
		head = bodySHA(append(bodyEventCore(event), []byte(event.Signature)...))
		if event.HeadSHA256 != head {
			return nil, pin, ErrOriginalBodyUnavailable
		}
	}
	raw, e = bodyPrivateFile(j.paths.PinPath, 0600)
	if e != nil {
		return nil, pin, e
	}
	if json.Unmarshal(raw, &pin) != nil || pin.Sequence != int64(len(events)) || pin.HeadSHA256 != head || !hmac.Equal([]byte(pin.Signature), []byte(bodyPinMAC(key, pin))) {
		return nil, pin, ErrOriginalBodyUnavailable
	}
	return events, pin, nil
}
func bodyWritePrivateAtomic(path string, data []byte) error {
	parent, e := filepath.EvalSymlinks(filepath.Dir(path))
	if e != nil || parent != filepath.Dir(path) {
		return ErrOriginalBodyUnavailable
	}
	if st, e := os.Lstat(path); e == nil {
		if !st.Mode().IsRegular() || st.Mode().Perm() != 0600 {
			return ErrOriginalBodyUnavailable
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	temp, e := os.CreateTemp(parent, ".body-journal-")
	if e != nil {
		return e
	}
	name := temp.Name()
	defer os.Remove(name)
	if e = temp.Chmod(0600); e != nil {
		temp.Close()
		return e
	}
	if _, e = temp.Write(data); e != nil {
		temp.Close()
		return e
	}
	if e = temp.Sync(); e != nil {
		temp.Close()
		return e
	}
	if e = temp.Close(); e != nil {
		return e
	}
	if e = os.Rename(name, path); e != nil {
		return e
	}
	dir, e := os.Open(parent)
	if e != nil {
		return e
	}
	defer dir.Close()
	return dir.Sync()
}

// Explicit owned initialization; a conflicting marker/file is never adopted.
func (s *OriginalBodyRetentionStore) InitializeJournal() error {
	key, e := s.journal.key()
	if e != nil {
		return e
	}
	path := filepath.Join(s.journal.paths.Directory, "purge-ledger.json")
	for _, p := range []string{path, s.journal.paths.PinPath} {
		if _, e := os.Lstat(p); !errors.Is(e, os.ErrNotExist) {
			return ErrOriginalBodyUnavailable
		}
	}
	if e = bodyWritePrivateAtomic(path, []byte("[]")); e != nil {
		return e
	}
	pin := originalBodyJournalPin{}
	pin.Signature = bodyPinMAC(key, pin)
	raw, _ := json.Marshal(pin)
	return bodyWritePrivateAtomic(s.journal.paths.PinPath, raw)
}
func (s *OriginalBodyRetentionStore) checkJournalInTx(tx *gorm.DB) error {
	var head originalBodyJournalHead
	if e := bodyInternal(tx).Where("id=1").Take(&head).Error; e != nil {
		return e
	}
	if s.journal == nil {
		if head.Sequence == 0 {
			return nil
		}
		return ErrOriginalBodyUnavailable
	}
	_, pin, e := s.journal.read()
	if e != nil {
		return e
	}
	if pin.Sequence != head.Sequence || pin.HeadSHA256 != head.HeadSHA256 {
		return ErrOriginalBodyUnavailable
	}
	return nil
}
func (s *OriginalBodyRetentionStore) checkExternalBody(id string) error {
	if s.journal == nil {
		return nil
	}
	events, _, e := s.journal.read()
	if e != nil {
		return e
	}
	for _, event := range events {
		if event.BodyID == id {
			return ErrOriginalBodyUnavailable
		}
	}
	return nil
}
func (j *originalBodyJournal) append(event originalBodyJournalEvent) (originalBodyJournalEvent, error) {
	events, pin, e := j.read()
	if e != nil {
		return event, e
	}
	key, e := j.key()
	if e != nil {
		return event, e
	}
	lock, e := os.OpenFile(filepath.Join(j.paths.Directory, "writer.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return event, e
	}
	defer lock.Close()
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		return event, e
	}
	events, pin, e = j.read()
	if e != nil {
		return event, e
	}
	for _, old := range events {
		if old.BodyID == event.BodyID {
			if old.BodySHA256 != event.BodySHA256 || old.ScopeSHA256 != event.ScopeSHA256 {
				return event, ErrOriginalBodyUnavailable
			}
			return old, nil
		}
	}
	event.Sequence = pin.Sequence + 1
	event.PreviousSHA256 = pin.HeadSHA256
	event.Signature = bodyMAC(key, bodyEventCore(event))
	event.HeadSHA256 = bodySHA(append(bodyEventCore(event), []byte(event.Signature)...))
	events = append(events, event)
	raw, e := json.Marshal(events)
	if e != nil {
		return event, e
	}
	if e = bodyWritePrivateAtomic(filepath.Join(j.paths.Directory, "purge-ledger.json"), raw); e != nil {
		return event, e
	}
	pin = originalBodyJournalPin{Sequence: event.Sequence, HeadSHA256: event.HeadSHA256}
	pin.Signature = bodyPinMAC(key, pin)
	raw, _ = json.Marshal(pin)
	return event, bodyWritePrivateAtomic(j.paths.PinPath, raw)
}

var _ = fmt.Sprintf
