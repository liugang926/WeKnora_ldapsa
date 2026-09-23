package service

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	apprepo "github.com/Tencent/WeKnora/internal/application/repository"
	"github.com/Tencent/WeKnora/internal/types"
	"github.com/Tencent/WeKnora/internal/types/interfaces"
	"github.com/google/uuid"
)

var (
	// ErrInvalidDirectoryConfig rejects unsafe or incomplete directory settings.
	ErrInvalidDirectoryConfig = errors.New("invalid directory configuration")
	// ErrDirectoryFileManaged rejects UI writes to file-managed settings.
	ErrDirectoryFileManaged = errors.New("directory configuration is file-managed")
	// ErrIncompleteDirectorySync prevents a partial snapshot from replacing memberships.
	ErrIncompleteDirectorySync = errors.New("directory snapshot is incomplete")
	// ErrDirectorySnapshotInvalid identifies inconsistent snapshot data.
	ErrDirectorySnapshotInvalid = errors.New("directory snapshot is invalid")
	// ErrDirectoryGroupCycle identifies a cycle in nested groups.
	ErrDirectoryGroupCycle = errors.New("directory group hierarchy contains a cycle")
	// ErrDirectorySyncInProgress indicates that another sync owns the lease.
	ErrDirectorySyncInProgress = errors.New("directory synchronization is already in progress")
	// ErrDirectorySyncLeaseLost indicates that a running sync lost its lease.
	ErrDirectorySyncLeaseLost = errors.New("directory synchronization lease was lost")
)

const (
	directorySyncLeaseDuration  = 2 * time.Minute
	directorySyncLeaseHeartbeat = 30 * time.Second
	directorySyncLeaseRelease   = 5 * time.Second
)

type directoryService struct {
	repo        interfaces.DirectoryRepository
	invalidator interfaces.PermissionInvalidator
	mu          sync.Mutex
	running     map[string]struct{}
}

// NewDirectoryService constructs the atomic directory snapshot service.
func NewDirectoryService(repo interfaces.DirectoryRepository) interfaces.DirectoryService {
	return NewDirectoryServiceWithInvalidator(repo, nil)
}

// NewDirectoryServiceWithInvalidator also invalidates access after a snapshot change.
func NewDirectoryServiceWithInvalidator(
	repo interfaces.DirectoryRepository,
	invalidator interfaces.PermissionInvalidator,
) interfaces.DirectoryService {
	return &directoryService{repo: repo, invalidator: invalidator, running: make(map[string]struct{})}
}

func (s *directoryService) Create(ctx context.Context, directory *types.Directory) (*types.Directory, error) {
	if err := normalizeDirectory(directory); err != nil {
		return nil, err
	}
	if err := s.repo.Create(ctx, directory); err != nil {
		return nil, err
	}
	return s.repo.Get(ctx, directory.ID)
}

func (s *directoryService) Update(ctx context.Context, directory *types.Directory) (*types.Directory, error) {
	if directory == nil || strings.TrimSpace(directory.ID) == "" {
		return nil, fmt.Errorf("%w: id is required", ErrInvalidDirectoryConfig)
	}
	existing, err := s.repo.Get(ctx, directory.ID)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, fmt.Errorf("directory %q not found", directory.ID)
	}
	if existing.IsFileManaged() {
		return nil, ErrDirectoryFileManaged
	}
	// An omitted password means "keep the encrypted credential", never clear
	// it accidentally from the redacted management response.
	if directory.PasswordCiphertext == "" {
		directory.PasswordCiphertext = existing.PasswordCiphertext
	}
	directory.ConfigSource = types.DirectoryConfigSourceUI
	if err := normalizeDirectory(directory); err != nil {
		return nil, err
	}
	if err := s.repo.Update(ctx, directory); err != nil {
		return nil, err
	}
	return s.repo.Get(ctx, directory.ID)
}

func normalizeDirectory(directory *types.Directory) error {
	if directory == nil {
		return fmt.Errorf("%w: nil configuration", ErrInvalidDirectoryConfig)
	}
	directory.Name = strings.TrimSpace(directory.Name)
	directory.BaseDN = strings.TrimSpace(directory.BaseDN)
	directory.UserBaseDN = strings.TrimSpace(directory.UserBaseDN)
	directory.GroupBaseDN = strings.TrimSpace(directory.GroupBaseDN)
	directory.AllowedLoginFilter = strings.TrimSpace(directory.AllowedLoginFilter)
	directory.ServiceAccountDN = strings.TrimSpace(directory.ServiceAccountDN)
	if directory.Name == "" || directory.BaseDN == "" || directory.ServiceAccountDN == "" {
		return fmt.Errorf("%w: name, base_dn and service_account_dn are required", ErrInvalidDirectoryConfig)
	}
	if !directory.Protocol.IsValid() {
		return fmt.Errorf("%w: unsupported protocol %q", ErrInvalidDirectoryConfig, directory.Protocol)
	}
	if !directory.TLSMode.IsValid() {
		return fmt.Errorf("%w: TLS mode must be ldaps or starttls", ErrInvalidDirectoryConfig)
	}
	if len(directory.ServerURLs) == 0 {
		return fmt.Errorf("%w: at least one server URL is required", ErrInvalidDirectoryConfig)
	}
	if len(directory.ServerNames) != 0 && len(directory.ServerNames) != len(directory.ServerURLs) {
		return fmt.Errorf("%w: server_names must be empty or parallel server_urls", ErrInvalidDirectoryConfig)
	}
	for i := range directory.ServerNames {
		directory.ServerNames[i] = strings.TrimSpace(directory.ServerNames[i])
	}
	for i, raw := range directory.ServerURLs {
		serverURL := strings.TrimSpace(raw)
		if serverURL == "" {
			return fmt.Errorf("%w: server URL %d is empty", ErrInvalidDirectoryConfig, i)
		}
		if directory.TLSMode == types.DirectoryTLSLDAPS && !strings.HasPrefix(strings.ToLower(serverURL), "ldaps://") {
			return fmt.Errorf("%w: LDAPS server URLs must use ldaps://", ErrInvalidDirectoryConfig)
		}
		if directory.TLSMode == types.DirectoryTLSStartTLS &&
			!strings.HasPrefix(strings.ToLower(serverURL), "ldap://") {
			return fmt.Errorf("%w: StartTLS server URLs must use ldap://", ErrInvalidDirectoryConfig)
		}
		directory.ServerURLs[i] = serverURL
	}
	if directory.UserBaseDN == "" {
		directory.UserBaseDN = directory.BaseDN
	}
	if directory.GroupBaseDN == "" {
		directory.GroupBaseDN = directory.BaseDN
	}
	if directory.ConnectTimeoutSeconds <= 0 {
		directory.ConnectTimeoutSeconds = 5
	}
	if directory.QueryTimeoutSeconds <= 0 {
		directory.QueryTimeoutSeconds = 10
	}
	if directory.PageSize <= 0 {
		directory.PageSize = 500
	}
	if directory.ResultLimit <= 0 {
		directory.ResultLimit = 10000
	}
	if directory.SyncIntervalSeconds <= 0 {
		directory.SyncIntervalSeconds = 300
	}
	if directory.StaleAfterSeconds <= 0 {
		directory.StaleAfterSeconds = 900
	}
	if directory.ConfigSource == "" {
		directory.ConfigSource = types.DirectoryConfigSourceUI
	}
	if directory.ConfigSource == "ui" {
		directory.ConfigSource = types.DirectoryConfigSourceDatabase
	}
	if directory.ConfigSource != types.DirectoryConfigSourceDatabase &&
		directory.ConfigSource != types.DirectoryConfigSourceFile {
		return fmt.Errorf("%w: config_source must be database or file", ErrInvalidDirectoryConfig)
	}
	return nil
}

func (s *directoryService) Get(ctx context.Context, id string) (*types.Directory, error) {
	return s.repo.Get(ctx, id)
}

func (s *directoryService) List(ctx context.Context) ([]*types.Directory, error) {
	return s.repo.List(ctx)
}

func (s *directoryService) Delete(ctx context.Context, id string) error {
	directory, err := s.repo.Get(ctx, id)
	if err != nil {
		return err
	}
	if directory != nil && directory.IsFileManaged() {
		return ErrDirectoryFileManaged
	}
	return s.repo.Delete(ctx, id)
}

func (s *directoryService) LinkIdentity(ctx context.Context, identityID, userID string) error {
	if strings.TrimSpace(identityID) == "" || strings.TrimSpace(userID) == "" {
		return fmt.Errorf("%w: identity_id and user_id are required", ErrDirectorySnapshotInvalid)
	}
	if err := s.repo.LinkIdentity(ctx, identityID, userID); err != nil {
		if errors.Is(err, apprepo.ErrDirectoryIdentityLinkConflict) {
			return ErrDirectoryIdentityAlreadyLinked
		}
		return err
	}
	return nil
}

func (s *directoryService) UnlinkIdentity(ctx context.Context, identityID string) error {
	if strings.TrimSpace(identityID) == "" {
		return fmt.Errorf("%w: identity_id is required", ErrDirectorySnapshotInvalid)
	}
	return s.repo.UnlinkIdentity(ctx, identityID)
}

func (s *directoryService) ApplySnapshot(
	ctx context.Context,
	snapshot *types.DirectorySnapshot,
) (*types.DirectorySnapshotResult, error) {
	if snapshot == nil || strings.TrimSpace(snapshot.DirectoryID) == "" {
		return nil, fmt.Errorf("%w: directory id is required", ErrDirectorySnapshotInvalid)
	}
	if !s.startSync(snapshot.DirectoryID) {
		return nil, ErrDirectorySyncInProgress
	}
	defer s.finishSync(snapshot.DirectoryID)
	return s.applySnapshotUnlocked(ctx, snapshot)
}

// RunSync holds the per-directory mutex across network collection as well as
// validation/apply. Adapters should use this entry point; ApplySnapshot stays
// available for tests and callers that already collected a snapshot.
func (s *directoryService) RunSync(
	ctx context.Context,
	directoryID string,
	fetch func(context.Context) (*types.DirectorySnapshot, error),
) (*types.DirectorySnapshotResult, error) {
	directoryID = strings.TrimSpace(directoryID)
	if directoryID == "" || fetch == nil {
		return nil, fmt.Errorf("%w: directory id and fetch callback are required", ErrDirectorySnapshotInvalid)
	}
	if !s.startSync(directoryID) {
		return nil, ErrDirectorySyncInProgress
	}
	defer s.finishSync(directoryID)

	leaseOwner := uuid.NewString()
	acquired, err := s.repo.TryAcquireSyncLease(ctx, directoryID, leaseOwner, directorySyncLeaseDuration)
	if err != nil {
		return nil, fmt.Errorf("acquire directory sync lease: %w", err)
	}
	if !acquired {
		return nil, ErrDirectorySyncInProgress
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), directorySyncLeaseRelease)
		defer cancel()
		_ = s.repo.ReleaseSyncLease(releaseCtx, directoryID, leaseOwner)
	}()

	leaseCtx, cancelLease := context.WithCancel(ctx)
	leaseLost := make(chan error, 1)
	stopHeartbeat := make(chan struct{})
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(directorySyncLeaseHeartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-leaseCtx.Done():
				return
			case <-stopHeartbeat:
				return
			case <-ticker.C:
				renewed, renewErr := s.repo.RenewSyncLease(
					leaseCtx,
					directoryID,
					leaseOwner,
					directorySyncLeaseDuration,
				)
				if renewErr != nil {
					select {
					case leaseLost <- fmt.Errorf("%w: renew failed: %v", ErrDirectorySyncLeaseLost, renewErr):
					default:
					}
					cancelLease()
					return
				}
				if !renewed {
					select {
					case leaseLost <- ErrDirectorySyncLeaseLost:
					default:
					}
					cancelLease()
					return
				}
			}
		}
	}()
	defer func() {
		close(stopHeartbeat)
		cancelLease()
		<-heartbeatDone
	}()

	snapshot, err := fetch(leaseCtx)
	if err != nil {
		select {
		case leaseErr := <-leaseLost:
			return nil, leaseErr
		default:
		}
		return nil, err
	}
	select {
	case leaseErr := <-leaseLost:
		return nil, leaseErr
	default:
	}
	if snapshot == nil {
		return nil, fmt.Errorf("%w: fetch returned nil snapshot", ErrDirectorySnapshotInvalid)
	}
	if snapshot.DirectoryID == "" {
		snapshot.DirectoryID = directoryID
	}
	if snapshot.DirectoryID != directoryID {
		return nil, fmt.Errorf(
			"%w: fetched directory %q does not match %q",
			ErrDirectorySnapshotInvalid,
			snapshot.DirectoryID,
			directoryID,
		)
	}
	result, err := s.applySnapshotUnlocked(leaseCtx, snapshot)
	if err != nil {
		return nil, err
	}
	select {
	case leaseErr := <-leaseLost:
		return nil, leaseErr
	default:
	}
	return result, nil
}

func (s *directoryService) applySnapshotUnlocked(
	ctx context.Context,
	snapshot *types.DirectorySnapshot,
) (*types.DirectorySnapshotResult, error) {
	if snapshot.Trigger == "" {
		snapshot.Trigger = types.DirectorySyncTriggerManual
	}
	if snapshot.Trigger != types.DirectorySyncTriggerManual &&
		snapshot.Trigger != types.DirectorySyncTriggerScheduled &&
		snapshot.Trigger != types.DirectorySyncTriggerLogin {
		return nil, fmt.Errorf("%w: invalid sync trigger %q", ErrDirectorySnapshotInvalid, snapshot.Trigger)
	}
	if !snapshot.Complete || !snapshot.PaginationComplete {
		return nil, ErrIncompleteDirectorySync
	}
	if len(snapshot.Anomalies) > 0 {
		return nil, fmt.Errorf("%w: %s", ErrDirectorySnapshotInvalid, strings.Join(snapshot.Anomalies, "; "))
	}
	directory, err := s.repo.Get(ctx, snapshot.DirectoryID)
	if err != nil {
		return nil, err
	}
	if directory == nil {
		return nil, fmt.Errorf("directory %q not found", snapshot.DirectoryID)
	}
	if snapshot.ExpectedConfigVersion == 0 {
		snapshot.ExpectedConfigVersion = directory.ConfigVersion
	}
	if directory.ResultLimit > 0 &&
		(len(snapshot.Identities) > directory.ResultLimit || len(snapshot.Groups) > directory.ResultLimit) {
		return nil, fmt.Errorf("%w: result limit %d exceeded", ErrDirectorySnapshotInvalid, directory.ResultLimit)
	}
	normalized, err := normalizeDirectorySnapshot(snapshot)
	if err != nil {
		return nil, err
	}
	result, err := s.repo.ApplySnapshot(ctx, normalized)
	if err != nil {
		return nil, err
	}
	if s.invalidator != nil {
		for tenantID, version := range result.AffectedTenants {
			_ = s.invalidator.InvalidateTenantPermissions(ctx, tenantID, version)
		}
	}
	return result, nil
}

func (s *directoryService) startSync(directoryID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.running[directoryID]; exists {
		return false
	}
	s.running[directoryID] = struct{}{}
	return true
}

func (s *directoryService) finishSync(directoryID string) {
	s.mu.Lock()
	delete(s.running, directoryID)
	s.mu.Unlock()
}

func normalizeDirectorySnapshot(input *types.DirectorySnapshot) (*types.DirectorySnapshot, error) {
	result := *input
	result.Identities = append([]types.DirectoryIdentitySnapshot(nil), input.Identities...)
	result.Groups = append([]types.DirectoryGroupSnapshot(nil), input.Groups...)
	result.GroupEdges = append([]types.DirectoryGroupEdgeSnapshot(nil), input.GroupEdges...)

	identities := make(map[string]types.DirectoryIdentitySnapshot, len(input.Identities))
	for _, identity := range input.Identities {
		identity.ObjectGUID = strings.TrimSpace(identity.ObjectGUID)
		if identity.ObjectGUID == "" {
			return nil, fmt.Errorf("%w: identity has empty objectGUID", ErrDirectorySnapshotInvalid)
		}
		if _, exists := identities[identity.ObjectGUID]; exists {
			return nil, fmt.Errorf(
				"%w: duplicate identity objectGUID %q",
				ErrDirectorySnapshotInvalid,
				identity.ObjectGUID,
			)
		}
		identities[identity.ObjectGUID] = identity
	}
	groups := make(map[string]types.DirectoryGroupSnapshot, len(input.Groups))
	groupBySID := make(map[string]string, len(input.Groups))
	for _, group := range input.Groups {
		group.ObjectGUID = strings.TrimSpace(group.ObjectGUID)
		if group.ObjectGUID == "" {
			return nil, fmt.Errorf("%w: group has empty objectGUID", ErrDirectorySnapshotInvalid)
		}
		if _, exists := groups[group.ObjectGUID]; exists {
			return nil, fmt.Errorf("%w: duplicate group objectGUID %q", ErrDirectorySnapshotInvalid, group.ObjectGUID)
		}
		groups[group.ObjectGUID] = group
		if group.ObjectSID != "" {
			groupBySID[group.ObjectSID] = group.ObjectGUID
		}
	}

	parents := make(map[string][]string, len(groups))
	children := make(map[string][]string, len(groups))
	edgeSeen := make(map[string]struct{}, len(input.GroupEdges))
	for _, edge := range input.GroupEdges {
		if _, ok := groups[edge.ParentGroupObjectGUID]; !ok {
			return nil, fmt.Errorf(
				"%w: unknown parent group %q",
				ErrDirectorySnapshotInvalid,
				edge.ParentGroupObjectGUID,
			)
		}
		if _, ok := groups[edge.ChildGroupObjectGUID]; !ok {
			return nil, fmt.Errorf("%w: unknown child group %q", ErrDirectorySnapshotInvalid, edge.ChildGroupObjectGUID)
		}
		key := edge.ParentGroupObjectGUID + "\x00" + edge.ChildGroupObjectGUID
		if _, duplicate := edgeSeen[key]; duplicate {
			continue
		}
		edgeSeen[key] = struct{}{}
		parents[edge.ChildGroupObjectGUID] = append(parents[edge.ChildGroupObjectGUID], edge.ParentGroupObjectGUID)
		children[edge.ParentGroupObjectGUID] = append(children[edge.ParentGroupObjectGUID], edge.ChildGroupObjectGUID)
	}
	if hasDirectoryGroupCycle(groups, children) {
		return nil, ErrDirectoryGroupCycle
	}

	type membershipKey struct{ user, group string }
	memberships := make(map[membershipKey]types.DirectoryMembershipSnapshot)
	addDirect := func(userGUID, groupGUID string, primary bool) error {
		if _, ok := identities[userGUID]; !ok {
			return fmt.Errorf("%w: membership references unknown identity %q", ErrDirectorySnapshotInvalid, userGUID)
		}
		if _, ok := groups[groupGUID]; !ok {
			return fmt.Errorf("%w: membership references unknown group %q", ErrDirectorySnapshotInvalid, groupGUID)
		}
		source := types.DirectoryMembershipDirect
		if primary {
			source = types.DirectoryMembershipPrimary
		}
		key := membershipKey{user: userGUID, group: groupGUID}
		if old, exists := memberships[key]; exists {
			old.Direct = true
			old.Primary = old.Primary || primary
			if old.Primary {
				old.Source = types.DirectoryMembershipPrimary
			}
			memberships[key] = old
			return nil
		}
		memberships[key] = types.DirectoryMembershipSnapshot{
			GroupObjectGUID: groupGUID, UserObjectGUID: userGUID, Direct: true,
			Primary: primary, Depth: 0, Source: source,
		}
		return nil
	}
	for _, member := range input.Memberships {
		if err := addDirect(member.UserObjectGUID, member.GroupObjectGUID, member.Primary); err != nil {
			return nil, err
		}
	}
	for userGUID, identity := range identities {
		if identity.PrimaryGroupSID == "" {
			continue
		}
		if groupGUID, ok := groupBySID[identity.PrimaryGroupSID]; ok {
			if err := addDirect(userGUID, groupGUID, true); err != nil {
				return nil, err
			}
		}
	}

	// Walk upward from every direct membership. A breadth-first traversal
	// records the shortest provenance depth when diamonds occur.
	base := make([]types.DirectoryMembershipSnapshot, 0, len(memberships))
	for _, member := range memberships {
		base = append(base, member)
	}
	for _, member := range base {
		type step struct {
			group string
			depth int
		}
		queue := []step{{group: member.GroupObjectGUID, depth: 0}}
		seen := map[string]bool{member.GroupObjectGUID: true}
		for len(queue) > 0 {
			current := queue[0]
			queue = queue[1:]
			for _, parent := range parents[current.group] {
				if seen[parent] {
					continue
				}
				seen[parent] = true
				depth := current.depth + 1
				key := membershipKey{user: member.UserObjectGUID, group: parent}
				if old, exists := memberships[key]; !exists || (!old.Direct && depth < old.Depth) {
					memberships[key] = types.DirectoryMembershipSnapshot{
						GroupObjectGUID: parent, UserObjectGUID: member.UserObjectGUID,
						Depth: depth, Source: types.DirectoryMembershipNested,
					}
				}
				queue = append(queue, step{group: parent, depth: depth})
			}
		}
	}
	result.Memberships = make([]types.DirectoryMembershipSnapshot, 0, len(memberships))
	for _, member := range memberships {
		result.Memberships = append(result.Memberships, member)
	}
	sort.Slice(result.Memberships, func(i, j int) bool {
		if result.Memberships[i].UserObjectGUID != result.Memberships[j].UserObjectGUID {
			return result.Memberships[i].UserObjectGUID < result.Memberships[j].UserObjectGUID
		}
		if result.Memberships[i].GroupObjectGUID != result.Memberships[j].GroupObjectGUID {
			return result.Memberships[i].GroupObjectGUID < result.Memberships[j].GroupObjectGUID
		}
		return result.Memberships[i].Depth < result.Memberships[j].Depth
	})
	return &result, nil
}

func hasDirectoryGroupCycle(groups map[string]types.DirectoryGroupSnapshot, children map[string][]string) bool {
	const (
		unseen = iota
		visiting
		done
	)
	state := make(map[string]int, len(groups))
	var visit func(string) bool
	visit = func(group string) bool {
		switch state[group] {
		case visiting:
			return true
		case done:
			return false
		}
		state[group] = visiting
		for _, child := range children[group] {
			if visit(child) {
				return true
			}
		}
		state[group] = done
		return false
	}
	for group := range groups {
		if visit(group) {
			return true
		}
	}
	return false
}

func (s *directoryService) RecordSyncFailure(
	ctx context.Context,
	directoryID, code, message string,
	startedAt time.Time,
	expectedSnapshotVersion, expectedConfigVersion uint64,
	trigger types.DirectorySyncTrigger,
) (*types.DirectorySyncRun, error) {
	code = strings.TrimSpace(code)
	message = strings.TrimSpace(message)
	if len(message) > 2000 {
		message = message[:2000]
	}
	return s.repo.RecordSyncFailure(
		ctx,
		directoryID,
		code,
		message,
		startedAt,
		expectedSnapshotVersion,
		expectedConfigVersion,
		trigger,
	)
}

func (s *directoryService) ListSyncRuns(
	ctx context.Context,
	directoryID string,
	offset, limit int,
) ([]*types.DirectorySyncRun, error) {
	return s.repo.ListSyncRuns(ctx, directoryID, offset, limit)
}
