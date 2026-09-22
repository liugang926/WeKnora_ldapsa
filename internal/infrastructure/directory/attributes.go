package directory

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
)

// ParseObjectGUID converts AD's mixed-endian 16-byte objectGUID into the
// canonical textual UUID representation.
func ParseObjectGUID(raw []byte) (string, error) {
	if len(raw) != 16 {
		return "", fmt.Errorf("%w: objectGUID has %d bytes, want 16", ErrInvalidDirectoryObject, len(raw))
	}
	return fmt.Sprintf(
		"%08x-%04x-%04x-%02x%02x-%02x%02x%02x%02x%02x%02x",
		binary.LittleEndian.Uint32(raw[0:4]),
		binary.LittleEndian.Uint16(raw[4:6]),
		binary.LittleEndian.Uint16(raw[6:8]),
		raw[8], raw[9], raw[10], raw[11], raw[12], raw[13], raw[14], raw[15],
	), nil
}

// SID is the structured form of an AD objectSid.
type SID struct {
	Revision       uint8
	Authority      uint64
	SubAuthorities []uint32
}

// Bytes returns the binary representation used by LDAP objectSid equality
// filters. AD encodes the identifier authority big-endian and each
// sub-authority little-endian.
func (s SID) Bytes() ([]byte, error) {
	if s.Revision != 1 || s.Authority > 0xffffffffffff || len(s.SubAuthorities) == 0 || len(s.SubAuthorities) > 15 {
		return nil, fmt.Errorf("%w: SID cannot be encoded", ErrInvalidDirectoryObject)
	}
	encoded := make([]byte, 8+4*len(s.SubAuthorities))
	encoded[0] = s.Revision
	encoded[1] = byte(len(s.SubAuthorities))
	authority := s.Authority
	for i := 7; i >= 2; i-- {
		encoded[i] = byte(authority)
		authority >>= 8
	}
	for i, subAuthority := range s.SubAuthorities {
		binary.LittleEndian.PutUint32(encoded[8+i*4:], subAuthority)
	}
	return encoded, nil
}

// ParseObjectSID decodes the binary SID format used by Active Directory.
func ParseObjectSID(raw []byte) (SID, error) {
	if len(raw) < 8 {
		return SID{}, fmt.Errorf("%w: objectSid is too short", ErrInvalidDirectoryObject)
	}
	if raw[0] != 1 {
		return SID{}, fmt.Errorf("%w: unsupported objectSid revision %d", ErrInvalidDirectoryObject, raw[0])
	}
	count := int(raw[1])
	if count == 0 || count > 15 || len(raw) != 8+4*count {
		return SID{}, fmt.Errorf("%w: invalid objectSid length/count", ErrInvalidDirectoryObject)
	}
	authority := uint64(0)
	for _, b := range raw[2:8] {
		authority = authority<<8 | uint64(b)
	}
	sid := SID{Revision: raw[0], Authority: authority, SubAuthorities: make([]uint32, count)}
	for i := 0; i < count; i++ {
		offset := 8 + i*4
		sid.SubAuthorities[i] = binary.LittleEndian.Uint32(raw[offset : offset+4])
	}
	return sid, nil
}

// ParseSID parses the canonical S-Revision-Authority-SubAuthority form.
func ParseSID(value string) (SID, error) {
	parts := strings.Split(strings.TrimSpace(value), "-")
	if len(parts) < 3 || !strings.EqualFold(parts[0], "S") {
		return SID{}, fmt.Errorf("%w: malformed SID %q", ErrInvalidDirectoryObject, value)
	}
	revision, err := strconv.ParseUint(parts[1], 10, 8)
	if err != nil || revision != 1 {
		return SID{}, fmt.Errorf("%w: malformed SID revision", ErrInvalidDirectoryObject)
	}
	authority, err := strconv.ParseUint(parts[2], 10, 48)
	if err != nil {
		return SID{}, fmt.Errorf("%w: malformed SID authority", ErrInvalidDirectoryObject)
	}
	if len(parts)-3 > 15 {
		return SID{}, fmt.Errorf("%w: too many SID sub-authorities", ErrInvalidDirectoryObject)
	}
	sid := SID{Revision: uint8(revision), Authority: authority, SubAuthorities: make([]uint32, 0, len(parts)-3)}
	for _, part := range parts[3:] {
		value, parseErr := strconv.ParseUint(part, 10, 32)
		if parseErr != nil {
			return SID{}, fmt.Errorf("%w: malformed SID sub-authority", ErrInvalidDirectoryObject)
		}
		sid.SubAuthorities = append(sid.SubAuthorities, uint32(value))
	}
	return sid, nil
}

func (s SID) String() string {
	parts := []string{"S", strconv.FormatUint(uint64(s.Revision), 10), strconv.FormatUint(s.Authority, 10)}
	for _, sub := range s.SubAuthorities {
		parts = append(parts, strconv.FormatUint(uint64(sub), 10))
	}
	return strings.Join(parts, "-")
}

// PrimaryGroupSID replaces the user's RID with primaryGroupID, yielding the
// SID of the corresponding AD primary group.
func PrimaryGroupSID(userSID string, primaryGroupID uint32) (string, error) {
	sid, err := ParseSID(userSID)
	if err != nil {
		return "", err
	}
	if primaryGroupID == 0 || len(sid.SubAuthorities) == 0 {
		return "", fmt.Errorf("%w: primary group cannot be derived", ErrInvalidDirectoryObject)
	}
	sid.SubAuthorities[len(sid.SubAuthorities)-1] = primaryGroupID
	return sid.String(), nil
}
