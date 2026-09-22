package directory

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseObjectGUID(t *testing.T) {
	raw := []byte{0x33, 0x22, 0x11, 0x00, 0x55, 0x44, 0x77, 0x66, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff}
	guid, err := ParseObjectGUID(raw)
	require.NoError(t, err)
	require.Equal(t, "00112233-4455-6677-8899-aabbccddeeff", guid)

	_, err = ParseObjectGUID(raw[:15])
	require.ErrorIs(t, err, ErrInvalidDirectoryObject)
}

func TestParseObjectSIDAndPrimaryGroup(t *testing.T) {
	raw := testSIDBytes(1107)
	sid, err := ParseObjectSID(raw)
	require.NoError(t, err)
	require.Equal(t, "S-1-5-21-1-2-3-1107", sid.String())
	encoded, err := sid.Bytes()
	require.NoError(t, err)
	require.Equal(t, raw, encoded)

	primary, err := PrimaryGroupSID(sid.String(), 513)
	require.NoError(t, err)
	require.Equal(t, "S-1-5-21-1-2-3-513", primary)

	_, err = ParseObjectSID(raw[:len(raw)-1])
	require.ErrorIs(t, err, ErrInvalidDirectoryObject)
	_, err = PrimaryGroupSID("not-a-sid", 513)
	require.ErrorIs(t, err, ErrInvalidDirectoryObject)
	_, err = PrimaryGroupSID("S-1-5", 0)
	require.ErrorIs(t, err, ErrInvalidDirectoryObject)
}

func TestParseSIDRejectsMalformedValues(t *testing.T) {
	for _, value := range []string{"", "S-x-5-1", "S-1-x-1", "S-1-5-x"} {
		_, err := ParseSID(value)
		require.True(t, errors.Is(err, ErrInvalidDirectoryObject), value)
	}
}

func testSIDBytes(rid uint32) []byte {
	// S-1-5-21-1-2-RID
	raw := make([]byte, 8+5*4)
	raw[0] = 1
	raw[1] = 5
	raw[7] = 5
	for i, value := range []uint32{21, 1, 2, 3, rid} {
		binary.LittleEndian.PutUint32(raw[8+i*4:], value)
	}
	return raw
}
