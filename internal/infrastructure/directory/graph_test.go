package directory

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestComputeEffectiveMembershipsNestedAndPrimary(t *testing.T) {
	effective, err := ComputeEffectiveMemberships(
		[]string{"user-1"},
		[]string{"direct", "parent", "primary"},
		[]UserGroupMembership{
			{UserGUID: "user-1", GroupGUID: "direct", Source: MembershipDirect},
			{UserGUID: "user-1", GroupGUID: "primary", Source: MembershipPrimary},
		},
		[]GroupMembership{{MemberGroupGUID: "direct", ParentGroupGUID: "parent"}},
	)
	require.NoError(t, err)
	require.Equal(t, []EffectiveMembership{
		{
			UserGUID: "user-1", GroupGUID: "direct", Source: MembershipDirect,
			OriginSource: MembershipDirect, OriginGroupGUID: "direct", Depth: 0,
		},
		{
			UserGUID: "user-1", GroupGUID: "parent", Source: MembershipNested,
			OriginSource: MembershipDirect, OriginGroupGUID: "direct", Depth: 1,
		},
		{
			UserGUID: "user-1", GroupGUID: "primary", Source: MembershipPrimary,
			OriginSource: MembershipPrimary, OriginGroupGUID: "primary", Depth: 0,
		},
	}, effective)
}

func TestComputeEffectiveMembershipsKeepsDistinctOriginsAndShortestPath(t *testing.T) {
	effective, err := ComputeEffectiveMemberships(
		[]string{"u"},
		[]string{"a", "b", "c", "d"},
		[]UserGroupMembership{
			{UserGUID: "u", GroupGUID: "a", Source: MembershipDirect},
			{UserGUID: "u", GroupGUID: "b", Source: MembershipDirect},
		},
		[]GroupMembership{
			{MemberGroupGUID: "a", ParentGroupGUID: "c"},
			{MemberGroupGUID: "a", ParentGroupGUID: "d"},
			{MemberGroupGUID: "d", ParentGroupGUID: "c"},
			{MemberGroupGUID: "b", ParentGroupGUID: "c"},
		},
	)
	require.NoError(t, err)
	var cMemberships []EffectiveMembership
	for _, membership := range effective {
		if membership.GroupGUID == "c" {
			cMemberships = append(cMemberships, membership)
		}
	}
	require.Len(t, cMemberships, 2)
	require.Equal(t, 1, cMemberships[0].Depth)
	require.Equal(t, 1, cMemberships[1].Depth)
}

func TestComputeEffectiveMembershipsRejectsEveryCycle(t *testing.T) {
	tests := []struct {
		name  string
		edges []GroupMembership
	}{
		{"self", []GroupMembership{{MemberGroupGUID: "a", ParentGroupGUID: "a"}}},
		{"unreferenced by user", []GroupMembership{
			{MemberGroupGUID: "a", ParentGroupGUID: "b"},
			{MemberGroupGUID: "b", ParentGroupGUID: "a"},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ComputeEffectiveMemberships([]string{"u"}, []string{"a", "b"}, nil, test.edges)
			require.ErrorIs(t, err, ErrMembershipCycle)
		})
	}
}

func TestComputeEffectiveMembershipsRejectsAbnormalGraph(t *testing.T) {
	tests := []struct {
		name   string
		users  []string
		groups []string
		seeds  []UserGroupMembership
		edges  []GroupMembership
	}{
		{"duplicate IDs", []string{"u", "u"}, []string{"g"}, nil, nil},
		{
			"unknown user",
			[]string{"u"},
			[]string{"g"},
			[]UserGroupMembership{{UserGUID: "missing", GroupGUID: "g", Source: MembershipDirect}},
			nil,
		},
		{
			"unknown group",
			[]string{"u"},
			[]string{"g"},
			nil,
			[]GroupMembership{{MemberGroupGUID: "g", ParentGroupGUID: "missing"}},
		},
		{"duplicate seed", []string{"u"}, []string{"g"}, []UserGroupMembership{
			{UserGUID: "u", GroupGUID: "g", Source: MembershipDirect},
			{UserGUID: "u", GroupGUID: "g", Source: MembershipDirect},
		}, nil},
		{"duplicate edge", []string{"u"}, []string{"a", "b"}, nil, []GroupMembership{
			{MemberGroupGUID: "a", ParentGroupGUID: "b"},
			{MemberGroupGUID: "a", ParentGroupGUID: "b"},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ComputeEffectiveMemberships(test.users, test.groups, test.seeds, test.edges)
			require.True(t, errors.Is(err, ErrInvalidMembershipGraph) || errors.Is(err, ErrMembershipCycle))
		})
	}
}
