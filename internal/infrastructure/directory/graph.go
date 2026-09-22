package directory

import (
	"fmt"
	"sort"
	"strings"
)

// ComputeEffectiveMemberships validates the complete group graph, rejects
// cycles, then expands every direct/primary user edge through nested parents.
// Results are deterministic and preserve the seed that granted membership.
func ComputeEffectiveMemberships(
	userGUIDs []string,
	groupGUIDs []string,
	seeds []UserGroupMembership,
	nesting []GroupMembership,
) ([]EffectiveMembership, error) {
	users, err := uniqueIDs("user", userGUIDs)
	if err != nil {
		return nil, err
	}
	groups, err := uniqueIDs("group", groupGUIDs)
	if err != nil {
		return nil, err
	}

	parents := make(map[string][]string, len(groups))
	seenGroupEdges := make(map[string]struct{}, len(nesting))
	for _, edge := range nesting {
		child := strings.TrimSpace(edge.MemberGroupGUID)
		parent := strings.TrimSpace(edge.ParentGroupGUID)
		if _, ok := groups[child]; !ok {
			return nil, fmt.Errorf("%w: unknown member group %q", ErrInvalidMembershipGraph, child)
		}
		if _, ok := groups[parent]; !ok {
			return nil, fmt.Errorf("%w: unknown parent group %q", ErrInvalidMembershipGraph, parent)
		}
		key := child + "\x00" + parent
		if _, duplicate := seenGroupEdges[key]; duplicate {
			return nil, fmt.Errorf("%w: duplicate group edge %q -> %q", ErrInvalidMembershipGraph, child, parent)
		}
		seenGroupEdges[key] = struct{}{}
		parents[child] = append(parents[child], parent)
	}
	for child := range parents {
		sort.Strings(parents[child])
	}
	if err := detectMembershipCycle(groups, parents); err != nil {
		return nil, err
	}

	sortedSeeds := append([]UserGroupMembership(nil), seeds...)
	sort.Slice(sortedSeeds, func(i, j int) bool {
		if sortedSeeds[i].UserGUID != sortedSeeds[j].UserGUID {
			return sortedSeeds[i].UserGUID < sortedSeeds[j].UserGUID
		}
		if sortedSeeds[i].GroupGUID != sortedSeeds[j].GroupGUID {
			return sortedSeeds[i].GroupGUID < sortedSeeds[j].GroupGUID
		}
		return sortedSeeds[i].Source < sortedSeeds[j].Source
	})
	seenSeeds := make(map[string]struct{}, len(sortedSeeds))
	for _, seed := range sortedSeeds {
		if _, ok := users[seed.UserGUID]; !ok {
			return nil, fmt.Errorf("%w: unknown user %q", ErrInvalidMembershipGraph, seed.UserGUID)
		}
		if _, ok := groups[seed.GroupGUID]; !ok {
			return nil, fmt.Errorf("%w: unknown group %q", ErrInvalidMembershipGraph, seed.GroupGUID)
		}
		if seed.Source != MembershipDirect && seed.Source != MembershipPrimary {
			return nil, fmt.Errorf("%w: invalid seed source %q", ErrInvalidMembershipGraph, seed.Source)
		}
		key := seed.UserGUID + "\x00" + seed.GroupGUID + "\x00" + string(seed.Source)
		if _, duplicate := seenSeeds[key]; duplicate {
			return nil, fmt.Errorf("%w: duplicate user/group seed", ErrInvalidMembershipGraph)
		}
		seenSeeds[key] = struct{}{}
	}

	type queueItem struct {
		group string
		depth int
	}
	result := make([]EffectiveMembership, 0, len(sortedSeeds))
	for _, seed := range sortedSeeds {
		queue := []queueItem{{group: seed.GroupGUID, depth: 0}}
		bestDepth := map[string]int{seed.GroupGUID: 0}
		for len(queue) > 0 {
			item := queue[0]
			queue = queue[1:]
			source := MembershipNested
			if item.depth == 0 {
				source = seed.Source
			}
			result = append(result, EffectiveMembership{
				UserGUID:        seed.UserGUID,
				GroupGUID:       item.group,
				Source:          source,
				OriginSource:    seed.Source,
				OriginGroupGUID: seed.GroupGUID,
				Depth:           item.depth,
			})
			for _, parent := range parents[item.group] {
				depth := item.depth + 1
				if prior, seen := bestDepth[parent]; seen && prior <= depth {
					continue
				}
				bestDepth[parent] = depth
				queue = append(queue, queueItem{group: parent, depth: depth})
			}
		}
	}

	sort.Slice(result, func(i, j int) bool {
		a, b := result[i], result[j]
		if a.UserGUID != b.UserGUID {
			return a.UserGUID < b.UserGUID
		}
		if a.GroupGUID != b.GroupGUID {
			return a.GroupGUID < b.GroupGUID
		}
		if a.OriginGroupGUID != b.OriginGroupGUID {
			return a.OriginGroupGUID < b.OriginGroupGUID
		}
		if a.OriginSource != b.OriginSource {
			return a.OriginSource < b.OriginSource
		}
		return a.Depth < b.Depth
	})
	return result, nil
}

func uniqueIDs(kind string, ids []string) (map[string]struct{}, error) {
	out := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			return nil, fmt.Errorf("%w: empty %s ID", ErrInvalidMembershipGraph, kind)
		}
		if _, exists := out[id]; exists {
			return nil, fmt.Errorf("%w: duplicate %s ID %q", ErrInvalidMembershipGraph, kind, id)
		}
		out[id] = struct{}{}
	}
	return out, nil
}

func detectMembershipCycle(groups map[string]struct{}, parents map[string][]string) error {
	ids := make([]string, 0, len(groups))
	for id := range groups {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	state := make(map[string]uint8, len(groups))
	stack := make([]string, 0, len(groups))
	positions := make(map[string]int, len(groups))
	var visit func(string) error
	visit = func(group string) error {
		state[group] = 1
		positions[group] = len(stack)
		stack = append(stack, group)
		for _, parent := range parents[group] {
			switch state[parent] {
			case 0:
				if err := visit(parent); err != nil {
					return err
				}
			case 1:
				start := positions[parent]
				cycle := append(append([]string(nil), stack[start:]...), parent)
				return fmt.Errorf("%w: %s", ErrMembershipCycle, strings.Join(cycle, " -> "))
			}
		}
		stack = stack[:len(stack)-1]
		delete(positions, group)
		state[group] = 2
		return nil
	}
	for _, id := range ids {
		if state[id] == 0 {
			if err := visit(id); err != nil {
				return err
			}
		}
	}
	return nil
}
