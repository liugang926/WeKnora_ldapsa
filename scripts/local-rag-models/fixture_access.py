"""Resolve synthetic benchmark personas; this does not implement app authorization."""

from __future__ import annotations


def persona_scopes(manifest: dict) -> dict[str, list[str]]:
    access = manifest.get("access_model")
    if access is None:
        return manifest["personas"]

    groups = access["groups"]
    assignments = access["persona_groups"]
    known_scopes = {document["scope"] for document in manifest["documents"]}
    visiting: set[str] = set()
    resolved: dict[str, set[str]] = {}

    def inherited(group_name: str) -> set[str]:
        if group_name not in groups:
            raise ValueError(f"unknown synthetic group: {group_name}")
        if group_name in visiting:
            raise ValueError(f"cycle in synthetic group hierarchy: {group_name}")
        if group_name in resolved:
            return resolved[group_name]
        visiting.add(group_name)
        group = groups[group_name]
        scopes = set(group.get("scopes", []))
        if not scopes.issubset(known_scopes):
            raise ValueError(f"unknown synthetic scope in group: {group_name}")
        for parent in group.get("parent_groups", []):
            scopes.update(inherited(parent))
        visiting.remove(group_name)
        resolved[group_name] = scopes
        return scopes

    for group_name in groups:
        inherited(group_name)
    if "employee" not in assignments:
        raise ValueError("synthetic access model needs employee persona")
    result: dict[str, list[str]] = {}
    for persona, direct_groups in assignments.items():
        scopes = {"all"}
        for group_name in direct_groups:
            scopes.update(inherited(group_name))
        result[persona] = sorted(scopes)
    return result
