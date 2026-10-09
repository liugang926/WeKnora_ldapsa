export function resolveStorageInitialSelection(
  currentID: string,
  defaultID: string,
  activeIDs: string[],
  hasFiles: boolean,
): { displayID: string; persistID: string | null } {
  const displayID = currentID || defaultID || activeIDs[0] || ''
  // A disabled picker may show a suggested default for legacy KBs, but must
  // never silently bind that default when a KB may already contain files.
  return { displayID, persistID: displayID && !hasFiles ? displayID : null }
}
