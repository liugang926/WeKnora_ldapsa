// The editor needs KB details to render its Data Sources section. The
// ordinary knowledge list can reject a stale published Nextcloud document
// while its current replacement is failed. Treat a failed count probe as
// "files exist" so any file-sensitive edits remain locked.
export async function loadKBEditorReadiness<T>(
  details: Promise<T>,
  files: Promise<{ total?: number }>,
): Promise<{ kbInfo: T; hasKnowledgeFiles: boolean }> {
  const [kbInfo, hasKnowledgeFiles] = await Promise.all([
    details,
    files.then(page => typeof page?.total === 'number' &&
      Number.isFinite(page.total) && page.total >= 0 ? page.total > 0 : true,
    ).catch(() => true),
  ])
  return { kbInfo, hasKnowledgeFiles }
}
