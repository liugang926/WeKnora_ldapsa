import type { NextcloudFailedCandidateRetryStatus } from '@/api/datasource'

// Clear the in-memory generation before the POST. A 409 always reloads the
// list, so a stale ETag/candidate ID cannot remain available for another click.
export async function submitFailedCandidateRetry(
  status: NextcloudFailedCandidateRetryStatus,
  clear: () => void,
  retry: (status: NextcloudFailedCandidateRetryStatus) => Promise<unknown>,
  reload: () => Promise<unknown>,
  isCurrent: () => boolean = () => true,
): Promise<'scheduled' | 'changed'> {
  clear()
  try {
    await retry(status)
  } catch (error: any) {
    if (error?.status !== 409) throw error
    if (isCurrent()) await reload()
    return 'changed'
  }
  if (isCurrent()) await reload()
  return 'scheduled'
}
