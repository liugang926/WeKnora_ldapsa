/** Accept both the project's standard `{ success, data }` envelope and a direct payload. */
export function unwrapDirectoryAccessResponse<T>(response: unknown): T {
  if (response && typeof response === 'object' && 'data' in response) {
    return (response as { data: T }).data
  }
  return response as T
}
