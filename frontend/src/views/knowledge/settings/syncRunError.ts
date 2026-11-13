import type { DataSource } from '@/api/datasource'

type Translate = (key: string) => string

function isDingTalkScheduleError(message: string) {
  return message === 'dingtalk_schedule_authorization_required' ||
    message === 'dingtalk_schedule_authorization_revoked'
}

export function syncRunError(message: string, t: Translate): string {
  return isDingTalkScheduleError(message) ? t(`datasource.syncError.${message}`) : message
}

export function dataSourceError(source: DataSource, t: Translate): string {
  const log = source.latest_sync_log
  if (source.type !== 'dingtalk') return source.error_message
  if (source.status !== 'error' && isDingTalkScheduleError(source.error_message)) return ''
  if (log && source.status === 'error') {
    if (log.status === 'failed' && isDingTalkScheduleError(log.error_message)) {
      return syncRunError(log.error_message, t)
    }
    // A previous authorization error may still be present on the source.
    if (isDingTalkScheduleError(source.error_message)) return ''
  }
  return syncRunError(source.error_message, t)
}
