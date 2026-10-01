export type ReviewVerdict = '' | 'pass' | 'fail' | 'not_applicable'
export interface ReviewInput {
  faithfulness: ReviewVerdict
  citation_accuracy: ReviewVerdict
  abstention: ReviewVerdict
}
export interface CaseReview extends ReviewInput {
  reviewed_by: string
  reviewed_at: string
}
export interface ReviewCase {
  question_id: number
  reference_answer: string
  review?: CaseReview
  review_history?: CaseReview[]
}
export interface ReviewRun {
  task: { total?: number }
  cases?: Array<ReviewCase | null>
}

const isJudged = (value: ReviewVerdict) => value === 'pass' || value === 'fail'

export function defaultReview(entry: ReviewCase): ReviewInput {
  return entry.review ? {
    faithfulness: entry.review.faithfulness,
    citation_accuracy: entry.review.citation_accuracy,
    abstention: entry.review.abstention,
  } : entry.reference_answer ? {
    faithfulness: '', citation_accuracy: '', abstention: 'not_applicable',
  } : {
    faithfulness: 'not_applicable', citation_accuracy: 'not_applicable', abstention: '',
  }
}

export function validReview(entry: ReviewCase, draft?: ReviewInput): boolean {
  return !!draft && (entry.reference_answer ?
    isJudged(draft.faithfulness) && isJudged(draft.citation_accuracy) && draft.abstention === 'not_applicable' :
    draft.faithfulness === 'not_applicable' && draft.citation_accuracy === 'not_applicable' && isJudged(draft.abstention))
}

export function manualCoverage(run: ReviewRun): string {
  return `${run.cases?.filter(entry => !!entry?.review).length ?? 0}/${run.task.total ?? run.cases?.length ?? 0}`
}

export function manualRate(run: ReviewRun, dimension: keyof ReviewInput): number | undefined {
  // Never call a partial or legacy result fully reviewed just because its
  // available subset has labels. The persisted task total is authoritative.
  if (!run.task.total || run.cases?.filter(Boolean).length !== run.task.total) return undefined
  const applicable = run.cases?.filter((entry): entry is ReviewCase => !!entry &&
    (dimension === 'abstention' ? !entry.reference_answer : !!entry.reference_answer)) || []
  if (!applicable.length || applicable.some(entry => !entry.review || !isJudged(entry.review[dimension]))) return undefined
  return applicable.filter(entry => entry.review?.[dimension] === 'pass').length / applicable.length
}
