// List and selected-file status requests have independent lifetimes. Loading
// another page must not invalidate an in-flight exact status GET, or vice versa.
export class FailedCandidateRequestGate {
  private listRevision = 0
  private detailRevision = 0
  private contextRevision = 0

  beginList(reset: boolean) {
    if (reset) this.detailRevision++
    return ++this.listRevision
  }

  beginDetail() { return ++this.detailRevision }
  isCurrentList(revision: number) { return revision === this.listRevision }
  isCurrentDetail(revision: number) { return revision === this.detailRevision }
  currentContext() { return this.contextRevision }
  isCurrentContext(revision: number) { return revision === this.contextRevision }

  invalidate() {
    this.contextRevision++
    this.listRevision++
    this.detailRevision++
  }
}
