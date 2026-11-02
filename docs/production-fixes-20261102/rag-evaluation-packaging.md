# Evaluation benchmark packaging

The runtime stage copied dataset/samples but omitted the tracked benchmark fixtures.
Copy dataset/benchmarks too. The bounded local loader remains unchanged.

For this same-binary packaging hotfix, build an image FROM the precise deployed
app image and COPY only dataset/benchmarks. Preserve WEKNORA_BUILD_COMMIT as
cbd596b48fad94e20f5a928f0ebc6e4ae80c7ac2; record the packaging Git revision in
org.weknora.packaging-revision. This avoids misrepresenting binary provenance.
No application/AnyDoc binary or runtime dependency is replaced.

Extract the candidate image dataset/benchmarks directory, compare every file hash
to this source tree, then run TestPackagedEvaluationBenchmark against that
extracted directory via EVALUATION_PACKAGED_BENCHMARK_DIR. Verify samples remain
and compare /app/WeKnora hash to the base image. Production evaluation must be
accepted separately; do not count this fixture-only test as model/QA acceptance.
