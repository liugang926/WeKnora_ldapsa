import unittest

from release_gate import evaluate

COMMIT = 'a' * 40
APP_IMAGE = '.github/workflows/app-image-pr.yml'


def run(number, path, status='completed', conclusion='success',
        commit=COMMIT, event='push', branch='main'):
    return {
        'id': number,
        'name': 'workflow-' + str(number),
        'path': path,
        'status': status,
        'conclusion': conclusion,
        'head_sha': commit,
        'event': event,
        'head_branch': branch,
    }


class ReleaseGateTests(unittest.TestCase):
    def test_waits_for_always_on_image_verification(self):
        self.assertEqual(evaluate([], '1', COMMIT), 'waiting-for-sentinel')

    def test_waits_until_every_exact_commit_run_finishes(self):
        runs = [
            run(2, APP_IMAGE),
            run(3, '.github/workflows/app.yml', status='in_progress',
                conclusion=None),
        ]
        self.assertEqual(evaluate(runs, '1', COMMIT), 'waiting-for-tests')

    def test_all_successful_runs_are_ready(self):
        runs = [
            run(2, APP_IMAGE),
            run(3, '.github/workflows/app.yml'),
            run(4, '.github/workflows/frontend.yml'),
        ]
        self.assertEqual(evaluate(runs, '1', COMMIT), 'ready')

    def test_any_failed_or_cancelled_run_blocks_release(self):
        for conclusion in ('failure', 'cancelled', 'timed_out'):
            runs = [
                run(2, APP_IMAGE),
                run(3, '.github/workflows/app.yml', conclusion=conclusion),
            ]
            with self.subTest(conclusion=conclusion), self.assertRaises(RuntimeError):
                evaluate(runs, '1', COMMIT)

    def test_self_and_unrelated_runs_are_ignored(self):
        runs = [
            run(1, '.github/workflows/production-release.yml',
                status='in_progress', conclusion=None),
            run(2, APP_IMAGE),
            run(3, '.github/workflows/app.yml', commit='b' * 40),
            run(4, '.github/workflows/frontend.yml', event='schedule'),
            run(5, '.github/workflows/ldap-directory.yml', branch='feature'),
        ]
        self.assertEqual(evaluate(runs, '1', COMMIT), 'ready')


if __name__ == '__main__':
    unittest.main()
