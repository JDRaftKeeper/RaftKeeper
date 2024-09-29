import os
import sys

from generate_test_report import generate_report
from github_utils import comment_on_pr

# Report to PR comment
def report(report_dir, report_type):
    content = None
    title_prefix = None

    if report_type == 'unit':
        title_prefix = f"Unit test report"
    elif report_type == 'integration':
        title_prefix = f"Integration test report"
    else:
        raise ValueError('Integration test report not available')

    content = generate_report(report_dir, title_prefix)
    if content is not None:
        comment_on_pr(content, title_prefix)


if __name__ == "__main__":
    report(sys.argv[1], sys.argv[2])
