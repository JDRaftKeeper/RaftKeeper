import os
import sys

from generate_test_report import generate_report
from github_utils import comment_on_pr

# Report to PR comment
def report(report_dir, report_type):
    content = None
    title = None

    if report_type == 'unit':
        title = f"Unit test report"
    elif report_type == 'integration':
        title = f"Integration test report"
    else:
        raise ValueError('Integration test report not available')

    content = generate_report(report_dir, title)
    if content is not None:
        comment_on_pr(content, title)

if __name__ == "__main__":
    report(sys.argv[1], sys.argv[2])
