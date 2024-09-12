import xml.etree.ElementTree as ET

import os


def generate_report(report_dir, report_title):
    print("report_dir:", report_dir)
    test_cases = {}

    # Iterate over all files in the directory
    for report_file in os.listdir(report_dir):
        print("report_file:", report_file)
        if report_file.endswith(('-none.xml', '-tsan.xml', '-msan.xml', '-asan.xml', '-ubsan.xml')):
            sanitize_type = report_file.split('-')[-1].split('.')[0]
            with open(os.path.join(report_dir, report_file), 'r') as file:
                xml_data = file.read()
            print("xml_data:", xml_data)
            root = ET.fromstring(xml_data)

            for testsuite in root.findall('testsuite'):
                for testcase in testsuite.findall('testcase'):
                    # integration test classname is like test_auto.test, we only need the first part
                    classname = testcase.get('classname').split('.')[0]
                    name = testcase.get('name')
                    failure = testcase.find('failure')
                    status = '❌' if failure is not None else '✅'
                    error_message = failure.get('message').replace('\n', '<br>') if failure is not None else ''

                    print(f"test case: {classname} {name} {failure} {error_message} {status}")
                    if (classname, name) not in test_cases:
                        test_cases[(classname, name)] = []

                    test_cases[(classname, name)].append((sanitize_type, status, error_message))

    header = "| Classname     | Name                                   | Sanitize Type | Status | Error Message |\n"
    header += "|---------------|----------------------------------------|---------------|--------|---------------|\n"

    successful_tests = []
    failed_tests = []

    for (classname, name), results in test_cases.items():
        # If all of test case are passed or not passed, squash them into one row and set sanitize_type to '-'
        statuses = {status for _, status, _ in results}
        if len(statuses) == 1:
            status = statuses.pop()
            # If there's an error message, use it; otherwise, set it to an empty string
            error_message = next((error for _, _, error in results if error), '')
            row = f"| {classname} | {name} |   | {status} | {error_message} |"
            if error_message != '':
                failed_tests.append(row)
            else:
                successful_tests.append(row)
        else:
            # Process normally if not squashed
            for sanitize_type, status, error_message in results:
                row = f"| {classname} | {name} | {sanitize_type} | {status} | {error_message} |"
                if error_message != '':
                    failed_tests.append(row)
                else:
                    successful_tests.append(row)

    failed_table = header + "\n".join(failed_tests) if failed_tests else "All test cases passed!"
    successful_table = header + "\n".join(successful_tests) if successful_tests else ""

    collapsible_successful_table = (
            "<details>\n"
            "<summary>Successful Test Cases</summary>\n\n"
            + successful_table +
            "\n</details>"
    )

    commit_id = os.environ['GITHUB_SHA']
    print("commit_id:", commit_id)

    report = f"{report_title} for commit {commit_id}.\n\n{failed_table}\n\n{collapsible_successful_table}"
    return report
