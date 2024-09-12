import os
import subprocess
import sys

def run_tests(tests_root_dir, *args):
    test_result = "succeed"
    os.chdir(tests_root_dir)

    result = subprocess.run(
        ["./runner", "--binary", f"{tests_root_dir}/../../build/programs/raftkeeper",
         "--base-configs-dir", f"{tests_root_dir}/../../programs/server", ' '.join(args)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )

    with open("/tmp/tests_output.log", "w") as log_file:
        log_file.write(result.stdout)

    if result.returncode != 0:
        test_result = "failed"
        failed_test_cases = subprocess.run(
            ["grep", "-E", "^test.*(FAILED|ERROR)", "/tmp/tests_output.log"],
            stdout=subprocess.PIPE, text=True
        ).stdout.splitlines()

        failed_test_cases = sorted(set(line.split('/')[0] for line in failed_test_cases if '/' in line))

        with open("/tmp/tests_report.log", "w") as report_file:
            for failed_test_case in failed_test_cases:
                report_file.write(f"{failed_test_case}\n")
                raftkeeper_instances = os.listdir(f"{failed_test_case}/_instances")
                raftkeeper_instances = [instance for instance in raftkeeper_instances if "node" in instance]

                for raftkeeper_instance in raftkeeper_instances:
                    print(f"\n----------------- Captured {failed_test_case} {raftkeeper_instance} raftkeeper-server.log -----------------")
                    with open(f"{failed_test_case}/_instances/{raftkeeper_instance}/logs/raftkeeper-server.log") as log_file:
                        print(log_file.read())
                    print(f"\n----------------- Captured {failed_test_case} {raftkeeper_instance} stderr.log -----------------")
                    with open(f"{failed_test_case}/_instances/{raftkeeper_instance}/logs/stderr.log") as log_file:
                        print(log_file.read())

    return test_result


if __name__ == "__main__":
    tests_root_dir = sys.argv[1]
    args = sys.argv[2:]
    test_result = run_tests(tests_root_dir, *args)

    if test_result == "failed":
        sys.exit(1)