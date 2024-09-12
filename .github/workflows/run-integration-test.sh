#!/bin/bash

# ${github.workspace}/tests/integration
tests_root_dir=$1
cd "$tests_root_dir"
shift 1

test_result="succeed"

# shellcheck disable=SC2120
function run_tests()
{
  echo "tests_root_dir: ${tests_root_dir}"
  ./runner --binary ../../build/programs/raftkeeper  \
                 --base-configs-dir ../../programs/server \
                 " $@" \
                 | tee /tmp/tests_output.log

  if [ ${PIPESTATUS[0]} -ne 0 ]; then
    test_result="failed"
    failed_test_cases=($(grep -E '^test.*(FAILED|ERROR)' /tmp/tests_output.log | grep '/' | awk -F'/' '{print $1}' | sort | uniq))
    for failed_test_case in ${failed_test_cases[*]}
    do
      echo $failed_test_case >> /tmp/$failed_test_cases.log
      raftkeeper_instances=$(ls "$failed_test_case"/_instances | grep node)
      for raftkeeper_instance in ${raftkeeper_instances[*]}
      do
        echo -e "\n----------------- Captured $failed_test_case $raftkeeper_instance raftkeeper-server.log -----------------"
        sudo cat "$failed_test_case"/_instances/"$raftkeeper_instance"/logs/raftkeeper-server.log
        echo -e "\n----------------- Captured $failed_test_case $raftkeeper_instance stderr.log -----------------"
        sudo cat "$failed_test_case"/_instances/"$raftkeeper_instance"/logs/stderr.log
      done
    done
  fi
}


run_tests "$@"

if [ $test_result == "failed" ]; then
    exit 1;
fi
