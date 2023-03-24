#!/bin/bash

# ${github.workspace}/tests/integration
tests_root_dir=$1
cd "$tests_root_dir"

test_result="succeed"

function run_tests()
{
  ./runner --binary "${tests_root_dir}"/../../build/programs/raftkeeper  \
                 --base-configs-dir "${tests_root_dir}"/../../programs/server \
                 | tee /tmp/tests_output.log

  if [ ${PIPESTATUS[0]} -ne 0 ]; then
    test_result="failed"
    failed_test_cases=($(grep -E '^test.*(FAILED|ERROR)' /tmp/tests_output.log | grep '/' | awk -F'/' '{print $1}' | sort | uniq))
    for failed_test_case in ${failed_test_cases[*]}
    do
      raftkeeper_instances=$(ls "$failed_test_case"/_instances | grep node)
      for raftkeeper_instance in ${raftkeeper_instances[*]}
      do
        echo -e "\n----------------- Captured $failed_test_case $raftkeeper_instance raftkeeper-server.log -----------------"
        sudo cat "$failed_test_case"/_instances/"$raftkeeper_instance"/logs/raftkeeper-server.log
      done
    done
  fi
}

function run_tests_individually()
{
  # shellcheck disable=SC2207
  test_cases=($(ls "$tests_root_dir" | grep test_))
  #test_cases=(test_multinode_simple/test.py::test_follower_restart)
  echo "Total ${#test_cases[*]} test cases to run."
  # shellcheck disable=SC1073
  for test_case in ${test_cases[*]}
  do
      echo -e "\n----------------- Run test $test_case -----------------"
      ./runner --binary "${tests_root_dir}"/../../build/programs/raftkeeper  \
               --base-configs-dir "${tests_root_dir}"/../../programs/server \
               "$test_case"
      # shellcheck disable=SC2181
      if [ $? -ne 0 ]; then
           test_result="failed"
          raftkeeper_instances=$(ls "$test_case"/_instances | grep node)
          for raftkeeper_instance in ${raftkeeper_instances[*]}
          do
              echo -e "\n----------------- Captured $test_case $raftkeeper_instance raftkeeper-server.log -----------------"
              sudo cat "$test_case"/_instances/"$raftkeeper_instance"/logs/raftkeeper-server.log
          done
      fi
  done
}


run_tests

if [ $test_result == "failed" ]; then
    exit 1;
fi
