#!/bin/bash

# ${github.workspace}/tests/integration
work_dir=$1
# shellcheck disable=SC2164
cd "$work_dir"

test_cases=($(ls "$work_dir" | grep test_))
#test_cases=(test_three_nodes_two_alive/test.py)
test_result="succeed"

echo "Total ${#test_cases[*]} test cases to run."

# shellcheck disable=SC1073
for test_case in ${test_cases[*]}
do
    echo -e "\n================= Run test $test_case ================="
    ./runner --binary "${work_dir}"/../../build/programs/raftkeeper  \
             --base-configs-dir "${work_dir}"/../../programs/server \
             "$test_case"
    # shellcheck disable=SC2181
    if [ $? -ne 0 ]; then
         test_result="failed"
        # print docker log and raftkeeper instances log
        echo -e "\n================= Test $test_case failed! ================="
        raftkeeper_instances=$(ls "$test_case"/_instances | grep node)
        for raftkeeper_instance in ${raftkeeper_instances[*]}
        do
            echo -e "\n================= $test_case raftkeeper $raftkeeper_instance raftkeeper-server.log ================="
            sudo cat "$test_case"/_instances/"$raftkeeper_instance"/logs/raftkeeper-server.log
            echo -e "\n================= $test_case raftkeeper $raftkeeper_instance stderr.log ================="
            sudo cat "$test_case"/_instances/"$raftkeeper_instance"/logs/stderr.log
        done
    fi
done

if [ $test_result == "failed" ]; then
    exit 1;
fi
