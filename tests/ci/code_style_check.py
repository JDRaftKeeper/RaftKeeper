#!/usr/bin/env/python3

import subprocess
import os
import logging
import csv
import sys
import re


def process_result(result_folder):
    test_results = []
    additional_files = []
    # Just upload all files from result_folder.
    # If task provides processed results, then it's responsible for content of result_folder.
    if os.path.exists(result_folder):
        test_files = [f for f in os.listdir(result_folder) if os.path.isfile(os.path.join(result_folder, f))]
        additional_files = [os.path.join(result_folder, f) for f in test_files]

    status = []
    status_path = os.path.join(result_folder, "check_status.tsv")
    if os.path.exists(status_path):
        logging.info("Found check_status.tsv")
        with open(status_path, 'r', encoding='utf-8') as status_file:
            status = list(csv.reader(status_file, delimiter='\t'))
    if len(status) != 1 or len(status[0]) != 2:
        logging.info("Files in result folder %s", os.listdir(result_folder))
        return "error", "Invalid check_status.tsv", test_results, additional_files
    state, description = status[0][0], status[0][1]

    try:
        results_path = os.path.join(result_folder, "test_results.tsv")
        test_results = list(csv.reader(open(results_path, 'r'), delimiter='\t'))
        if len(test_results) == 0:
            raise Exception("Empty results")

        return state, description, test_results, additional_files
    except Exception:
        if state == "success":
            state, description = "error", "Failed to read test_results.tsv"
        return state, description, test_results, additional_files

def printErrorFile(result_folder):
    style_log_path = '{}/style_output.txt'.format(result_folder)
    if not os.path.exists(style_log_path):
        logging.info("No style check log on path %s", style_log_path)
    elif os.stat(style_log_path).st_size != 0:
        print("\n\033[31mStyle [Error] **********************************************************************************************************\033[0m\n")
        printFile(style_log_path)

    typos_log_path = '{}/typos_output.txt'.format(result_folder)
    if not os.path.exists(style_log_path):
        logging.info("No typos check log on path %s", style_log_path)
    elif os.stat(style_log_path).st_size != 0:
        print("\n\033[31mTypos [Error] **********************************************************************************************************\033[0m\n")
        printFile(typos_log_path)

    whitespaces_log_path = '{}/whitespaces_output.txt'.format(result_folder)
    if not os.path.exists(style_log_path):
        logging.info("No whitespaces check log on path %s", style_log_path)
    elif os.stat(whitespaces_log_path).st_size != 0:
        print("\n\033[31mWhitespace [Error] **********************************************************************************************************\033[0m\n")
        printFile(whitespaces_log_path)

    duplicate_log_path = '{}/duplicate_output.txt'.format(result_folder)
    if not os.path.exists(duplicate_log_path):
        logging.info("No header duplicates check log on path %s", duplicate_log_path)
    elif os.stat(duplicate_log_path).st_size != 0:
        print("\n\033[31mDuplicate [Error] **********************************************************************************************************\033[0m\n")
        printFile(duplicate_log_path)

def printFile(file):
    f = open(file)
    if "style_output.txt" in file:
        for line in f:

            patternDoth = re.compile(r'.h:[\s]+')
            subStrDoth = patternDoth.findall(line)
            if len(subStrDoth) > 0:
                continue

            patternDotcpp = re.compile(r'.cpp:[\s]+')
            subStrDotcpp = patternDotcpp.findall(line)
            if len(subStrDotcpp) > 0:
                continue

            patternDotpy = re.compile(r'.py:[\s]+')
            subStrDotpy = patternDotpy.findall(line)
            if len(subStrDotpy) > 0:
                continue

            pattern = re.compile(r':[0-9]+:')
            subStrArr = pattern.findall(line)
            if len(subStrArr) == 0:
                print(line)
                continue
            subStr = subStrArr[0]
            res = line.split(subStr, 1)

            if res[1].isspace():
                print(res[0],subStr," whitespace error")
            else:
                print(line)
    else:
        for line in f:
            print(line)
    f.close()


if __name__ == "__main__":
    repo_path = os.path.join(os.getenv("GITHUB_WORKSPACE", os.path.abspath("../../")))
    temp_path = "./temp/style_check"
    # print(repo_path)
    # print(temp_path)

    subprocess.check_output(f"mkdir -p temp/style_check", shell=True)
    subprocess.check_output(f"../../utils/check-style/check-style -n 2>&1 | tee temp/style_check/style_output.txt", shell=True)
    subprocess.check_output(f"../../utils/check-style/check-typos 2>&1 | tee temp/style_check/typos_output.txt ", shell=True)
    subprocess.check_output(f"../../utils/check-style/check-whitespaces -n 2>&1 | tee temp/style_check/whitespaces_output.txt ", shell=True)
    subprocess.check_output(f"../../utils/check-style/check-duplicate-includes.sh 2>&1 | tee temp/style_check/duplicate_output.txt", shell=True)
    subprocess.check_output(f"python3 process_style_check_result.py", shell=True)

    state, description, test_results, additional_files = process_result(temp_path)

    if state == "failure" or state == "exception":
        print("state is {}".format(state))
        printErrorFile(temp_path)
        sys.exit(1)
    else:
        sys.exit(0)
