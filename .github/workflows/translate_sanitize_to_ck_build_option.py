#!/usr/bin/env python3

import sys

# translate sanitize to ck build option, for example: tsan to thread
def translate_sanitize_to_ck_option(sanitize):
    sanitize_map = {
        'none': 'none',
        'tsan': 'thread',
        'asan': 'address',
        'msan': 'memory',
        'ubsan': 'undefined'
    }
    return sanitize_map.get(sanitize, '')

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: translate_sanitize_to_ck_build_option.py <sanitize>")
        sys.exit(1)
    sanitize = sys.argv[1]
    ck_option = translate_sanitize_to_ck_option(sanitize)
    print(ck_option)
