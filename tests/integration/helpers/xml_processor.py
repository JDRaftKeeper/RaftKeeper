import os
import sys
import xml.etree.ElementTree as ET


def replace_xml_tagvalue(file_path, tag="", value=""):
    try:
        tree = ET.parse(file_path)
    except Exception as e:
        print(e)
        return

    def value_dfs(root, tag, value):
        for child in root:
            value_dfs(child, tag, value)

        if root.tag == tag:
            root.text = value


    root = tree.getroot()
    value_dfs(root, tag, value)

    try:
        tree.write(file_path)
    except Exception as e:
        print(e)


def replace_xml_tagname(file_path, before="", after=""):
    try:
        tree = ET.parse(file_path)
    except Exception as e:
        print(e)
        return


    def tag_dfs(root, before, after):
        for child in root:
            tag_dfs(child, before, after)

        if root.tag == before:
            root.tag = after


    root = tree.getroot()
    tag_dfs(root, before, after)

    try:
        tree.write(file_path)
    except Exception as e:
        print(e)


def process_xml_files(dir_path, func, **kwargs):
    try:
        filenames = os.listdir(dir_path)
    except Exception as e:
        print(e)
        return

    for filename in filenames:
        file_path = os.path.join(dir_path, filename)
        if os.path.isdir(file_path):
            process_xml_files(file_path, func, **kwargs)
        elif filename.endswith('.xml'):
            print(f"Processing {file_path}...")
            func(file_path, **kwargs)

def append_newline(file_path):
    try:
        with open(file_path, 'a') as f:
            f.write('\n')
    except Exception as e:
        print(e)


if __name__ == '__main__':
    folder_path = '../'
    # processing tagname
    process_xml_files(folder_path, replace_xml_tagname, before="max_session_timeout_ms", after="some_other_string")

    # processing tag value
    process_xml_files(folder_path, replace_xml_tagvalue, tag="some_other_string", value="400000")

    # append newline at the end of file
    process_xml_files(folder_path, append_newline)

