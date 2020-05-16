#!/usr/bin/python3

from collections import namedtuple
import sys


INCLUDE_MARKER = "__INCLUDE_RUST_DOC__"

Template = namedtuple("Template", ["header", "footer", "doc_path", "start"])


def read_rust_doc_lines(path):
    with open(path, "r") as rust_doc:
        for line in rust_doc:
            if line.startswith('//! '):
                yield line[4:]
            elif line.startswith('//!'):
                yield line[3:]
            else:
                break


def parse_template_file(path):
    content = [line for line in open(path, "r")]
    try:
        marker_position = [n for (n, line) in enumerate(content)
                           if line.startswith(INCLUDE_MARKER)][0]
    except IndexError:
        raise Exception("Missing include marker")
    include_info = content[marker_position].strip().split('$')
    doc_path = include_info[1]
    start = None
    if len(include_info) > 2:
        start = include_info[2]
    return Template(
        header=content[0:marker_position], footer=content[marker_position+1:],
        doc_path=include_info[1], start=start,
    )


template = parse_template_file("readme_template")
doc = read_rust_doc_lines(template.doc_path)

output = sys.stdout

for line in template.header:
    output.write(line)

if template.start:
    for line in doc:
        if line.startswith(template.start):
            output.write(line)
            break

for line in doc:
    output.write(line)

for line in template.footer:
    output.write(line)
