#!/usr/bin/env python3
"""Keep tracked, hand-written Rust source below 5,000 non-test lines."""

import argparse
import re
import subprocess
import sys
import unittest
from pathlib import Path
from typing import NamedTuple


LIMIT = 5_000
REPORT_THRESHOLD = 4_000
ROOT = Path(__file__).resolve().parents[1]
RAW_STRING = re.compile(r'(?:br|cr|r)(#*)"')

# Only explicitly named generated files may bypass the check. Each needs a reason.
# build.rs emits version metadata, not Rust source; no tracked generated files exist today.
GENERATED_FILES: dict[str, str] = {}

# Existing large files may shrink but must not grow. Split them by area when worked on.
EXCEPTIONS: dict[str, tuple[int, str]] = {}


class Token(NamedTuple):
    text: str
    start: int
    end: int


def tokens(source: str) -> list[Token]:
    """Tokenize enough Rust to match module braces without reading literal contents."""
    result = []
    i = 0
    while i < len(source):
        start = i
        if source[i].isspace():
            i += 1
            continue
        if source.startswith("//", i):
            end = source.find("\n", i)
            i = len(source) if end < 0 else end
            continue
        if source.startswith("/*", i):
            depth = 1
            i += 2
            while i < len(source) and depth:
                if source.startswith("/*", i):
                    depth += 1
                    i += 2
                elif source.startswith("*/", i):
                    depth -= 1
                    i += 2
                else:
                    i += 1
            if depth:
                raise ValueError("unterminated block comment")
            continue

        raw = RAW_STRING.match(source, i)
        if raw:
            closing = '"' + raw.group(1)
            end = source.find(closing, raw.end())
            if end < 0:
                raise ValueError("unterminated raw string")
            i = end + len(closing)
            result.append(Token("literal", start, i))
            continue
        if source[i] == '"':
            i += 1
            while i < len(source) and source[i] != '"':
                i += 2 if source[i] == "\\" else 1
            if i >= len(source):
                raise ValueError("unterminated string")
            i += 1
            result.append(Token("literal", start, i))
            continue
        if source[i] == "'":
            # A lifetime has no closing quote after one character or escape.
            end = i + 1
            if source.startswith("\\u{", end):
                close = source.find("}", end + 3)
                end = len(source) if close < 0 else close + 1
            elif source.startswith("\\x", end):
                end += 4
            elif source.startswith("\\", end):
                end += 2
            else:
                end += 1
            if end < len(source) and source[end] == "'":
                i = end + 1
                result.append(Token("literal", start, i))
                continue
        if source[i].isalpha() or source[i] == "_":
            if source.startswith("r#", i):
                i += 2  # A raw identifier is not a raw string, but is one module name.
            i += 1
            while i < len(source) and (source[i].isalnum() or source[i] == "_"):
                i += 1
        else:
            i += 1
        result.append(Token(source[start:i], start, i))
    return result


def group_end(items: list[Token], start: int, opening: str, closing: str) -> int:
    depth = 0
    for i in range(start, len(items)):
        if items[i].text == opening:
            depth += 1
        elif items[i].text == closing:
            depth -= 1
            if depth == 0:
                return i + 1
    raise ValueError(f"unclosed {opening}")


def test_module_spans(source: str) -> list[tuple[int, int]]:
    items = tokens(source)
    spans = []
    i = 0
    attribute = ["#", "[", "cfg", "(", "test", ")", "]"]
    while i < len(items):
        if [item.text for item in items[i : i + 7]] != attribute:
            i += 1
            continue
        start = i
        i += 7
        # Other outer attributes and visibility do not make a test module production code.
        while [item.text for item in items[i : i + 2]] == ["#", "["]:
            i = group_end(items, i + 1, "[", "]")
        if i < len(items) and items[i].text == "pub":
            i += 1
            if i < len(items) and items[i].text == "(":
                i = group_end(items, i, "(", ")")
        if i + 2 >= len(items) or items[i].text != "mod":
            continue
        i += 2  # Skip `mod` and the module name.
        if items[i].text != "{":
            continue  # A path module (`mod tests;`) removes no lines from this file.
        end = group_end(items, i, "{", "}")
        spans.append((items[start].start, items[end - 1].end))
        i = end
    return spans


def count_source_lines(source: str) -> int:
    spans = iter(test_module_spans(source))
    span = next(spans, None)
    count = 0
    offset = 0
    for line in source.splitlines(keepends=True):
        end = offset + len(line)
        cursor = offset
        remaining = []
        touched = False
        while span is not None and span[0] < end:
            touched = True
            remaining.append(source[cursor : max(cursor, span[0])])
            cursor = min(end, span[1])
            if span[1] > end:
                break
            span = next(spans, None)
        remaining.append(source[cursor:end])
        # Preserve production code sharing a line with a test module, and blank lines outside it.
        if not touched or "".join(remaining).strip():
            count += 1
        offset = end
    return count


def exempt(path: str) -> bool:
    parts = Path(path).parts
    return (
        any(part in {"tests", "benches", "test-vectors"} for part in parts[:-1])
        or parts[-1].endswith("tests.rs")
        or parts[-1] == "test_support.rs"
        or path in GENERATED_FILES
    )


def size_message(path: str, count: int) -> tuple[bool, str]:
    prefix = f"{path}: {count} hand-written lines"
    if path in EXCEPTIONS:
        pin, _reason = EXCEPTIONS[path]
        if count < LIMIT:
            return True, f"{prefix} (limit: under {LIMIT}); remove its exception."
        if count > pin:
            return True, f"{prefix} (pinned limit: {pin}); split it by area; do not raise the pin."
        if count < pin:
            return False, f"NOTICE {prefix} (pinned limit: {pin}); lower the pin to {count}."
    elif count >= LIMIT:
        return True, f"{prefix} (limit: under {LIMIT}); split it by area."
    return False, ""


class SelfTests(unittest.TestCase):
    def test_raw_strings_and_nested_module(self):
        source = '''fn production() {}
#[cfg(test)]
mod tests {
    const JSON: &str = r#"{"brace": "}"}"#;
    const SQL: &str = r"select '}'";
    const MORE: &str = r###"}"## }"###;
    const BYTES: &[u8] = br##"} "# }"##;
    const C: &std::ffi::CStr = cr#"}"#;
    // } and #[cfg(test)] mod fake {
    /* } /* nested { */ } */
    mod nested {
        fn chars() { let _ = ('}', '{', '\\'', '\\\\', '\\u{7d}', b'}'); }
    }
}
fn after() {}
'''
        self.assertEqual(count_source_lines(source), 2)

    def test_path_module_removes_nothing(self):
        source = "#[cfg(test)] mod x;\nmod production {\n    fn f() {}\n}\n"
        self.assertEqual(count_source_lines(source), 4)

    def test_non_test_module_and_blank_lines(self):
        source = "// comment\n\nmod production {\n    fn f() {}\n}\n"
        self.assertEqual(count_source_lines(source), 5)

    def test_strings_comments_and_lifetimes_outside_tests(self):
        source = '''const TEXT: &str = "#[cfg(test)] mod fake { \\" }";
/* #[cfg(test)] mod fake { } */
fn borrow<'a>(s: &'a str) -> &'a str { s }
#[cfg(test)]
mod tests { const S: &str = "} \\" {"; }
'''
        self.assertEqual(count_source_lines(source), 3)

    def test_attributes_visibility_and_multiline_cfg(self):
        source = "#[cfg(\n test\n)]\n#[allow(dead_code)]\npub(crate) mod checks {\n}\n"
        self.assertEqual(count_source_lines(source), 0)
        self.assertEqual(count_source_lines("#[cfg(test)] mod r#type {}\n"), 0)

    def test_multiple_modules_and_shared_lines(self):
        source = "#[cfg(test)] mod one {}\n\nmod prod { #[cfg(test)] mod two {} }\n"
        self.assertEqual(count_source_lines(source), 2)

    def test_empty_and_no_final_newline(self):
        self.assertEqual(count_source_lines(""), 0)
        self.assertEqual(count_source_lines("fn f() {}"), 1)
        self.assertEqual(count_source_lines("#[cfg(test)] mod tests {}"), 0)

    def test_file_exemptions(self):
        for path in (
            "crates/a/tests/x.rs", "crates/a/benches/x.rs", "crates/a/test-vectors/x.rs",
            "crates/a/src/foo_tests.rs", "crates/a/src/tests.rs", "crates/a/src/test_support.rs",
        ):
            with self.subTest(path=path):
                self.assertTrue(exempt(path))
        self.assertFalse(exempt("crates/a/src/testing.rs"))

    def test_limit_and_pins(self):
        path = "crates/self-test.rs"
        self.assertFalse(size_message(path, LIMIT - 1)[0])
        self.assertTrue(size_message(path, LIMIT)[0])
        EXCEPTIONS[path] = (LIMIT + 10, "Self-test fixture.")
        try:
            self.assertEqual(size_message(path, LIMIT + 10), (False, ""))
            self.assertTrue(size_message(path, LIMIT + 11)[0])
            failed, message = size_message(path, LIMIT)
            self.assertFalse(failed)
            self.assertIn("lower the pin", message)
            failed, message = size_message(path, LIMIT - 1)
            self.assertTrue(failed)
            self.assertIn("remove its exception", message)
        finally:
            del EXCEPTIONS[path]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", action="store_true", help="list files with at least 4,000 non-test lines")
    parser.add_argument("--self-test", action="store_true", help="run the scanner's regression tests")
    args = parser.parse_args()
    if args.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(SelfTests)
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1

    try:
        tracked = subprocess.check_output(["git", "ls-files", "-z", "--", "crates/"], cwd=ROOT)
        paths = sorted(path for path in tracked.decode().split("\0") if path.endswith(".rs") and not exempt(path))
        failed = False
        counts = []
        for path in paths:
            try:
                count = count_source_lines((ROOT / path).read_text(encoding="utf-8"))
            except (OSError, ValueError) as error:
                print(f"{path}: cannot count source lines: {error}")
                failed = True
                continue
            counts.append((count, path))
            violation, message = size_message(path, count)
            failed |= violation
            if message:
                print(message)
        for path in sorted(EXCEPTIONS.keys() - set(paths)):
            print(f"{path}: not a checked source file; remove its exception.")
            failed = True
        if args.report:
            print(f"Rust files with at least {REPORT_THRESHOLD} hand-written lines (excluding in-file tests):")
            for count, path in sorted(counts, key=lambda item: (-item[0], item[1])):
                if count >= REPORT_THRESHOLD:
                    print(f"{count:6}  {path}")
        print(f"File size check {'FAILED' if failed else 'passed'}: {len(counts)} Rust source files checked (limit: under {LIMIT}).")
        return 1 if failed else 0
    except (OSError, subprocess.CalledProcessError) as error:
        print(f"File size check failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
