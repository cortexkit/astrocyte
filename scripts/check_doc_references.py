#!/usr/bin/env python3
"""Check that the overview docs name only things that exist.

Every backticked token in the checked Markdown files must resolve:

- a path (contains "/" or has a file extension) must be a tracked file or
  directory, or a path-component suffix of one;
- a dotted lowercase name such as spend.report must appear as a string literal
  in the Rust source;
- anything else must appear as a whole word in the Rust source or a Cargo.toml.

Two lists are checked for set equality rather than mere existence, because
naming a table that a later migration dropped, or leaving out a served
operation, would pass an existence check:

- the Markdown table whose first header cell is "Table" must list exactly the
  tables that exist after applying every migration in store.rs to an empty
  in-memory SQLite database;
- the Markdown table whose first header cell is "Operation" must list exactly
  PUBLIC_OPERATION_NAMES from ops.rs.

The highest migration version must also appear as "version N".

Usage: scripts/check_doc_references.py [FILE.md ...]
Defaults to ARCHITECTURE.md and STRUCTURE.md. Exits 1 on any miss.
"""

import pathlib
import re
import sqlite3
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
STORE = ROOT / "crates/astrocyte-core/src/store.rs"
OPS = ROOT / "crates/astrocyte-module/src/ops.rs"
FILE_SUFFIXES = (".rs", ".md", ".py", ".sh", ".json", ".jsonc", ".jsonl", ".yml", ".toml")


def tracked_paths():
    out = subprocess.check_output(["git", "ls-files"], cwd=ROOT, text=True)
    files = set(out.split())
    dirs = set()
    for path in files:
        parts = path.split("/")
        for i in range(1, len(parts)):
            dirs.add("/".join(parts[:i]))
    return files, dirs


def source_text():
    out = subprocess.check_output(["git", "ls-files", "*.rs", "*Cargo.toml"], cwd=ROOT, text=True)
    return "\n".join((ROOT / p).read_text() for p in out.split())


def migrations():
    text = STORE.read_text()
    body = text[text.index("pub const MIGRATIONS") : text.index("#[cfg(test)]")]
    return [
        (int(v), sql)
        for v, sql in re.findall(r'version:\s*(\d+),\s*statements:\s*r#"(.*?)"#', body, re.S)
    ]


def final_tables(chain):
    conn = sqlite3.connect(":memory:")
    for _, sql in chain:
        conn.executescript(sql)
    rows = conn.execute(
        "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    return {name for (name,) in rows}


def served_operations():
    text = OPS.read_text()
    consts = dict(re.findall(r'pub const (OP_\w+): &str = "([^"]+)";', text))
    names = re.search(r"PUBLIC_OPERATION_NAMES: \[&str; \d+\] = \[(.*?)\];", text, re.S).group(1)
    return {consts[name] for name in re.findall(r"OP_\w+", names)}


def markdown_column(text, header):
    """First-column backticked values of the Markdown table whose first header is `header`."""
    values, inside = set(), False
    for line in text.splitlines():
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if line.lstrip().startswith("|") and cells[0] == header:
            inside = True
            continue
        if inside:
            if not line.lstrip().startswith("|"):
                break
            match = re.fullmatch(r"`([^`]+)`", cells[0])
            if match:
                values.add(match.group(1))
    return values


def resolve_path(token, files, dirs):
    token = token.rstrip("/")
    if token in files or token in dirs:
        return True
    return any(p.endswith("/" + token) for p in files | dirs)


def resolve(token, files, dirs, source):
    for word in token.split():
        if word.startswith("<"):
            continue
        if "/" in word or word.endswith(FILE_SUFFIXES):
            ok = resolve_path(word, files, dirs)
        elif re.fullmatch(r"[a-z]+(\.[a-z_]+)+", word):
            ok = f'"{word}"' in source
        else:
            ok = all(
                re.search(r"(?<![\w-])" + re.escape(part) + r"(?![\w-])", source)
                for part in word.split("::")
            )
        if not ok:
            return False
    return True


# Both overview files are loaded at the head of every agent session in this
# repository, so their size is paid on every turn, not once. They are meant to
# stay short maps; merges that add a line each turn them into a change log. At
# 2026-09-24 the pair was 12,768 bytes (about 3,100 tokens). The ceiling leaves
# room for real growth and fails well before the ~60 KB (~15K tokens) at which
# a sibling project's pair had to be rewritten. A failure means trim the
# files; raising the number is the wrong fix.
MAX_OVERVIEW_BYTES = 20_000
OVERVIEW_FILES = ("ARCHITECTURE.md", "STRUCTURE.md")


def controls(files, dirs, source, tables, operations):
    """Why this run's verdict can't be trusted, or nothing.

    The resolver must reject one invented token of each kind it handles (a
    path, a dotted operation name, an identifier) and accept a real one of
    each, and every reference set must be non-empty. Without this, a resolver
    edited into one that accepts everything, or a source read that came back
    empty, would still print "0 misses".
    """
    broken = []
    if not (files and source and tables and operations):
        broken.append(
            f"a reference set is empty: files={len(files)} source={len(source)} "
            f"tables={len(tables)} operations={len(operations)}"
        )
        return broken
    for token in ("crates/astrocyte-core/src/no_such_file.rs", "spend.no.such_op", "NoSuchSymbolAnywhere"):
        if resolve(token, files, dirs, source):
            broken.append(f"the resolver accepted an invented token: `{token}`")
    for token in ("crates/astrocyte-core/src/store.rs", "spend.report", "PUBLIC_OPERATION_NAMES"):
        if not resolve(token, files, dirs, source):
            broken.append(f"the resolver rejected a real token: `{token}`")
    return broken


def main(argv):
    names = argv[1:] or list(OVERVIEW_FILES)
    files, dirs = tracked_paths()
    source = source_text()
    chain = migrations()
    tables = final_tables(chain)
    operations = served_operations()
    broken = controls(files, dirs, source, tables, operations)
    for reason in broken:
        print(f"CONTROL {reason}")
    if broken:
        return 1
    misses, checked = [], 0

    for name in names:
        text = (ROOT / name).read_text()
        for token in sorted(set(re.findall(r"`([^`\n]+)`", text))):
            checked += 1
            if not resolve(token, files, dirs, source):
                misses.append(f"{name}: `{token}` not found in the tree or the source")
        listed = markdown_column(text, "Table")
        if listed and listed != tables:
            misses.append(
                f"{name}: table list differs from the migrated schema; "
                f"missing {sorted(tables - listed)}, extra {sorted(listed - tables)}"
            )
        listed = markdown_column(text, "Operation")
        if listed and listed != operations:
            misses.append(
                f"{name}: operation list differs from PUBLIC_OPERATION_NAMES; "
                f"missing {sorted(operations - listed)}, extra {sorted(listed - operations)}"
            )

    overview_bytes = sum(len((ROOT / n).read_bytes()) for n in OVERVIEW_FILES)
    if overview_bytes > MAX_OVERVIEW_BYTES:
        misses.append(
            f"ARCHITECTURE.md + STRUCTURE.md are {overview_bytes} bytes, over the "
            f"{MAX_OVERVIEW_BYTES}-byte ceiling: trim them, don't raise the ceiling"
        )

    latest = max(v for v, _ in chain)
    if not any(f"version {latest}" in (ROOT / n).read_text() for n in names):
        misses.append(f"no checked file states the current schema as 'version {latest}'")

    for miss in misses:
        print(miss)
    print(
        f"checked {checked} tokens, {len(tables)} tables, {len(operations)} operations "
        f"in {len(names)} files: {len(misses)} misses (controls caught)"
    )
    return 1 if misses or checked == 0 else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
