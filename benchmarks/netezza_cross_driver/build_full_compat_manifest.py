#!/usr/bin/env python3
"""Build a JSON compatibility manifest from the shared C# reference corpus.

The source is the referenceQueries.js file used by the existing C# comparison
suite. This parser only reads its query arrays; it never imports or executes
Node code.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path


def strip_comments(text: str) -> str:
    out: list[str] = []
    i = 0
    quote: str | None = None
    while i < len(text):
        if quote:
            out.append(text[i])
            if text[i] == "\\" and i + 1 < len(text):
                out.append(text[i + 1])
                i += 2
                continue
            if text[i] == quote:
                quote = None
            i += 1
            continue
        if text[i] in "\"'\x60":
            quote = text[i]
            out.append(text[i])
            i += 1
            continue
        if text.startswith("//", i):
            end = text.find("\n", i)
            i = len(text) if end < 0 else end
            continue
        if text.startswith("/*", i):
            end = text.find("*/", i + 2)
            i = len(text) if end < 0 else end + 2
            continue
        out.append(text[i])
        i += 1
    return "".join(out)


def matching(text: str, start: int, opening: str, closing: str) -> str:
    depth = 0
    quote: str | None = None
    i = start
    while i < len(text):
        char = text[i]
        if quote:
            if char == "\\":
                i += 2
                continue
            if char == quote:
                quote = None
            i += 1
            continue
        if char in "\"'\x60":
            quote = char
        elif char == opening:
            depth += 1
        elif char == closing:
            depth -= 1
            if depth == 0:
                return text[start + 1 : i]
        i += 1
    raise ValueError(f"unterminated {opening} expression")


def literals(expression: str) -> list[str]:
    values: list[str] = []
    i = 0
    while i < len(expression):
        if expression[i] not in "\"'\x60":
            i += 1
            continue
        quote = expression[i]
        i += 1
        chars: list[str] = []
        while i < len(expression) and expression[i] != quote:
            if expression[i] == "\\" and i + 1 < len(expression):
                chars.append(bytes(expression[i : i + 2], "utf-8").decode("unicode_escape"))
                i += 2
            else:
                chars.append(expression[i])
                i += 1
        if i >= len(expression):
            raise ValueError("unterminated string literal")
        values.append("".join(chars))
        i += 1
    return values


def array_values(source: str, name: str, variables: dict[str, str]) -> list[str]:
    match = re.search(rf"\bconst\s+{re.escape(name)}\s*=\s*\[", source)
    if not match:
        return []
    body = matching(source, source.find("[", match.start()), "[", "]")
    values: list[str] = []
    for item in body.split("\n"):
        item = item.strip().rstrip(",")
        if not item:
            continue
        identifier = re.fullmatch(r"[A-Za-z_$][A-Za-z0-9_$]*", item)
        if identifier and identifier.group() in variables:
            values.append(variables[identifier.group()])
            continue
        values.extend(literals(item))
    return [value.strip() for value in values if value.strip()]


def variable_values(source: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for match in re.finditer(r"\bconst\s+([A-Za-z_$][A-Za-z0-9_$]*)\s*=\s*([\x60\"'])", source):
        name = match.group(1)
        quote = match.group(2)
        start = match.start(2)
        if quote == "\x60":
            end = source.find("\x60", start + 1)
            if end >= 0:
                values[name] = source[start + 1 : end]
        else:
            expression = source[start : source.find("\n", start)]
            parsed = literals(expression)
            if parsed:
                values[name] = parsed[0]
    return values


def main() -> None:
    if len(sys.argv) != 4:
        raise SystemExit("usage: build_full_compat_manifest.py CORE.json referenceQueries.js OUTPUT.json")
    core_path, source_path, output_path = map(Path, sys.argv[1:])
    manifest = json.loads(core_path.read_text())
    source = strip_comments(Path(source_path).read_text(encoding="utf-8"))
    variables = variable_values(source)
    queries = []
    for array_name in ("queries", "systemQueries"):
        queries.extend(array_values(source, array_name, variables))
    seen = {case["id"] for case in manifest["cases"]}
    existing_sql = {case["sql"] for case in manifest["cases"]}
    for index, query in enumerate(queries, 1):
        case_id = f"csharp_reference_{index:04d}"
        if query in existing_sql or case_id in seen:
            continue
        upper = query.upper()
        reasons: list[str] = []
        if (
            "NUMERIC(38" in upper
            or "923281625142643375987" in upper
            or "NUMERIC_TEST" in upper
        ):
            reasons.append("C# decimal rounds wide NUMERIC values while Rust preserves exact values")
        if any(token in upper for token in ("CURRENT_DATE", "CURRENT_TIMESTAMP", "NOW()")):
            reasons.append("current date/time values are sampled by two sequential sessions")
        if any(token in upper for token in ("SYSTEM.ADMIN.", "._V_", "._T_", "._VT_")):
            reasons.append("system catalog values and row counts can change between sequential sessions")
        manifest["cases"].append(
            {
                "id": case_id,
                "category": "csharp-reference",
                "mode": "query",
                "sql": query,
                "compare_types": False,
                "float_tolerance": 0.000001,
                "trim_trailing_spaces": True,
                "known_divergence": "; ".join(reasons) if reasons else None,
            }
        )
        seen.add(case_id)
        existing_sql.add(query)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")
    print(f"wrote {output_path} with {len(manifest['cases'])} cases")


if __name__ == "__main__":
    main()
