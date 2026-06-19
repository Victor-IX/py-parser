# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

py-parser is a small Python package that scans files under a directory for regex or
literal pattern matches, parallelised across worker processes. The public API is a single
`FileScanner` class, re-exported from `py_parser/__init__.py`.

Targets **Python 3.11+**.

## Code Style

- **Line length 120, target py311, formatted with ruff** (`ruff format` / `ruff check`).
- Public API is curated in `py_parser/__init__.py` via `__all__` — add new public symbols there.

## Comments & Docstrings

Comments are a maintenance cost: they drift out of date as the code changes. Write them only when the code cannot speak for itself. When editing existing code, trim its comments to this standard as you go.

- **Default to no comment.** Prefer clear names over explanation. A well-named function, constant, or variable needs none.
- **At most one short line** of high-level intent above a non-trivial block. Never write multi-line comment blocks.
- **Never state what is discoverable elsewhere**: how often or where a function is called, idempotency, early-return/guard behavior (visible on the first lines of the body), or anything that merely restates the code.
- **Do not narrate the change you are making.** Why the new code exists or what feature it implements belongs in the commit message or PR description, not the source. A guard clause or early return almost never needs a comment justifying it.
- **Do keep** the rare comment that explains what the code genuinely cannot show at a glance: why an assertion is safe, a non-obvious workaround, or a subtle ordering constraint.

**Litmus test before writing any comment:** could a competent reader infer this from the code in a few seconds? If yes, delete it. Only write it when the answer is genuinely no, and the missing context cannot be encoded in a name.

**Docstrings** follow Google style and stay to a single summary line of description. Reserve a multi-line description for the cases that earn it: a class needing broader context, or a genuinely complex function whose behavior a reader cannot infer from the signature and a one-liner. Trivial functions need only the one-line summary.

- **Always document params and return values.** If a function takes arguments it must have an `Args:` section; if it returns a value it must have a `Returns:` section (and `Raises:` likewise). This is non-negotiable even for trivial functions — only the *description* collapses to one line, never the `Args:`/`Returns:` sections.
- **No module-level docstring** at the top of a file describing what the file contains or which classes it holds.
