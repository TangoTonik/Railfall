"""Release checks and notes, used by the `release` workflow and `make release-check`.

    python -m railfall.release check v1.0.0          # tag == package version, notes exist
    python -m railfall.release notes v1.0.0 > notes.md

The version lives in one place (`railfall.__version__`, read by setuptools at
build time). A tag is only released when it matches that version *and* the
changelog has a dated section for it, so a forgotten bump or an empty
"Unreleased" section fails before anything is published.
"""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from railfall import __version__

ROOT = Path(__file__).resolve().parents[2]
CHANGELOG = ROOT / "CHANGELOG.md"

SEMVER = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$")
# "## [1.0.0] - 2026-11-26" (Keep a Changelog); the date is mandatory for a release.
HEADING = re.compile(r"^## \[(?P<version>[^\]]+)\](?:\s+-\s+(?P<date>\d{4}-\d{2}-\d{2}))?\s*$")


class ReleaseError(ValueError):
    """The tag, the package version and the changelog disagree."""


@dataclass(frozen=True)
class Section:
    version: str
    date: str | None
    body: str


def normalise_tag(tag: str) -> str:
    """`v1.0.0` or `refs/tags/v1.0.0` -> `1.0.0`; anything not semver is refused."""
    version = tag.removeprefix("refs/tags/").removeprefix("v")
    if not SEMVER.match(version):
        raise ReleaseError(f"tag {tag!r} is not vMAJOR.MINOR.PATCH")
    return version


def parse_changelog(text: str) -> list[Section]:
    """Split a Keep-a-Changelog file into its `## [version]` sections, in file order."""
    sections: list[Section] = []
    current: tuple[str, str | None] | None = None
    lines: list[str] = []

    def flush() -> None:
        if current is not None:
            # Link references ("[1.0.0]: https://...") belong to the footer, not the notes.
            body = [line for line in lines if not re.match(r"^\[[^\]]+\]:\s", line)]
            sections.append(Section(current[0], current[1], "\n".join(body).strip()))

    for line in text.splitlines():
        match = HEADING.match(line)
        if match:
            flush()
            current = (match["version"], match["date"])
            lines = []
        elif current is not None:
            lines.append(line)
    flush()
    return sections


def notes_for(version: str, changelog: str) -> str:
    """The release notes of `version`: its changelog section, which must be dated and non-empty."""
    for section in parse_changelog(changelog):
        if section.version == version:
            if section.date is None:
                raise ReleaseError(f"CHANGELOG section [{version}] has no release date")
            if not section.body:
                raise ReleaseError(f"CHANGELOG section [{version}] is empty")
            return section.body + "\n"
    raise ReleaseError(f"CHANGELOG has no section for [{version}]")


def check(tag: str, changelog: str, package_version: str = __version__) -> str:
    """Validate a tag against the package version and the changelog; return the version."""
    version = normalise_tag(tag)
    if version != package_version:
        raise ReleaseError(
            f"tag {tag} does not match the package version {package_version} "
            "(bump railfall.__version__ first)"
        )
    notes_for(version, changelog)
    return version


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m railfall.release", description=__doc__)
    parser.add_argument("command", choices=("check", "notes"))
    parser.add_argument("tag", nargs="?", default=f"v{__version__}")
    parser.add_argument("--changelog", type=Path, default=CHANGELOG)
    args = parser.parse_args(argv)

    try:
        text = args.changelog.read_text(encoding="utf-8")
        if args.command == "check":
            version = check(args.tag, text)
            print(f"ok: {args.tag} matches railfall {version} and has release notes")
        else:
            sys.stdout.write(notes_for(normalise_tag(args.tag), text))
    except (OSError, ReleaseError) as exc:
        print(f"release: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
