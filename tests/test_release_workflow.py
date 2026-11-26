"""Guards on the release workflow: only checked, tagged versions get published."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "release.yml"


@pytest.fixture(scope="module")
def doc() -> dict:
    return yaml.safe_load(WORKFLOW.read_text())


def _runs(doc: dict) -> list[str]:
    return [s.get("run", "") for s in doc["jobs"]["release"]["steps"]]


def _index(doc: dict, needle: str) -> int:
    return next(i for i, run in enumerate(_runs(doc)) if needle in run)


def test_triggers_only_on_semver_tags(doc):
    # PyYAML reads the bare `on:` key as the boolean True.
    trigger = doc[True]
    assert set(trigger) == {"push"}
    assert trigger["push"] == {"tags": ["v*.*.*"]}


def test_token_can_only_write_contents(doc):
    assert doc["permissions"] == {"contents": "write"}


def test_check_runs_before_build_and_publish(doc):
    check = _index(doc, "railfall.release check")
    assert check < _index(doc, "python -m build") < _index(doc, "gh release create")


def test_publish_uses_changelog_notes_and_existing_tag(doc):
    publish = _runs(doc)[_index(doc, "gh release create")]
    assert "--verify-tag" in publish
    assert "--notes-file release-notes.md" in publish
    assert _index(doc, "railfall.release notes") < _index(doc, "gh release create")


def test_changelog_parses_and_unreleased_comes_first():
    from railfall import release

    sections = release.parse_changelog((ROOT / "CHANGELOG.md").read_text())
    assert sections, "CHANGELOG.md has no ## [version] sections"
    released = [s for s in sections if s.version != "Unreleased"]
    assert all(s.date for s in released), "every released version needs a date"
    if sections[0].version != "Unreleased":
        from railfall import __version__

        assert sections[0].version == __version__, "newest section must be the package version"
