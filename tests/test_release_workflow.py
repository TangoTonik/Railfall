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


def test_token_can_only_write_contents_and_read_runs(doc):
    assert doc["permissions"] == {"contents": "write", "actions": "read"}


def test_results_come_from_the_tagged_commits_green_e2e_run(doc):
    fetch = _runs(doc)[_index(doc, "gh run download")]
    assert '--commit "$GITHUB_SHA"' in fetch
    assert "--workflow e2e.yml" in fetch
    # --exit-status turns a red e2e run into a failed release.
    assert "--exit-status" in fetch
    assert fetch.index("gh run watch") < fetch.index("gh run download")
    assert "--name weather-results" in fetch


def test_results_are_attached_after_the_check(doc):
    assert _index(doc, "railfall.release check") < _index(doc, "gh run download")
    assert _index(doc, "gh run download") < _index(doc, "gh release create")
    publish = _runs(doc)[_index(doc, "gh release create")]
    assert "results.zip" in publish
    assert "RESULTS-${GITHUB_REF_NAME}.md" in publish


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
