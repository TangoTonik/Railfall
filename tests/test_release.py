"""Release checks: tag, package version and changelog must agree before publishing."""

from __future__ import annotations

import pytest

from railfall import __version__, release

CHANGELOG = """# Changelog

## [Unreleased]

## [1.0.0] - 2026-11-26

### Added
- Weather results page.

## [0.1.0] - 2026-10-27

- Project skeleton.

[1.0.0]: https://github.com/TangoTonik/Railfall/releases/tag/v1.0.0
"""


@pytest.mark.parametrize(
    ("tag", "version"),
    [("v1.0.0", "1.0.0"), ("1.2.3", "1.2.3"), ("refs/tags/v10.0.1", "10.0.1")],
)
def test_normalise_tag(tag, version):
    assert release.normalise_tag(tag) == version


@pytest.mark.parametrize("tag", ["v1.0", "v1.0.0-rc1", "latest", "v01.0.0", ""])
def test_normalise_tag_refuses_non_semver(tag):
    with pytest.raises(release.ReleaseError):
        release.normalise_tag(tag)


def test_parse_changelog_keeps_order_and_dates():
    sections = release.parse_changelog(CHANGELOG)
    assert [(s.version, s.date) for s in sections] == [
        ("Unreleased", None),
        ("1.0.0", "2026-11-26"),
        ("0.1.0", "2026-10-27"),
    ]
    assert sections[0].body == ""


def test_notes_drop_link_references():
    notes = release.notes_for("1.0.0", CHANGELOG)
    assert notes == "### Added\n- Weather results page.\n"


def test_notes_need_a_section():
    with pytest.raises(release.ReleaseError, match="no section"):
        release.notes_for("2.0.0", CHANGELOG)


def test_notes_refuse_an_undated_section():
    with pytest.raises(release.ReleaseError, match="no release date"):
        release.notes_for("Unreleased", CHANGELOG)


def test_notes_refuse_an_empty_section():
    text = "## [1.1.0] - 2026-12-01\n\n## [1.0.0] - 2026-11-26\n- x\n"
    with pytest.raises(release.ReleaseError, match="empty"):
        release.notes_for("1.1.0", text)


def test_check_requires_the_package_version():
    assert release.check("v1.0.0", CHANGELOG, package_version="1.0.0") == "1.0.0"
    with pytest.raises(release.ReleaseError, match="bump"):
        release.check("v1.0.0", CHANGELOG, package_version="0.9.0")


def test_cli_notes_and_check(tmp_path, capsys):
    path = tmp_path / "CHANGELOG.md"
    path.write_text(CHANGELOG)
    assert release.main(["notes", "v0.1.0", "--changelog", str(path)]) == 0
    assert capsys.readouterr().out == "- Project skeleton.\n"

    # The tag defaults to the package version, which this fixture may not describe.
    code = release.main(["check", "v9.9.9", "--changelog", str(path)])
    assert code == 1
    assert "does not match" in capsys.readouterr().err


def test_cli_missing_changelog(tmp_path, capsys):
    assert release.main(["notes", "v1.0.0", "--changelog", str(tmp_path / "nope.md")]) == 1
    assert "release:" in capsys.readouterr().err


def test_version_is_single_sourced():
    """pyproject reads the version from railfall.__version__; it must not repeat it."""
    import tomllib

    pyproject = tomllib.loads((release.ROOT / "pyproject.toml").read_text())
    assert "version" not in pyproject["project"]
    assert "version" in pyproject["project"]["dynamic"]
    attr = pyproject["tool"]["setuptools"]["dynamic"]["version"]["attr"]
    assert attr == "railfall.__version__"
    assert release.SEMVER.match(__version__)
