#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Optional

from pydash.objects import get

from connector_ops.utils import Connector, ConnectorLanguage


def check_migration_guide(connector: Connector) -> bool:
    """Check if a migration guide is available for the connector if a breaking change was introduced."""
    breaking_changes = get(connector.metadata, "releases.breakingChanges")
    if not breaking_changes:
        return True

    migration_guide_file_path = connector.migration_guide_file_path
    migration_guide_exists = migration_guide_file_path is not None and migration_guide_file_path.exists()
    if not migration_guide_exists:
        print(
            f"Migration guide file is missing for {connector.name}. Please create a migration guide at {connector.migration_guide_file_path}",
        )
        return False

    # Check that the migration guide begins with # {connector name} Migration Guide
    expected_title = f"# {connector.name_from_metadata} Migration Guide"
    expected_version_header_start = "## Upgrading to "
    with open(migration_guide_file_path) as f:
        first_line = f.readline().strip()
        if first_line != expected_title:
            print(
                f"Migration guide file for {connector.technical_name} does not start with the correct header. Expected '{expected_title}', got '{first_line}'",
            )
            return False

        # Check that the migration guide contains a section for each breaking change key ## Upgrading to {version}
        # Note that breaking change is a dict where the version is the key
        # Note that the migration guide must have the sections in order of the version descending
        # 3.0.0, 2.0.0, 1.0.0, etc
        # This means we have to record the headings in the migration guide and then check that they are in order
        # We also have to check that the headings are in the breaking changes dict

        ordered_breaking_changes = sorted(breaking_changes.keys(), reverse=True)
        ordered_expected_headings = [f"{expected_version_header_start}{version}" for version in ordered_breaking_changes]

        ordered_heading_versions = []
        for line in f:
            stripped_line = line.strip()
            if stripped_line.startswith(expected_version_header_start):
                version = stripped_line.replace(expected_version_header_start, "")
                ordered_heading_versions.append(version)

        if ordered_breaking_changes != ordered_heading_versions:
            print(f"Migration guide file for {connector.name} has incorrect version headings.")
            print("Check for missing, extra, or misordered headings, or headers with typos.")
            print(f"Expected headings: {ordered_expected_headings}")
            return False

    return True


def check_documentation_file_exists(connector: Connector) -> bool:
    """Check if a markdown file with connector documentation is available
    in docs/integrations/<connector-type>s/<connector-name>.md

    Args:
        connector (Connector): a Connector dataclass instance.

    Returns:
        bool: Wether a documentation file was found.
    """
    file_path = connector.documentation_file_path

    return file_path is not None and file_path.exists()


def check_documentation_follows_guidelines(connector: Connector) -> bool:
    """Documentation guidelines are defined here https://hackmd.io/Bz75cgATSbm7DjrAqgl4rw"""
    follows_guidelines = True
    with open(connector.documentation_file_path) as f:
        doc_lines = [line.lower() for line in f.read().splitlines()]
    if not doc_lines[0].startswith("# "):
        print("The connector name is not used as the main header in the documentation.")
        follows_guidelines = False
    # We usually don't have a metadata if the connector is not published.
    if connector.metadata:
        if doc_lines[0].strip() != f"# {connector.metadata['name'].lower()}":
            print("The connector name is not used as the main header in the documentation.")
            follows_guidelines = False
    elif not doc_lines[0].startswith("# "):
        print("The connector name is not used as the main header in the documentation.")
        follows_guidelines = False

    expected_sections = ["## Prerequisites", "## Setup guide", "## Supported sync modes", "## Supported streams", "## Changelog"]

    for expected_section in expected_sections:
        if expected_section.lower() not in doc_lines:
            print(f"Connector documentation is missing a '{expected_section.replace('#', '').strip()}' section.")
            follows_guidelines = False
    return follows_guidelines


def check_changelog_entry_is_updated(connector: Connector) -> bool:
    """Check that the changelog entry is updated for the latest connector version
    in docs/integrations/<connector-type>/<connector-name>.md

    Args:
        connector (Connector): a Connector dataclass instance.

    Returns:
        bool: Wether a the changelog is up to date.
    """
    if not check_documentation_file_exists(connector):
        return False
    with open(connector.documentation_file_path) as f:
        after_changelog = False
        for line in f:
            if "# changelog" in line.lower():
                after_changelog = True
            if after_changelog and connector.version in line:
                return True
    return False


def check_connector_icon_is_available(connector: Connector) -> bool:
    """Check an SVG icon exists for a connector in
    in airbyte-config-oss/init-oss/src/main/resources/icons/<connector-name>.svg

    Args:
        connector (Connector): a Connector dataclass instance.

    Returns:
        bool: Wether an icon exists for this connector.
    """
    return connector.icon_path.exists()


def read_all_files_in_directory(
    directory: Path,
    ignored_directories: Optional[set[str]] = None,
    ignored_filename_patterns: Optional[set[str]] = None,
) -> Iterable[tuple[str, str]]:
    ignored_directories = ignored_directories if ignored_directories is not None else {}
    ignored_filename_patterns = ignored_filename_patterns if ignored_filename_patterns is not None else {}

    for path in directory.rglob("*"):
        ignore_directory = any([ignored_directory in path.parts for ignored_directory in ignored_directories])
        ignore_filename = any([path.match(ignored_filename_pattern) for ignored_filename_pattern in ignored_filename_patterns])
        ignore = ignore_directory or ignore_filename
        if path.is_file() and not ignore:
            try:
                for line in open(path):
                    yield path, line
            except UnicodeDecodeError:
                print(f"{path} could not be decoded as it is not UTF8.")
                continue


IGNORED_DIRECTORIES_FOR_HTTPS_CHECKS = {
    ".venv",
    "tests",
    "unit_tests",
    "integration_tests",
    "build",
    "source-file",
    ".pytest_cache",
    "acceptance_tests_logs",
    ".hypothesis",
}

IGNORED_FILENAME_PATTERN_FOR_HTTPS_CHECKS = {
    "*Test.java",
    "*.jar",
    "*.pyc",
    "*.gz",
    "*.svg",
    "expected_records.jsonl",
    "expected_records.json",
}
IGNORED_URLS_PREFIX = {
    "http://json-schema.org",
    "http://localhost",
}


def is_comment(line: str, file_path: Path):
    language_comments = {
        ".py": "#",
        ".yml": "#",
        ".yaml": "#",
        ".java": "//",
        ".md": "<!--",
    }

    denote_comment = language_comments.get(file_path.suffix)
    if not denote_comment:
        return False

    trimmed_line = line.lstrip()
    return trimmed_line.startswith(denote_comment)


def check_connector_https_url_only(connector: Connector) -> bool:
    """Check a connector code contains only https url.

    Args:
        connector (Connector): a Connector dataclass instance.

    Returns:
        bool: Wether the connector code contains only https url.
    """
    files_with_http_url = set()
    ignore_comment = "# ignore-https-check"  # Define the ignore comment pattern

    for filename, line in read_all_files_in_directory(
        connector.code_directory,
        IGNORED_DIRECTORIES_FOR_HTTPS_CHECKS,
        IGNORED_FILENAME_PATTERN_FOR_HTTPS_CHECKS,
    ):
        line = line.lower()
        if is_comment(line, filename):
            continue
        if ignore_comment in line:
            continue
        for prefix in IGNORED_URLS_PREFIX:
            line = line.replace(prefix, "")
        if "http://" in line:
            files_with_http_url.add(str(filename))

    if files_with_http_url:
        files_with_http_url = "\n\t- ".join(files_with_http_url)
        print(f"The following files have http:// URLs:\n\t- {files_with_http_url}")
        return False
    return True


def check_connector_has_no_critical_vulnerabilities(connector: Connector) -> bool:
    """Check if the connector image is free of critical Snyk vulnerabilities.
    Runs a docker scan command.

    Args:
        connector (Connector): a Connector dataclass instance.

    Returns:
        bool: Wether the connector is free of critical vulnerabilities.
    """
    # TODO implement
    return True


def check_metadata_version_matches_dockerfile_label(connector: Connector) -> bool:
    version_in_dockerfile = connector.version_in_dockerfile_label
    if version_in_dockerfile is None:
        # Java connectors don't have Dockerfiles.
        return connector.language == ConnectorLanguage.JAVA
    return version_in_dockerfile == connector.version


DEFAULT_QA_CHECKS = (
    check_documentation_file_exists,
    check_migration_guide,
    # Disabling the following check because it's likely to not pass on a lot of connectors.
    # check_documentation_follows_guidelines,
    check_changelog_entry_is_updated,
    check_connector_icon_is_available,
    # TODO - check_connector_https_url_only might be redundant once the following issues are closed
    # https://github.com/airbytehq/airbyte/issues/20552
    # https://github.com/airbytehq/airbyte/issues/21606
    check_connector_https_url_only,
    check_connector_has_no_critical_vulnerabilities,
)


def get_qa_checks_to_run(connector: Connector) -> tuple[Callable]:
    if connector.has_dockerfile:
        return DEFAULT_QA_CHECKS + (check_metadata_version_matches_dockerfile_label,)
    return DEFAULT_QA_CHECKS


def remove_strict_encrypt_suffix(connector_technical_name: str) -> str:
    """Remove the strict encrypt suffix from a connector name.

    Args:
        connector_technical_name (str): the connector name.

    Returns:
        str: the connector name without the strict encrypt suffix.
    """
    strict_encrypt_suffixes = [
        "-strict-encrypt",
        "-secure",
    ]

    for suffix in strict_encrypt_suffixes:
        if connector_technical_name.endswith(suffix):
            new_connector_technical_name = connector_technical_name.replace(suffix, "")
            print("Checking connector " + new_connector_technical_name + " due to strict-encrypt")
            return new_connector_technical_name
    return connector_technical_name


def run_qa_checks():
    connector_technical_name = sys.argv[1].split("/")[-1]
    if not connector_technical_name.startswith("source-") and not connector_technical_name.startswith("destination-"):
        print("No QA check to run as this is not a connector.")
        sys.exit(0)

    connector_technical_name = remove_strict_encrypt_suffix(connector_technical_name)
    connector = Connector(connector_technical_name)
    print(f"Running QA checks for {connector_technical_name}:{connector.version}")
    qa_check_results = {qa_check.__name__: qa_check(connector) for qa_check in get_qa_checks_to_run(connector)}
    if not all(qa_check_results.values()):
        print(f"QA checks failed for {connector_technical_name}:{connector.version}:")
        for check_name, check_result in qa_check_results.items():
            check_result_prefix = "✅" if check_result else "❌"
            print(f"{check_result_prefix} - {check_name}")
        sys.exit(1)
    else:
        print(f"All QA checks succeeded for {connector_technical_name}:{connector.version}")
        sys.exit(0)
