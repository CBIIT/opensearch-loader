#!/usr/bin/env python3
"""Validate a Memgraph database before loading it into OpenSearch."""

import argparse
import getpass
import sys
from collections import defaultdict
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

import yaml
from neo4j import GraphDatabase


USER_AGENT = "opensearch-loader-preload-validation"
CHECK_NAMES = {
    1: "Memgraph connectivity",
    2: "Database counts",
    3: "Required properties",
    4: "Enum values",
    5: "Property types",
}


def heading(step, title):
    print(f"\n{'=' * 72}\nSTEP {step}: {title}\n{'=' * 72}")


def prompt_for_model_tag():
    tag = input("Datamodel tag (for example, 1.0.0): ").strip()
    if not tag:
        raise ValueError("A datamodel tag is required")
    return tag


def normalize_model_repository(value):
    value = value.strip().rstrip("/")
    if not value:
        raise ValueError("A datamodel repository is required")

    parsed = urlparse(value)
    if parsed.scheme:
        if parsed.scheme not in {"http", "https"} or parsed.netloc.lower() not in {
            "github.com",
            "www.github.com",
        }:
            raise ValueError("The datamodel repository must be hosted on GitHub")
        parts = [part for part in parsed.path.split("/") if part]
    else:
        parts = [part for part in value.split("/") if part]

    if len(parts) != 2:
        raise ValueError(
            "Use owner/repository or https://github.com/owner/repository"
        )
    owner, repository_name = parts
    if repository_name.endswith(".git"):
        repository_name = repository_name[:-4]
    if not owner or not repository_name:
        raise ValueError("The repository owner and name are required")
    return f"{owner}/{repository_name}"


def prompt_for_model_repository():
    value = input(
        "Datamodel repository (owner/repository or GitHub URL): "
    )
    return normalize_model_repository(value)


def parse_check_selection(value):
    value = value.strip().lower()
    if not value or value == "all":
        return set(CHECK_NAMES)

    selected = set()
    for part in value.split(","):
        part = part.strip()
        if not part:
            raise ValueError("Check selections cannot contain empty items")
        if "-" in part:
            bounds = part.split("-", 1)
            try:
                start, end = (int(bound.strip()) for bound in bounds)
            except ValueError as error:
                raise ValueError(f"Invalid check range: {part!r}") from error
            if start > end:
                raise ValueError(f"Check range must be ascending: {part!r}")
            selected.update(range(start, end + 1))
        else:
            try:
                selected.add(int(part))
            except ValueError as error:
                raise ValueError(f"Invalid check number: {part!r}") from error

    invalid = selected - set(CHECK_NAMES)
    if invalid:
        values = ", ".join(map(str, sorted(invalid)))
        raise ValueError(f"Unknown check number(s): {values}")
    if not selected:
        raise ValueError("At least one check must be selected")
    if selected - {1}:
        selected.add(1)
    return selected


def prompt_for_checks_text():
    print("\nAvailable validation checks")
    for number, name in CHECK_NAMES.items():
        print(f"  {number}. {name}")
    value = input(
        "Checks to run [all] (examples: 1,3,5 or 2-5): "
    )
    return parse_check_selection(value)


def toggle_check_selection(selected, item):
    all_checks = set(CHECK_NAMES)
    if item == 0:
        return set() if selected == all_checks else all_checks
    if item == 1 and selected - {1}:
        return set(selected)
    updated = set(selected)
    if item in updated:
        updated.remove(item)
    else:
        updated.add(item)
    if updated - {1}:
        updated.add(1)
    return updated


def read_checkbox_key():
    value = sys.stdin.read(1)
    if value == "":
        raise EOFError
    if value == "\x03":
        raise KeyboardInterrupt
    if value == "\x1b":
        remainder = sys.stdin.read(2)
        if remainder == "[A":
            return "up"
        if remainder == "[B":
            return "down"
        return "other"
    if value in {"\r", "\n"}:
        return "enter"
    if value == " ":
        return "toggle"
    if value.lower() == "k":
        return "up"
    if value.lower() == "j":
        return "down"
    return "other"


def render_checkboxes(items, selected, cursor, *, redraw=False):
    if redraw:
        sys.stdout.write(f"\x1b[{len(items)}A")
    for index, (number, name) in enumerate(items):
        checked = selected == set(CHECK_NAMES) if number == 0 else number in selected
        pointer = ">" if index == cursor else " "
        mark = "x" if checked else " "
        sys.stdout.write(f"\x1b[2K\r{pointer} [{mark}] {name}\r\n")
    sys.stdout.flush()


def prompt_for_checks_checkbox():
    import termios
    import tty

    items = [(0, "All checks")] + list(CHECK_NAMES.items())
    selected = set(CHECK_NAMES)
    cursor = 0
    print("\nSelect validation checks")
    print("Use Up/Down (or j/k) to move, Space to toggle, and Enter to continue.")
    print("Memgraph connectivity is selected automatically for other checks.")

    file_descriptor = sys.stdin.fileno()
    original_settings = termios.tcgetattr(file_descriptor)
    sys.stdout.write("\x1b[?25l")
    sys.stdout.flush()
    try:
        tty.setraw(file_descriptor)
        render_checkboxes(items, selected, cursor)
        while True:
            key = read_checkbox_key()
            if key == "up":
                cursor = (cursor - 1) % len(items)
            elif key == "down":
                cursor = (cursor + 1) % len(items)
            elif key == "toggle":
                selected = toggle_check_selection(selected, items[cursor][0])
            elif key == "enter":
                if selected:
                    break
                sys.stdout.write("\a")
                sys.stdout.flush()
            else:
                continue
            render_checkboxes(items, selected, cursor, redraw=True)
    finally:
        termios.tcsetattr(file_descriptor, termios.TCSADRAIN, original_settings)
        sys.stdout.write("\x1b[?25h")
        sys.stdout.flush()
    return parse_check_selection(",".join(map(str, sorted(selected))))


def prompt_for_checks():
    if sys.stdin.isatty() and sys.stdout.isatty():
        try:
            return prompt_for_checks_checkbox()
        except (ImportError, OSError):
            pass
    return prompt_for_checks_text()


def prompt_for_memgraph_connection():
    print("Memgraph connection details")
    host = input("Host: ").strip()
    if not host:
        raise ValueError("Host is required")

    port = input("Port [7687]: ").strip() or "7687"
    username = input("Username: ").strip()
    password = getpass.getpass("Password: ")
    tls_mode = input(
        "TLS mode [none/verified/self-signed] (default none): "
    ).strip().lower() or "none"
    schemes = {
        "none": "bolt",
        "verified": "bolt+s",
        "self-signed": "bolt+ssc",
    }
    if tls_mode not in schemes:
        raise ValueError("TLS mode must be none, verified, or self-signed")

    uri = f"{schemes[tls_mode]}://{host}:{port}"
    auth = (username, password) if username else None
    return uri, auth


def raw_url(repository, tag, path):
    owner, repository_name = repository.split("/", 1)
    return (
        "https://raw.githubusercontent.com/"
        f"{quote(owner, safe='')}/{quote(repository_name, safe='')}/"
        f"{quote(tag, safe='')}/{path}"
    )


def fetch_yaml(url):
    request = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(request, timeout=30) as response:
            return yaml.safe_load(response.read())
    except (HTTPError, URLError, TimeoutError, yaml.YAMLError) as error:
        raise RuntimeError(f"Could not read YAML from {url}: {error}") from error


def load_datamodel(repository, tag):
    repository_name = repository.split("/", 1)[1]
    properties_path = f"model-desc/{repository_name}-props.yml"
    model_path = f"model-desc/{repository_name}.yml"
    properties = fetch_yaml(raw_url(repository, tag, properties_path))
    model = fetch_yaml(raw_url(repository, tag, model_path))
    if not isinstance(properties, dict) or not isinstance(
        properties.get("PropDefinitions"), dict
    ):
        raise ValueError(f"{properties_path} has no PropDefinitions mapping")
    if not isinstance(model, dict) or not isinstance(model.get("Nodes"), dict):
        raise ValueError(f"{model_path} has no Nodes mapping")
    return properties["PropDefinitions"], model["Nodes"]


def connect(uri, auth):
    driver = GraphDatabase.driver(uri, auth=auth)
    try:
        with driver.session() as session:
            session.run("RETURN 1 AS connected").consume()
    except Exception:
        driver.close()
        raise
    return driver


def database_counts(driver):
    with driver.session() as session:
        node_count = session.run("MATCH (n) RETURN count(n) AS count").single()[
            "count"
        ]
        relationship_count = session.run(
            "MATCH ()-[r]->() RETURN count(r) AS count"
        ).single()["count"]
        rows = session.run(
            """MATCH (n)
UNWIND labels(n) AS node_type
RETURN node_type, count(*) AS count
ORDER BY node_type"""
        )
        counts_by_type = [(row["node_type"], row["count"]) for row in rows]
    return node_count, relationship_count, counts_by_type


def cypher_identifier(value):
    """Quote a model-controlled Cypher identifier."""
    return f"`{str(value).replace('`', '``')}`"


def node_property_rules(nodes, property_definitions):
    rules = []
    for node_type, node_definition in nodes.items():
        if str(node_type).startswith("_") or not isinstance(node_definition, dict):
            continue
        for property_name in node_definition.get("Props") or []:
            definition = property_definitions.get(property_name)
            if isinstance(definition, dict):
                rules.append((node_type, property_name, definition))
    return rules


def validate_required_properties(driver, rules):
    violations = []
    checked = 0
    with driver.session() as session:
        for node_type, property_name, definition in rules:
            if definition.get("Req") is not True:
                continue
            checked += 1
            node = cypher_identifier(node_type)
            prop = cypher_identifier(property_name)
            query = f"""MATCH (n:{node})
WHERE n.{prop} IS NULL OR n.{prop} = '' OR n.{prop} = []
RETURN count(n) AS count"""
            count = session.run(query).single()["count"]
            if count:
                violations.append((node_type, property_name, count))
    return checked, violations


def is_resource_url(value):
    if not isinstance(value, str):
        return False
    return urlparse(value).scheme in {"http", "https"}


def external_enum_values(resource, property_name, url):
    if isinstance(resource, list):
        return resource
    if not isinstance(resource, dict):
        raise ValueError(f"External enum {url} is not a mapping or list")

    definitions = resource.get("PropDefinitions", resource)
    if not isinstance(definitions, dict):
        raise ValueError(f"External enum {url} has no definitions mapping")

    values = definitions.get(property_name)
    if values is None and len(definitions) == 1:
        # A property may intentionally reuse the only vocabulary in a file.
        values = next(iter(definitions.values()))
    if not isinstance(values, list):
        raise ValueError(f"External enum {url} has no list for {property_name}")
    return values


def enum_values(property_name, definition, cache):
    values = []
    for value in definition.get("Enum") or []:
        if not is_resource_url(value):
            values.append(value)
            continue
        url = value
        if url not in cache:
            cache[url] = fetch_yaml(url)
        values.extend(external_enum_values(cache[url], property_name, url))
    return list(dict.fromkeys(values))


def validate_enums(driver, rules):
    violations = []
    checked = 0
    cache = {}
    resolved = {}
    with driver.session() as session:
        for node_type, property_name, definition in rules:
            if not definition.get("Enum") or definition.get("Strict") is False:
                continue
            if property_name not in resolved:
                resolved[property_name] = enum_values(
                    property_name, definition, cache
                )
            allowed = resolved[property_name]
            if not allowed:
                raise ValueError(f"Enum for {property_name} has no allowed values")
            checked += 1
            node = cypher_identifier(node_type)
            prop = cypher_identifier(property_name)
            query = f"""MATCH (n:{node})
WHERE n.{prop} IS NOT NULL AND NOT n.{prop} IN $allowed_values
RETURN n.{prop} AS value, count(*) AS count"""
            for row in session.run(query, allowed_values=allowed):
                violations.append(
                    (node_type, property_name, row["value"], row["count"])
                )
    return checked, violations


def expected_property_type(definition):
    type_definition = definition.get("Type")
    if type_definition == "string" or (
        isinstance(type_definition, dict) and "pattern" in type_definition
    ):
        return "string"
    if type_definition == "number":
        return "number"
    if type_definition == "integer":
        return "integer"
    if isinstance(type_definition, dict):
        if type_definition.get("value_type") == "list":
            return "list[string]"
        return None
    if type_definition is None and definition.get("Enum"):
        return "string"
    return None


def value_matches_type(value, expected):
    if expected == "string":
        return isinstance(value, str)
    if expected == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "list[string]":
        return isinstance(value, list) and all(
            isinstance(item, str) for item in value
        )
    return False


def actual_type(value):
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, str):
        return "string"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, list):
        item_types = sorted({actual_type(item) for item in value})
        content = ",".join(item_types) if item_types else "empty"
        return f"list[{content}]"
    return type(value).__name__


def example_value(value, limit=120):
    rendered = repr(value)
    return rendered if len(rendered) <= limit else f"{rendered[:limit - 3]}..."


def validate_property_types(driver, rules):
    expected_by_node = defaultdict(dict)
    for node_type, property_name, definition in rules:
        expected = expected_property_type(definition)
        if expected is not None:
            expected_by_node[node_type][property_name] = expected

    violations = {}
    checked = 0
    unmodeled = 0
    with driver.session() as session:
        rows = session.run(
            "MATCH (n) RETURN labels(n) AS node_types, properties(n) AS properties"
        )
        for row in rows:
            properties = row["properties"]
            for node_type in row["node_types"]:
                expectations = expected_by_node.get(node_type, {})
                for property_name, value in properties.items():
                    expected = expectations.get(property_name)
                    if expected is None:
                        unmodeled += 1
                        continue
                    checked += 1
                    if value_matches_type(value, expected):
                        continue
                    key = (
                        node_type,
                        property_name,
                        expected,
                        actual_type(value),
                    )
                    detail = violations.setdefault(
                        key, {"count": 0, "examples": []}
                    )
                    detail["count"] += 1
                    example = example_value(value)
                    if example not in detail["examples"] and len(
                        detail["examples"]
                    ) < 3:
                        detail["examples"].append(example)

    ordered = [
        (*key, detail["count"], detail["examples"])
        for key, detail in sorted(violations.items())
    ]
    rule_count = sum(len(values) for values in expected_by_node.values())
    return rule_count, checked, unmodeled, ordered


def print_required_result(checked, violations):
    print(f"Required node/property rules checked: {checked}")
    print(f"Nodes with null or empty required values: {sum(v[2] for v in violations)}")
    if not violations:
        print("PASS: Every required property is populated.")
        return
    print("FAIL: Required-property violations:")
    for node_type, property_name, count in violations:
        print(f"  {node_type}.{property_name}: {count}")


def print_enum_result(checked, violations):
    print(f"Enum node/property rules checked: {checked}")
    print(f"Nodes with illegal enum values: {sum(v[3] for v in violations)}")
    if not violations:
        print("PASS: Every populated enum property has a legal value.")
        return
    print("FAIL: Illegal enum values:")
    for node_type, property_name, value, count in violations:
        print(f"  {node_type}.{property_name} = {value!r}: {count}")


def print_type_result(rule_count, checked, unmodeled, violations):
    print(f"Node/property type rules available: {rule_count}")
    print(f"Populated modeled property values checked: {checked}")
    print(f"Populated unmodeled property values skipped: {unmodeled}")
    print(f"Property values with incorrect types: {sum(v[4] for v in violations)}")
    if not violations:
        print("PASS: Every populated modeled property has the expected type.")
        return
    print("FAIL: Property-type violations:")
    for node_type, property_name, expected, actual, count, examples in violations:
        print(
            f"  {node_type}.{property_name}: expected {expected}, "
            f"found {actual}: {count}"
        )
        print(f"    examples: {', '.join(examples)}")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--model-repo",
        help="GitHub datamodel repository; prompted for when omitted",
    )
    parser.add_argument(
        "--model-tag",
        help="datamodel tag; prompted for when omitted",
    )
    parser.add_argument(
        "--checks",
        help="checks to run (all, comma-separated numbers, or ranges)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    print("Preload database validation")
    selected = (
        parse_check_selection(args.checks) if args.checks else prompt_for_checks()
    )
    print(
        "Selected checks: "
        + ", ".join(
            f"{number} ({CHECK_NAMES[number]})" for number in sorted(selected)
        )
    )
    needs_model = bool(selected & {3, 4, 5})
    repository = None
    if needs_model:
        repository = (
            normalize_model_repository(args.model_repo)
            if args.model_repo
            else prompt_for_model_repository()
        )
    tag = (args.model_tag or prompt_for_model_tag()) if needs_model else None
    uri, auth = prompt_for_memgraph_connection()

    heading(1, "Memgraph connectivity")
    driver = connect(uri, auth)
    print(f"PASS: Connected to {uri}")

    required_violations = []
    enum_violations = []
    type_violations = []
    try:
        if 2 in selected:
            heading(2, "Database counts")
            node_count, relationship_count, counts_by_type = database_counts(driver)
            print(f"Total nodes: {node_count}")
            print(f"Total relationships: {relationship_count}")
            print("Node counts by label:")
            if counts_by_type:
                for node_type, count in counts_by_type:
                    print(f"  {node_type}: {count}")
            else:
                print("  (none)")

        if needs_model:
            print(f"\nReading datamodel tag {tag!r} from {repository} ...")
            property_definitions, nodes = load_datamodel(repository, tag)
            rules = node_property_rules(nodes, property_definitions)
            print(
                f"Loaded {len(nodes)} node types and "
                f"{len(property_definitions)} property definitions."
            )

        if 3 in selected:
            heading(3, "Required properties")
            required_checked, required_violations = validate_required_properties(
                driver, rules
            )
            print_required_result(required_checked, required_violations)

        if 4 in selected:
            heading(4, "Enum values")
            enum_checked, enum_violations = validate_enums(driver, rules)
            print_enum_result(enum_checked, enum_violations)

        if 5 in selected:
            heading(5, "Property types")
            (
                type_rule_count,
                type_checked,
                unmodeled_values,
                type_violations,
            ) = validate_property_types(driver, rules)
            print_type_result(
                type_rule_count,
                type_checked,
                unmodeled_values,
                type_violations,
            )
    finally:
        driver.close()

    failed = bool(required_violations or enum_violations or type_violations)
    heading("SUMMARY", "Validation result")
    if failed:
        print("FAIL: One or more validation checks found data problems.")
        return 1
    print("PASS: All validation checks completed without data problems.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (KeyboardInterrupt, EOFError):
        print("\nValidation cancelled.", file=sys.stderr)
        sys.exit(130)
    except Exception as error:
        print(f"\nERROR: {error}", file=sys.stderr)
        sys.exit(2)
