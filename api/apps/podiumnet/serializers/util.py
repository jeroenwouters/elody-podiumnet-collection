from app_context import g, request  # pyright: ignore
from apps.podiumnet.validation.util import (
    get_required_properties,
    get_schema_properties,
)
from datetime import datetime
from importlib import import_module


def get_aliases(property: str, property_value_map: dict):
    if value := property_value_map.get(property):
        return value.get("_customAttributes", {}).get("aliases", [property])
    return []


def get_iso_date(value):
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def get_parser(property: str, property_value_map: dict, property_type: str):
    parser = (
        property_value_map.get(property, {}).get("_customAttributes", {}).get("parser")
    )
    try:
        if not g.get("enable_parsers"):
            raise Exception("Parsers not enabled")
        return import_module(f"apps.podiumnet.parsers.{parser}").parse
    except Exception:
        if property == "_id":
            return lambda value, **_: value
        elif property_type == "metadata":
            return lambda value, **_: [{"key": property, "value": value}]
        elif property_type == "relations":
            return lambda value, **_: [{"key": value, "type": property}]
        else:
            raise Exception(f"Property type '{property_type}' not supported.")


def get_parser_reverse(property: str, property_value_map: dict):
    parser = (
        property_value_map.get(property, {}).get("_customAttributes", {}).get("parser")
    )
    try:
        if not g.get("enable_parsers"):
            raise Exception("Parsers not enabled")
        return import_module(f"apps.podiumnet.parsers.{parser}").parse_reverse
    except Exception:
        return lambda value, **_: value


def get_properties(key: str, property_value_map: dict):
    properties = []
    key = key.lower().strip().replace(" ", "_")
    for property, value in property_value_map.items():
        aliases = value.get("_customAttributes", {}).get("aliases", [])
        if property == key or key in aliases:
            properties.append(property)
    return properties


def get_property_value_map(document_type: str):
    return get_schema_properties(document_type)


def get_user_requested_properties(property_value_map):
    fields = request.args.getlist("field") or property_value_map.keys()
    return [
        property
        for property in property_value_map.keys()
        if (
            property in fields
            or any(
                alias in fields for alias in get_aliases(property, property_value_map)
            )
        )
    ]


def is_exportable(property: str, property_value_map: dict):
    if property_value := property_value_map.get(property, {}):
        return property_value.get("_customAttributes", {}).get(
            "exportable", True
        ) and not property_value.get("_customAttributes", {}).get("virtual", False)
    return False


def is_immutable(property: str, property_value_map: dict):
    if property_value_map and request.method != "PUT":
        if property_value := property_value_map.get(property, {}):
            return request.method in ["PATCH", "DELETE"] and property_value.get(
                "_customAttributes", {}
            ).get("immutable", False)
        return True
    return False


def is_readonly(property: str, property_value_map: dict):
    if request.method != "PUT":
        return (
            property_value_map.get(property, {})
            .get("_customAttributes", {})
            .get("readonly", False)
        )
    return False


def is_required_property(property: str, document_type: str):
    return property in get_required_properties(document_type)
