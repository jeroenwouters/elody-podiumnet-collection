from app_context import request  # pyright: ignore
from apps.podiumnet.validation.dams_validator import (SCHEMA_TEMPLATE)
from importlib import import_module
from json import loads


def get_schema(document_type):
    if not document_type:
        return {}
    elif document_type == "elody":
        return __construct_schema()

    validator = __import_module(document_type)
    metadata_map = validator.METADATA_MAP
    required_metadata = validator.REQUIRED_METADATA
    relations_map = validator.RELATIONS_MAP
    required_relations = validator.REQUIRED_RELATIONS

    return __construct_schema(
        metadata_map, required_metadata, relations_map, required_relations
    )


def get_schema_properties(document_type):
    if not document_type or document_type == "elody":
        return {}

    try:
        validator = __import_module(document_type)
    except ModuleNotFoundError:
        return {}
    return {**validator.METADATA_MAP, **validator.RELATIONS_MAP}


def get_required_properties(document_type):
    if not document_type or document_type == "elody":
        return []

    try:
        validator = __import_module(document_type)
    except ModuleNotFoundError:
        return []
    return [*validator.REQUIRED_METADATA, *validator.REQUIRED_RELATIONS]


def get_virtual_properties(document_type):
    if not document_type or document_type == "elody":
        return []

    try:
        validator = __import_module(document_type)
        properties = {**validator.METADATA_MAP, **validator.RELATIONS_MAP}
        return [
            key
            for key, property in properties.items()
            if property.get("_customAttributes", {}).get("virtual")
        ]
    except ModuleNotFoundError:
        return []


def get_hierarchy_properties_in_order(document_type, reverse=False):
    if not document_type or document_type == "elody":
        return []

    try:
        validator = __import_module(document_type)
        properties = {**validator.METADATA_MAP, **validator.RELATIONS_MAP}
        properties = dict(
            sorted(
                properties.items(),
                key=lambda property: property[1]
                .get("_customAttributes", {})
                .get("hierarchy_level", float("inf")),
                reverse=reverse,
            )
        )
        return [
            key
            for key, property in properties.items()
            if property.get("_customAttributes", {}).get("hierarchy_level")
        ]
    except ModuleNotFoundError:
        return []


def __construct_schema(
    metadata_map={}, required_metadata=[], relations_map={}, required_relations=[]
) -> dict:
    metadata, required_metadata = __construct_metadata(metadata_map, required_metadata)
    relations, required_relations = __construct_relations(
        relations_map, required_relations
    )

    return loads(
        str(SCHEMA_TEMPLATE)
        .replace(", 'METADATA': []", f", 'oneOf': {metadata}" if metadata else "")
        .replace(
            "'REQUIRED_METADATA': [],",
            (
                f"'allOf': {required_metadata},"
                if required_metadata
                and request.method != "PATCH"
                and request.endpoint != "elody.elodydocumentrelations"
                else ""
            ),
        )
        .replace(", 'RELATIONS': []", f", 'oneOf': {relations}" if relations else "")
        .replace(
            "'REQUIRED_RELATIONS': [],",
            (
                f"'allOf': {required_relations},"
                if required_relations
                and request.method != "PATCH"
                and request.endpoint != "elody.elodydocumentrelations"
                else ""
            ),
        )
        .replace("'", '"')
        .replace("False", "false")
        .replace("True", "true")
    )


def __construct_metadata(metadata_map={}, required_metadata=[]):
    metadata = []
    for key, value in metadata_map.items():
        metadata.append(
            {
                "type": "object",
                "properties": {
                    "key": {"const": key},
                    "value": {**value, "_property": key},
                },
                "required": ["key", "value"],
                "additionalProperties": True,
                "_customAttributes": value.get("_customAttributes", {}),
            }
        )
    metadata.append(
        {
            "type": "object",
            "properties": {
                "key": {"not": {"enum": list(metadata_map.keys())}},
                "value": {
                    "type": [
                        "array",
                        "boolean",
                        "null",
                        "number",
                        "object",
                        "string",
                    ]
                },
            },
            "required": ["key", "value"],
            "additionalProperties": True,
        }
    )

    required_metadata = [
        {
            "contains": {
                "properties": {"key": {"const": key}},
                "required": ["key"],
            },
            "minContains": 1,
            "maxContains": 1,
            "_customAttributes": value.get("_customAttributes", {}),
        }
        for key, value in metadata_map.items()
        if key in required_metadata
    ]

    return metadata, required_metadata


def __construct_relations(relations_map={}, required_relations=[]):
    relations = []
    for type, key in relations_map.items():
        relations.append(
            {
                "type": "object",
                "properties": {"type": {"const": type}, "key": key},
                "required": ["type", "key"],
                "additionalProperties": True,
            }
        )
    required_relations = [
        {
            "contains": {
                "properties": {"type": {"const": type}},
                "required": ["type"],
            },
            "minContains": 1,
        }
        for type in required_relations
    ]
    return relations, required_relations


def __import_module(document_type):
    paths = [
        f"apps.podiumnet.validation.{document_type}_validator",
        f"apps.podiumnet.validation.wrapper_object_validators.{document_type}_validator",
    ]
    for path in paths:
        try:
            return import_module(path)
        except ModuleNotFoundError:
            pass
    raise ModuleNotFoundError(paths)
