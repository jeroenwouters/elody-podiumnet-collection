SCHEMA_TEMPLATE = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "_id": {"minLength": 1, "type": "string"},
        "identifiers": {
            "items": {"minLength": 1, "type": "string"},
            "type": "array",
            "uniqueItems": True,
        },
        "created_by": {"type": "string"},
        "date_created": {},
        "date_updated": {},
        "last_editor": {"type": "string"},
        "metadata": {
            "REQUIRED_METADATA": [],
            "items": {
                "type": "object",
                "properties": {
                    "key": {"minLength": 1, "type": "string"},
                    "value": {},
                },
                "required": ["key", "value"],
                "additionalProperties": True,
                "METADATA": [],
            },
            "type": "array",
            "uniqueItems": True,
        },
        "relations": {
            "REQUIRED_RELATIONS": [],
            "items": {
                "type": "object",
                "properties": {
                    "key": {"minLength": 1, "type": "string"},
                    "type": {"minLength": 1, "type": "string"},
                },
                "required": ["key", "type"],
                "additionalProperties": True,
                "RELATIONS": [],
            },
            "type": "array",
            "uniqueItems": True,
        },
        "schema": {
            "type": "object",
            "properties": {
                "type": {"minLength": 1, "type": "string"},
                "version": {"minimum": 1, "type": "number"},
            },
            "required": ["type"],
            "additionalProperties": False,
        },
        "type": {"type": "string"},
    },
    "required": ["type"],
    "additionalProperties": True,
}
