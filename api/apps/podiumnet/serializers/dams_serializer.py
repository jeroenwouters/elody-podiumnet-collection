from apps.podiumnet.serializers.util import (
    get_aliases,
    get_iso_date,
    get_parser,
    get_parser_reverse,
    get_properties,
    get_property_value_map,
    get_user_requested_properties,
    is_exportable,
    is_immutable,
    is_readonly,
    is_required_property,
)
from copy import deepcopy
from elody.error_codes import ErrorCode, get_error_code, get_read, get_write
from io import BytesIO
from pandas import DataFrame, read_csv
from requests.exceptions import HTTPError
from serialization.case_converter import camel_to_snake  # pyright: ignore
from werkzeug.exceptions import BadRequest


class DamsSerializer:
    def from_dams_to_elody(self, document, **_):
        if not document:
            return {}

        serialized_document = {
            **deepcopy(document),
            "schema": {"type": "elody", "version": 1},
        }
        document_keys = list(serialized_document.keys())
        for key in document_keys:
            if key.startswith("__"):
                del serialized_document[key]
        return serialized_document

    def from_elody_to_dams(self, document, **_):
        if not document:
            return {}

        property_value_map = get_property_value_map(document["type"])
        serialized_document = {
            **deepcopy(document),
            "metadata": [],
            "relations": [],
            "schema": {"type": "dams", "version": 1},
        }
        for key, value in serialized_document.items():
            if value := get_iso_date(value):
                serialized_document[key] = value

        for metadata in document.get("metadata", []):
            if not is_immutable(
                metadata["key"], property_value_map
            ) and not is_readonly(metadata["key"], property_value_map):
                if value := get_iso_date(metadata["value"]):
                    metadata["value"] = value
                serialized_document["metadata"].append(metadata)
        for relation in document.get("relations", []):
            if not is_immutable(
                relation["type"], property_value_map
            ) and not is_readonly(relation["type"], property_value_map):
                serialized_document["relations"].append(relation)

        return serialized_document

    def from_dams_to_textcsv(self, documents, *, document_type, **_):
        data_frame = self._parse_dams_to_dataframe(documents, document_type)
        return data_frame.to_csv(index=False)

    def from_textcsv_to_dams(self, data, *, document_type, **_):
        property_value_map = get_property_value_map(document_type)

        skip = 0
        limit = 500
        while True:
            data_frame = read_csv(
                BytesIO(data),
                sep=None,
                engine="python",
                na_filter=False,
                skiprows=list(range(1, skip + 1)),
                nrows=limit,
            )
            if data_frame.empty:
                break
            for row in data_frame.itertuples(index=False):
                yield self._parse_dataframe_to_dams(
                    data_frame.columns, row, document_type, property_value_map
                )
            skip += limit

        return []

    def _parse_dams_to_dataframe(self, documents, document_type):
        property_value_map = get_property_value_map(document_type)
        properties = get_user_requested_properties(property_value_map)
        data = []

        for document in documents:
            base_frame_data = {"type": document_type}
            for property in properties:
                if is_required_property(property, document_type) or (
                    document_type == "mediafile" and property == "file_identifier"
                ):  # this ugly hardcoded condition is temporary until data format change
                    parse = get_parser_reverse(property, property_value_map)
                    for alias in get_aliases(property, property_value_map):
                        value = (
                            [
                                metadata["value"]
                                for metadata in document["metadata"]
                                if metadata["key"] == property
                            ]
                            + [
                                relation["key"]
                                for relation in document["relations"]
                                if relation["type"] == property
                            ]
                        )[0]
                        if not base_frame_data.get(alias):
                            base_frame_data.update({alias: parse(value)})

            frame_data = {}
            for property in properties:
                if (
                    camel_to_snake(property).startswith("has_")
                    or (
                        camel_to_snake(property).startswith("is_")
                        and camel_to_snake(property).endswith("_for")
                    )
                    or not is_exportable(property, property_value_map)
                ):
                    continue
                parse = get_parser_reverse(property, property_value_map)
                for alias in get_aliases(property, property_value_map):
                    values = [
                        metadata["value"]
                        for metadata in document["metadata"]
                        if metadata["key"] == property
                    ]
                    if not base_frame_data.get(alias) and not frame_data.get(alias):
                        frame_data.update({alias: parse(values[0]) if values else ""})

            multi_ref_properties = []
            for property in properties:
                if (
                    not camel_to_snake(property).startswith("has_")
                    and not (
                        camel_to_snake(property).startswith("is_")
                        and camel_to_snake(property).endswith("_for")
                    )
                    or property in multi_ref_properties
                    or not is_exportable(property, property_value_map)
                ):
                    continue
                parse = get_parser_reverse(property, property_value_map)
                for alias in get_aliases(property, property_value_map):
                    values = [
                        relation["key"]
                        for relation in document["relations"]
                        if relation["type"] == property
                    ]
                    if len(values) > 1:
                        multi_ref_properties.append(property)
                        values = [values[0]]
                    if not base_frame_data.get(alias) and not frame_data.get(alias):
                        frame_data.update({alias: parse(values[0]) if values else ""})
            data.append({**base_frame_data, **dict(sorted(frame_data.items()))})

            base_frame_data = {
                **base_frame_data,
                **{
                    key: ""
                    for key in frame_data.keys()
                    if key not in base_frame_data.keys()
                },
            }
            for property in multi_ref_properties:
                frame_data = {}
                parse = get_parser_reverse(property, property_value_map)
                for alias in get_aliases(property, property_value_map):
                    values = [
                        relation["key"]
                        for relation in document["relations"]
                        if relation["type"] == property
                    ]
                    values.pop(0)
                    if not base_frame_data.get(alias) and not frame_data.get(alias):
                        for value in values:
                            frame_data.update({alias: parse(value)})
                            data.append({**base_frame_data, **frame_data})

        return DataFrame(data)

    def _parse_dataframe_to_dams(
        self, columns, row, document_type="", property_value_map={}
    ):
        try:
            if not document_type:
                try:
                    document_type = dict(zip(columns, row))["type"].lower()
                except KeyError:
                    raise BadRequest(
                        f"{get_error_code(ErrorCode.REQUIRED_FIELD_MISSING, get_write())} | prefix: | property:type | type:unknown - Missing required property type | unknown"
                    )
            if not property_value_map:
                if not (property_value_map := get_property_value_map(document_type)):
                    raise BadRequest(
                        f"{get_error_code(ErrorCode.INVALID_FORMAT, get_read())} | prefix: | column_count:{len(columns)} | type:{document_type} - Row parsing failed. Rows in this file may have an inconsistent column count, or the number of columns in this row may exceed the expected count {len(columns)}. Alternatively, the value '{document_type}' for column 'type' may be unknown (this may be detected wrongly due to an inconsistent column count) | {document_type}"
                    )
        except Exception as exception:
            return {"exceptions": [exception]}

        exceptions = []
        document = {
            "identifiers": [],
            "metadata": [],
            "relations": [],
            "schema": {"type": "dams"},
            "type": document_type,
        }
        for column_name, value in zip(columns, row):
            try:
                column_name = column_name.strip()
                if isinstance(value, str):
                    value = value.strip()
                if value:
                    properties = get_properties(column_name, property_value_map)
                else:
                    continue

                for property in properties:
                    if is_readonly(property, property_value_map):
                        continue
                    property_type = (
                        "relations"
                        if camel_to_snake(property).startswith("has_")
                        else "metadata"
                    )
                    parse = get_parser(property, property_value_map, property_type)
                    if property == "_id":
                        document["identifiers"].append(
                            parse(value, columns=columns, row=row, document=document)
                        )
                    elif property:
                        document[property_type].extend(
                            parse(value, columns=columns, row=row, document=document)
                        )
            except HTTPError as exception:
                try:
                    message = exception.response.json()["message"]
                except:
                    message = exception.response.text
                exceptions.append(message)
            except BadRequest as exception:
                exceptions.append(exception.description)
            except Exception as exception:
                exceptions.append(str(exception))

        return document if not exceptions else {**document, "exceptions": exceptions}
