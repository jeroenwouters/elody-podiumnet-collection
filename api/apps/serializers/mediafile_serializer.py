from copy import deepcopy
from apps.vliz.resources.base_resource import VlizBaseResource
from apps.vliz.serializers.vliz_serializer import VlizSerializer
from urllib.parse import quote
from flask import request


DOCUMENT_METADATA_KEYS = {
    "filename",
    "md5sum",
    "mimetype",
    "original_file_location",
    "original_filename",
    "technical_metadata",
    "technical_origin",
    "thumbnail_file_location",
    "img_width",
    "img_height",
}


class MediafileSerializer(VlizSerializer):
    def __init__(self):
        self.base_resource = VlizBaseResource()

    def from_vliz_to_elody(self, document: dict, **_):
        if document.get("schema", {}).get("type") == "elody":
            return document
        serialized_document = super().from_vliz_to_elody(document)

        for key, value in document.get("metadata", {}).items():
            serialized_document.update({key: value})

        if serialized_document.get(
            "technical_origin", None
        ) == "transcode" and not serialized_document.get("transcode_file_location"):
            serialized_document["transcode_file_location"] = (
                serialized_document.get("original_file_location")
                or f"/download/{serialized_document.get('filename')}"
            )
        if serialized_document.get(
            "technical_origin", None
        ) == "original" and not serialized_document.get("display_filename"):
            serialized_document["display_filename"] = serialized_document.get(
                "transcode_filename", serialized_document.get("filename")
            )

        return self.base_resource._inject_api_urls_into_mediafiles(
            [serialized_document]
        )[0]

    def from_vliz_to_texturilist(self, document, filename_key="filename", **_):
        # storage_api_url = self.base_resource.storage_api_url.removesuffix("/")
        # original_filename = document.get("metadata", {}).get("original_filename")
        # ticket_id = self.base_resource._create_ticket(original_filename)
        # return f"{storage_api_url}/upload-with-ticket/{quote(original_filename)}?id={document['id']}&ticket_id={ticket_id}"  # noqa

        if request.method == "GET":
            if document.get("md5sum") and (
                filename := document.get(filename_key)
                # filename := document.get("metadata", {}).get(filename_key)
            ):
                ticket_id = self.base_resource._create_ticket(filename)
                storage_api_url_ext = (
                    self.base_resource.storage_api_url_ext.removesuffix("/")
                )
                return f"{storage_api_url_ext}/download-with-ticket/{quote(filename)}?ticket_id={ticket_id}"  # noqa
            return ""
        else:
            filename = document.get("metadata", {}).get(
                "original_filename", document.get(filename_key)
            )
            ticket_id = self.base_resource._create_ticket(filename)
            storage_api_url = self.base_resource.storage_api_url.removesuffix("/")
            return f"{storage_api_url}/upload-with-ticket/{quote(filename)}?id={document['_id']}&ticket_id={ticket_id}"  # noqa

    def from_elody_to_texturilist(self, document, **_):
        storage_api_url = self.base_resource.storage_api_url.removesuffix("/")
        original_filename = document.get("original_filename")
        ticket_id = self.base_resource._create_ticket(original_filename)
        return f"{storage_api_url}/upload-with-ticket/{quote(original_filename)}?id={document['id']}&ticket_id={ticket_id}"  # noqa

    def from_elody_to_vliz(self, document, **_):
        serialized_document = super().from_elody_to_vliz(document)

        if filename := document.get("filename"):
            serialized_document["properties"].update({"filename": {"value": filename}})

        for key in DOCUMENT_METADATA_KEYS:
            if value := document.get(key):
                if serialized_document.get("metadata"):
                    serialized_document["metadata"].update({key: value})
                else:
                    serialized_document.update({"metadata": {key: value}})
                serialized_document.pop(key)

        return serialized_document

    def _parse_dataframe_to_vliz(
        self, columns, row, document_type="", property_value_map={}
    ):
        document = super()._parse_dataframe_to_vliz(
            columns, row, document_type, property_value_map
        )

        new_document = deepcopy(document)

        for key in DOCUMENT_METADATA_KEYS:
            if value := document.get(key):
                if new_document.get("metadata"):
                    new_document["metadata"].update({key: value})
                    new_document.pop(key)
                else:
                    new_document.update({"metadata": {key: value}})
                    new_document.pop(key)

        return new_document
