from app_context import request  # pyright: ignore
from apps.podiumnet.resources.base_resource import PodiumnetBaseResource
from apps.podiumnet.serializers.dams_serializer import DamsSerializer
from configuration import get_object_configuration_mapper  # pyright: ignore
from elody.util import flatten_dict
from storage.storagemanager import StorageManager
from urllib.parse import quote
from uuid import uuid4


class MediafileSerializer(DamsSerializer):

    def from_dams_original_to_dams_ocr(self, document, *, operation, **_):
        document["_id"] = str(uuid4())


    def from_dams_to_elody(self, document, **kwargs):
        serialized_document = super().from_dams_to_elody(document, **kwargs)
        return self.__compute_values(document, serialized_document)

    def from_dams_to_texturilist(
        self, document, *, filename_key="filename", copyright_access=True, **_
    ):
        base_resource = PodiumnetBaseResource()
        if request.method == "GET":
            if document.get("md5sum") and (filename := document.get(filename_key)):
                if copyright_access is True:
                    ticket_id = base_resource._create_ticket(filename)
                    storage_api_url_ext = (
                        base_resource.storage_api_url_ext.removesuffix("/")
                    )
                    return f"{storage_api_url_ext}/download-with-ticket/{quote(filename)}?ticket_id={ticket_id}"
                elif isinstance(copyright_access, int):
                    image_api_url_ext = base_resource.image_api_url_ext.removesuffix(
                        "/"
                    )
                    return f"{image_api_url_ext}/iiif/3/{quote(filename)}/full/{copyright_access},/0/default.jpg"
            return ""
        else:
            filename = document["original_filename"]
            ticket_id = base_resource._create_ticket(filename)
            storage_api_url = base_resource.storage_api_url.removesuffix("/")
            return f"{storage_api_url}/upload-with-ticket/{quote(filename)}?id={document['_id']}&ticket_id={ticket_id}"

    def from_elody_to_dams(self, document, **kwargs):
        serialized_document = {**document, "metadata": []}
        for metadata in document.get("metadata", []):
            if metadata["key"] not in [
                "attribution",
                "file_identifier",
                "minimal_attribution",
            ]:
                serialized_document["metadata"].append(metadata)
        return super().from_elody_to_dams(serialized_document, **kwargs)

    def _parse_dams_to_dataframe(self, documents, document_type):
        for document in documents:
            document["metadata"].append(
                {"key": "file_identifier", "value": document["original_filename"]}
            )
        documents = [
            document
            for document in documents
            if document["technical_origin"] == "original"
        ]
        return super()._parse_dams_to_dataframe(documents, document_type)

    def __compute_values(self, document, serialized_document):
        if not request.method:
            return serialized_document

        if request.method == "GET":
            base_resource = PodiumnetBaseResource()
            serialized_document = base_resource._inject_api_urls_into_mediafiles(
                [serialized_document]
            )[0]
        elif serialized_document.get("technical_metadata"):
            del serialized_document["technical_metadata"]
        if request.endpoint == "elody.elodymediafilefilter":
            if not any(
                filter
                for filter in (request.json or [])
                if "relations.isMediafileFor.key" in str(filter.get("key", ""))
            ):
                return serialized_document
        elif (
            request.method == "PATCH"
            or request.endpoint not in ["elody.elodydocument", "elody.elodymediafile"]
            or not serialized_document.get("metadata")
        ):
            return serialized_document

        self.storage = StorageManager().get_db_engine()  # pyright: ignore
        object_lists = (
            get_object_configuration_mapper()
            .get(document["type"])
            .document_info()["object_lists"]
        )
        flat_document = flatten_dict(object_lists, document)
        flat_asset_document = self.__get_related_flat_document(
            flat_document.get("relations.isMediafileFor.key"), "asset"
        )

        try:
            photographers_metadata_value = self.__get_property_from_related_document(
                ["metadata.name.value"],
                flat_document.get("relations.hasPhotographer.key"),
                "photographer",
            )
            serialized_document["metadata"].extend(
                [
                    {
                        "key": "minimal_attribution",
                        "value": flat_document.get(
                            "metadata.minimal_attribution_overwrite.value",
                            ", ".join(
                                [
                                    value
                                    for value in [
                                        *self.__get_property_from_related_document(
                                            [
                                                "metadata.source_listing_name.value",
                                                "metadata.name.value",
                                            ],
                                            flat_asset_document.get(
                                                "relations.hasInstitution.key"
                                            ),
                                            "institution",
                                        ),
                                        (
                                            f"foto: {'; '.join(photographers_metadata_value)}"
                                            if photographers_metadata_value
                                            else ""
                                        ),
                                    ]
                                    if value
                                ]
                            ),
                        ),
                    },
                    {
                        "key": "attribution",
                        "value": flat_document.get(
                            "metadata.attribution_overwrite.value",
                            ", ".join(
                                [
                                    value
                                    for value in [
                                        *(
                                            flat_asset_document.get(
                                                "metadata.creator.value", []
                                            )
                                            if isinstance(
                                                flat_asset_document.get(
                                                    "metadata.creator.value"
                                                ),
                                                list,
                                            )
                                            else [
                                                flat_asset_document.get(
                                                    "metadata.creator.value", ""
                                                )
                                            ]
                                        ),
                                        *(
                                            flat_asset_document.get(
                                                "metadata.title.value", []
                                            )
                                            if isinstance(
                                                flat_asset_document.get(
                                                    "metadata.title.value"
                                                ),
                                                list,
                                            )
                                            else [
                                                flat_asset_document.get(
                                                    "metadata.title.value", ""
                                                )
                                            ]
                                        ),
                                        flat_asset_document.get(
                                            "metadata.identifier.value"
                                        ),
                                        *self.__get_property_from_related_document(
                                            [
                                                "metadata.source_listing_name.value",
                                                "metadata.name.value",
                                            ],
                                            flat_asset_document.get(
                                                "relations.hasInstitution.key"
                                            ),
                                            "institution",
                                        ),
                                        (
                                            f"foto: {'; '.join(photographers_metadata_value)}"
                                            if photographers_metadata_value
                                            else ""
                                        ),
                                    ]
                                    if value
                                ]
                            ),
                        ),
                    },
                ]
            )
        except Exception:
            pass
        return serialized_document

    def __get_property_from_related_document(
        self, flat_properties, document_id, document_type
    ):
        if not document_id or not document_type:
            return []
        related_flat_document = self.__get_related_flat_document(
            document_id, document_type
        )
        values = []

        for flat_property in flat_properties:
            values.append(related_flat_document.get(flat_property, ""))
        return values

    def __get_related_flat_document(self, document_id, document_type):
        config = get_object_configuration_mapper().get(document_type)
        document = (
            self.storage.get_item_from_collection_by_id(
                config.crud()["collection"], document_id
            )
            or {}
        )
        return flatten_dict(config.document_info()["object_lists"], document)
