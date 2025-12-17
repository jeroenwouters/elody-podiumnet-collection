from app_context import g, request  # pyright: ignore
from apps.podiumnet.object_configurations.entity_configuration import (
    EntityConfiguration,
)
from apps.podiumnet.serializers.mediafile_serializer import MediafileSerializer
from configuration import get_object_configuration_mapper  # pyright: ignore
from copy import deepcopy
from elody.util import flatten_dict, send_cloudevent
from rabbit import get_rabbit  # pyright: ignore
from resources.base_filter_resource import BaseFilterResource  # pyright: ignore
from storage.storagemanager import StorageManager
from uuid import uuid4


class MediafileConfiguration(EntityConfiguration):
    SCHEMA_TYPE = "dams"
    SCHEMA_VERSION = 1

    def crud(self):
        crud = {"collection": "mediafiles", "collection_history": "mediafiles_history"}
        return {**super().crud(), **crud}

    def document_info(self):
        document_info = {
            "indexes": {
                "mediafiles": [
                    ("id", "_id", True),
                    ("identifiers", "identifiers", True),
                    ("original_filename", "original_filename", True),
                    (
                        "type-technical_origin",
                        [("type", 1), ("technical_origin", 1)],
                        False,
                    ),
                ]
            },
            "tenant_id_resolver": lambda document: self.__tenant_id_resolver(document),
        }
        return {**super().document_info(), **document_info}

    def logging(self, flat_document, **kwargs):
        return super().logging(flat_document, **kwargs)

    def migration(self):
        return super().migration()

    def serialization(self, from_format, to_format):
        return getattr(MediafileSerializer(), f"from_{from_format}_to_{to_format}")

    def validation(self):
        return super().validation()

    def _creation_preparer(self, post_body, **_):
        if post_body.get("technical_origin", "original") == "original" and (
            id := (request.view_args or {}).get("id")
        ):
            for relation in post_body.get("relations", []):
                if relation["key"] == id and relation["type"] == "isMediafileFor":
                    break
            else:
                post_body["relations"] = post_body.get("relations", []) + [
                    {"key": id, "type": "isMediafileFor"}
                ]
        try:
            return self._creator(deepcopy(post_body))[-1]
        except Exception as exception:
            if request.args.get("soft"):
                return post_body
            raise exception

    def _creator(self, post_body, **_):
        post_body = self.__mutate_post_body(post_body)
        mediafile = super()._creator(
            post_body,
            document_defaults={
                "_id": post_body["_id"],
                "identifiers": [
                    post_body["_id"],
                    post_body["filename"],
                    *post_body.pop("identifiers", []),
                ],
                "metadata": (
                    [
                        {"key": "access", "value": "closed"},
                        {"key": "quality_access", "value": "low"},
                    ]
                    if post_body.get("technical_origin", "original") == "original"
                    else []
                ),
                # **self._get_audit_override(post_body),
            },
        )

        return [mediafile]

    def _post_crud_hook(self, *, crud, document, storage, **kwargs):
        self.__generate_upload_link(crud, document)
        self.__delete_derivatives(crud, document, storage)
        super()._post_crud_hook(crud=crud, document=document, storage=storage, **kwargs)
        self.__delete_file_from_storage(crud, document)

    def _pre_crud_hook(self, *, crud, document={}, **kwargs):
        document = self.__correct_document_metadata(crud, document)
        document = self.__duplicate_is_mediafile_for_relations_to_ref_assets(document)
        return super()._pre_crud_hook(crud=crud, document=document, **kwargs)

    def __correct_document_metadata(self, crud, document):
        if document:
            if crud == "create":
                document.update({"original_filename": document["filename"]})
            if md5sum := document.get("md5sum"):
                document["identifiers"].append(md5sum)
        return document

    def __delete_derivatives(self, crud, document, storage):
        if crud == "delete":
            derivatives = (
                BaseFilterResource()
                ._execute_advanced_search_with_query_v2(
                    self.get_derivatives_query(document["_id"]),
                    self.crud()["collection"],
                    limit=999999,
                )
                .get("results", [])
            )
            for derivative in derivatives:
                derivative = storage.get_item_from_collection_by_id(
                    self.crud()["collection"], derivative["_id"]
                )
                storage.delete_item(derivative)

    def __delete_file_from_storage(self, crud, document):
        if crud == "delete":
            serialize = self.serialization(self.SCHEMA_TYPE, "elody")
            send_cloudevent(
                get_rabbit(),
                "dams",
                "dams.mediafile_deleted",
                {"mediafile": serialize(document), "linked_entities": []},
            )

    def __duplicate_is_mediafile_for_relations_to_ref_assets(self, document):
        if document.get("technical_origin") != "original":
            return document

        document["ref_assets"] = [
            relation["key"]
            for relation in document.get("relations", [])
            if relation["type"] == "isMediafileFor"
        ]
        return document

    def __generate_upload_link(self, crud, document):
        if crud == "create":
            text_uri_list = g.get("text_uri_list", "")
            uri = self.serialization(self.SCHEMA_TYPE, "texturilist")(document)
            g.text_uri_list = text_uri_list + "\n" + uri if text_uri_list else uri

    def __mutate_post_body(self, post_body):
        flat_post_body = flatten_dict(self.document_info()["object_lists"], post_body)
        post_body["_id"] = post_body.get("_id", str(uuid4()))

        relations = []
        for relation in post_body["relations"]:
            if relation["type"] != "hasOrigin":
                relations.append(relation)
            elif relation["key"].lower().strip() != "file":
                relations.append(
                    {
                        "key": flat_post_body.get("metadata.file_identifier.value"),
                        "type": relation["type"],
                        "label": relation["key"],
                    }
                )
        post_body["relations"] = relations

        return post_body

    def __tenant_id_resolver(self, document):
        flat_document = flatten_dict(self.document_info()["object_lists"], document)
        asset_ids = flat_document.get("relations.isMediafileFor.key")
        if not isinstance(asset_ids, list):
            asset_ids = [asset_ids]

        institution_ids = []
        storage_manager = StorageManager()  # pyright: ignore
        config = get_object_configuration_mapper().get("asset")
        for asset_id in asset_ids:
            asset = (
                storage_manager.get_db_engine().get_item_from_collection_by_id(
                    config.crud()["collection"], asset_id
                )
                or {}
            )
            resolve_tenant_id = config.document_info()["tenant_id_resolver"]
            institution_ids.extend(resolve_tenant_id(asset).split(","))

        return ",".join(institution_ids)

    @classmethod
    def get_derivatives_query(cls, id):
        return [
            {"type": "type", "value": "mediafile"},
            {
                "type": "text",
                "key": ["dams:1|relations.isMediafileFor.key"],
                "value": id,
                "match_exact": True,
                "operator": "or",
            },
            {
                "type": "text",
                "key": ["dams:1|relations.isTranscodeFor.key"],
                "value": id,
                "match_exact": True,
                "operator": "or",
            },
            {
                "type": "text",
                "key": ["dams:1|relations.isOcrFor.key"],
                "value": id,
                "match_exact": True,
                "operator": "or",
            },
        ]
