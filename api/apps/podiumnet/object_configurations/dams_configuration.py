from app_context import g  # pyright: ignore
from apps.podiumnet.resources.base_resource import PodiumnetBaseResource
from apps.podiumnet.serializers.dams_serializer import DamsSerializer
from apps.podiumnet.util import generate_deterministic_uuid
from apps.podiumnet.validation.util import get_schema, get_virtual_properties
from configuration import get_object_configuration_mapper  # pyright: ignore
from copy import deepcopy
from datetime import datetime
from apps.podiumnet.object_configurations.entity_configuration import (
    EntityConfiguration,
)
from elody.util import flatten_dict, send_cloudevent
from os import getenv
from rabbit import get_rabbit  # pyright: ignore
from resources.base_filter_resource import BaseFilterResource  # pyright: ignore
from serialization.case_converter import (  # pyright: ignore
    camel_to_snake,
    snake_to_camel,
)


class DamsConfiguration(ElodyConfiguration):
    SCHEMA_TYPE = "dams"
    SCHEMA_VERSION = 1

    def crud(self):
        crud = {
            "collection": "entities",
            "collection_history": "entities_history",
            "creation_preparer": lambda post_body, **_: self._creation_preparer(
                post_body
            ),
            "document_exception_message_constructor": self._document_exception_message_constructor,
        }
        return {**super().crud(), **crud}

    def document_info(self):
        document_info = {
            "indexes": {
                "entities": [
                    ("id", "_id", True),
                    ("identifiers", "identifiers", True),
                    ("unique_field", "unique_field", True),
                ]
            },
            "tenant_id_resolver": lambda _: "",
        }
        return {**super().document_info(), **document_info}

    def logging(self, flat_document, **kwargs):
        return super().logging(flat_document, **kwargs)

    def migration(self):
        return super().migration()

    def serialization(self, from_format, to_format):
        return getattr(DamsSerializer(), f"from_{from_format}_to_{to_format}")

    def validation(self):  # pyright: ignore
        return "schema", get_schema("elody")

    def _creation_preparer(self, post_body, **_):
        return post_body

    def _document_exception_message_constructor(self, exception, fallback_message):
        return fallback_message

    def _get_audit_override(self, content):
        return {
            key: datetime.fromisoformat(str(value)) if "date_" in key else value
            for key, value in {
                "date_created": content.pop("date_created", ""),
                "created_by": content.pop("created_by", ""),
                "date_updated": content.pop("date_updated", ""),
            }.items()
            if value
        }

    def _post_crud_hook(self, *, crud, document, storage, **kwargs):
        self.__delete_origins(crud, document, storage)
        self.__sync_relations(crud=crud, document=document, **kwargs)
        super()._post_crud_hook(**kwargs)
        self.__add_document_to_history(document)

    def _pre_crud_hook(self, *, crud, document={}, unpatched_document={}, **kwargs):
        if document:
            document = self._sync_identifiers(document)
            document = self.__patch_identifier_from_unique_field(
                crud, document, unpatched_document
            )
            document = self.__order_relations(
                document, crud=crud, unpatched_document=unpatched_document, **kwargs
            )
            document = self.__sync_virtual_relations(
                document, crud=crud, unpatched_document=unpatched_document, **kwargs
            )
        return super()._pre_crud_hook(
            crud=crud,
            document=document,
            unpatched_document=unpatched_document,
            audit_override={
                "date_created": document.get("date_created"),
                "created_by": document.get("created_by"),
                "date_updated": document.get("date_updated"),
                "last_editor": document.get("last_editor"),
            },
            **kwargs,
        )

    def _sync_identifiers(self, document):
        document["identifiers"].extend(
            [
                relation["key"]
                for relation in document["relations"]
                if relation["type"] == "hasOrigin"
            ]
        )
        document["identifiers"] = list(set(document["identifiers"]))
        return document

    def __add_document_to_history(self, document):
        if self._should_create_history_object():
            create = get_object_configuration_mapper().get("history").crud()["creator"]
            send_cloudevent(
                get_rabbit(),
                getenv("MQ_EXCHANGE", "dams"),
                "collection.document.history.create",
                create(deepcopy(document)),
            )

    def __delete_origins(self, crud, document, storage):
        if crud != "delete":
            return

        origins = (
            BaseFilterResource()
            ._execute_advanced_search_with_query_v2(
                [
                    {
                        "type": "text",
                        "key": ["dams:1|identifiers"],
                        "value": document["_id"],
                        "match_exact": True,
                        "operator": "or",
                    },
                    {
                        "type": "text",
                        "key": ["dams:1|elody_id"],
                        "value": document["_id"],
                        "match_exact": True,
                        "operator": "or",
                    },
                ],
                "origins",
                limit=999999,
            )
            .get("results", [])
        )
        for origin in origins:
            storage.delete_item(origin)

    def __order_relations(self, document, crud=crud, unpatched_document={}, **kwargs):
        for type in ["hasAssetPart", "hasMediafile"]:
            order_map = {}
            order_map.update({"undefined": []})
            relations = [
                deepcopy(relation)
                for relation in document["relations"]
                if relation["type"] == type
            ]
            if crud == "update" and any(
                relation.get("sort") is None for relation in relations
            ):
                continue

            for relation in relations:
                if (
                    order := relation.get("sort", {}).get("order", [{}])[0].get("value")
                ) is not None:
                    order_map.update({str(order): relation})
                else:
                    order_map["undefined"].append(relation)
                document["relations"].remove(relation)

            created = []
            order = 1
            order_map = dict(
                sorted(
                    order_map.items(),
                    key=lambda item: (
                        int(item[0]) if item[0].isdigit() else float("inf")
                    ),
                )
            )
            for relations in order_map.values():
                if not isinstance(relations, list):
                    relations = [relations]
                for relation in relations:
                    created.append(
                        {
                            **relation,
                            "metadata": [
                                metadata
                                for metadata in relation.get("metadata", [])
                                if metadata["key"] != "order"
                            ]
                            + [{"key": "order", "value": order}],
                            "sort": {"order": [{"value": order}]},
                        }
                    )
                    order += 1

            if created:
                if type not in get_virtual_properties(document["type"]):
                    document["relations"].extend(created)
                self.__sync_relations(
                    crud=crud,
                    document=document,
                    unpatched_document=unpatched_document,
                    created=created,
                    deleted=[],
                    **kwargs,
                )

        return document

    def __patch_identifier_from_unique_field(self, crud, document, unpatched_document):
        if crud not in ["create", "update"] or not self.document_info().get(
            "unique_field"
        ):
            return document

        unique_field = flatten_dict(self.document_info()["object_lists"], document)[
            self.document_info()["unique_field"]
        ]
        old_unique_field = flatten_dict(
            self.document_info()["object_lists"], unpatched_document
        ).get(self.document_info()["unique_field"])
        if unique_field != old_unique_field:
            identifier = generate_deterministic_uuid(unique_field)
            if old_unique_field is not None:
                old_identifier = generate_deterministic_uuid(old_unique_field)
                document["identifiers"].remove(old_identifier)
            document["identifiers"].append(identifier)

        return document

    def __sync_relations(self, *, crud, document, created=None, deleted=None, **kwargs):
        unpatched_document = kwargs.get("unpatched_document", {})
        document = deepcopy(document)
        if document["type"] in ["download", "set"]:
            return

        created = (
            created
            if created is not None
            else [
                new_relation
                for new_relation in document["relations"]
                if not any(
                    [
                        old_relation
                        for old_relation in unpatched_document.get("relations", [])
                        if new_relation["key"] == old_relation["key"]
                        and new_relation["type"] == old_relation["type"]
                    ]
                )
            ]
        )
        deleted = (
            deleted
            if deleted is not None
            else [
                old_relation
                for old_relation in unpatched_document.get("relations", [])
                if not any(
                    [
                        new_relation
                        for new_relation in document["relations"]
                        if old_relation["key"] == new_relation["key"]
                        and old_relation["type"] == new_relation["type"]
                    ]
                )
            ]
        )
        if crud == "delete":
            deleted = deepcopy(created)
            created = []

        collection = ""
        base_resource = PodiumnetBaseResource()
        for relation in [*created, *deleted]:
            for collection in base_resource._resolve_collections(id=relation["key"]):
                if related_document := base_resource.storage.get_item_from_collection_by_id(
                    collection, relation["key"]
                ):
                    break
            else:
                related_document = {}

            if relation["type"] in [
                "hasAsset",
                "hasOcr",
                "hasOrigin",
                "isAssetPartFor",
                "isTranscodeFor",
            ]:
                continue
            elif relation["type"].startswith("has"):
                related_type = relation["type"].removeprefix("has")
            else:
                related_type = related_document.get("type", relation["type"])
            reverse_relation_type = (
                snake_to_camel(
                    camel_to_snake(
                        f"is{related_type[0].upper() + related_type[1:]}For"
                        if relation["type"].startswith("has")
                        else f"has{document['type'][0].upper() + document['type'][1:]}"
                    )
                )
                if relation["type"] != "isOcrFor"
                else "hasOcr"
            )
            if not related_document or related_type.lower() in [
                "institution",
                "license",
                "photographer",
                "tag",
            ]:
                continue

            if relation in created:
                base_resource.storage.patch_item_from_collection_v2(
                    collection,
                    related_document,
                    {
                        "relations": [
                            {
                                **relation,
                                "key": document["_id"],
                                "type": reverse_relation_type,
                            }
                        ],
                        "schema": related_document["schema"],
                        "type": related_document["type"],
                    },
                    "dams",
                    run_post_crud_hook=not g.get("dry_run"),
                )
            elif relation in deleted and related_document.get("relations"):
                new_related_document = deepcopy(related_document)
                relation["key"] = document["_id"]
                relation["type"] = reverse_relation_type
                new_related_document["relations"] = [
                    related_relation
                    for related_relation in related_document["relations"]
                    if related_relation["key"] != relation["key"]
                    or related_relation["type"] != relation["type"]
                ]
                base_resource.storage.put_item_from_collection(
                    collection,
                    related_document,
                    new_related_document,
                    "dams",
                    run_post_crud_hook=not g.get("dry_run"),
                )

    def __sync_virtual_relations(
        self, document, crud=crud, unpatched_document={}, **kwargs
    ):
        for type in get_virtual_properties(document["type"]):
            created = [
                deepcopy(relation)
                for relation in document["relations"]
                if relation["type"] == type
            ]
            for relation in created:
                document["relations"].remove(relation)
            if created:
                self.__sync_relations(
                    crud=crud,
                    document=document,
                    unpatched_document=unpatched_document,
                    created=created,
                    deleted=[],
                    **kwargs,
                )
        return document
