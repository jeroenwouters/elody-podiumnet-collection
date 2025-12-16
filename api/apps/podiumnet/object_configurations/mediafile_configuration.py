from copy import deepcopy
import re
from urllib.parse import unquote
from uuid import uuid4

from apps.podiumnet.object_configurations.entity_configuration import EntityConfiguration
# from apps.podiumnet.serializers.mediafile_serializer import MediafileSerializer
from apps.podiumnet.validation.util import get_schema
from configuration import (  # pyright: ignore[reportMissingImports]
    get_object_configuration_mapper,
)
from elody.util import send_cloudevent
from flask import g
from rabbit import get_rabbit  # pyright: ignore
from storage.storagemanager import StorageManager


class MediafileConfiguration(EntityConfiguration):
    SCHEMA_TYPE = "elody"
    SCHEMA_VERSION = 1

    def crud(self):
        crud = {
            "collection": "mediafiles",
            # TODO: This should probably just be called in the pre_crud_hook of the mediafiles
            "fix_linked_entities": lambda id, **kwargs: self.__fix_related_entities(
                id, **kwargs
            ),
        }
        return {**super().crud(), **crud}

    def document_info(self):
        # TODO: Add indexes

        info = {
            "tenant_id_resolver": lambda document: self.__tenant_id_resolver(document),
        }
        return {**super().document_info(), **info}

    def logging(self, flat_document, **kwargs):
        return super().logging(flat_document=flat_document, **kwargs)

    def migration(self):
        return super().migration()

#     def serialization(self, from_format, to_format):
#         return getattr(MediafileSerializer(), f"from_{from_format}_to_{to_format}")

    def validation(self):
        return "schema", get_schema("mediafile")

    def _pre_crud_hook(self, *, crud, document={}, **kwargs):
        if document:
            if not document.get("type"):
                document["type"] = "mediafile"
            self.__correct_document_metadata(crud, document)
            # FIXME: There is an issue with a patch supplying too much data somewhere
            # which includes urls that then clog up a request somewhere
            # I already spent too long on trying to figure out exactly what causes this
            # It's either something in the storage_api, or happens when a job update is called
            # I assume it's the job update, since that will use the basic routing key which
            # isn't caught by the podiumnet specific queue
            document = self.remove_injected_urls(document)
            return super()._pre_crud_hook(crud=crud, document=document, **kwargs)
        elif property := kwargs.get("property"):
            kwargs.pop("property")
            return super()._pre_crud_hook(crud=crud, property=property, **kwargs)

    def _creator(self, post_body, **_):
        _id: str = post_body.get("_id", "")
        if not _id.startswith("MED"):
            _id = f"MED-{str(uuid4())}"

        return super()._creator(
            post_body,
            document_defaults={
                "_id": _id,
                "id": _id,
                "identifiers": [_id],
            },
        )

    def _post_crud_hook(self, *, crud, document, **kwargs):
        self.__generate_upload_link(crud, document)
        super()._post_crud_hook(document=document, **kwargs)
        self.__delete_file_from_storage(crud, document)

    def __delete_file_from_storage(self, crud, document):
        if crud == "delete":
            serialize = self.serialization("podiumnet", "elody")
            send_cloudevent(
                get_rabbit(),
                "podiumnet",
                "podiumnet.mediafile_deleted",
                {"mediafile": serialize(document), "linked_entities": []},
            )

    def __generate_upload_link(self, crud, document):
        if crud == "create":
            text_uri_list = g.get("text_uri_list", "")
            uri = self.serialization("podiumnet", "texturilist")(document)
            g.text_uri_list = text_uri_list + "\n" + uri if text_uri_list else uri

    def __correct_document_metadata(self, crud, document):
        if document:
            if crud == "create":
                document["metadata"].update(
                    {"original_filename": document["metadata"]["filename"]}
                )
            if document["properties"].get("filename"):
                del document["properties"]["filename"]
            if md5sum := document["metadata"].get("md5sum"):
                document["identifiers"].append(md5sum)

    def __fix_related_entities(self, id, **kwargs):

        storage = StorageManager().get_db_engine()  # pyright: ignore[reportCallIssue]

        links = ["primary_mediafile_id", "primary_thumbnail_id"]
        query = {"$or": [{link: id} for link in links]}

        entities = storage.db[self.crud()["collection"]].find_one(query)
        if entities:
            for entity in entities:
                new_entity = deepcopy(entity)
                for link in links:
                    if entity.get(link) == id:
                        new_entity.pop(link)
                storage.put_item_to_collection_v2("mediafiles", entity, new_entity)

    # FIXME:
    def remove_injected_urls(self, document, **_):
        document_metadata = document.get("metadata")
        if original_file_location := document_metadata.get("original_file_location"):
            document_metadata["original_file_location"] = unquote(
                re.sub(
                    r"\?ticket_id=.*$",
                    "",
                    re.sub(
                        r"^htt.*/download-with-ticket/",
                        "/download/",
                        original_file_location,
                    ),
                )
            )

        if transcode_file_location := document_metadata.get("transcode_file_location"):
            document_metadata["transcode_file_location"] = unquote(
                re.sub(
                    r"\?ticket_id=.*$",
                    "",
                    re.sub(
                        r"^htt.*/download-with-ticket/",
                        "/download/",
                        transcode_file_location,
                    ),
                )
            )

        if root_transcode_file_location := document.get("transcode_file_location"):
            document["transcode_file_location"] = unquote(
                re.sub(
                    r"\?ticket_id=.*$",
                    "",
                    re.sub(
                        r"^htt.*/download-with-ticket/",
                        "/download/",
                        root_transcode_file_location,
                    ),
                )
            )

        if thumbnail_file_location := document_metadata.get("thumbnail_file_location"):

            document_metadata["thumbnail_file_location"] = unquote(
                re.sub(r"^.*/iiif/", "/iiif/", thumbnail_file_location)
            )

        return document

    def _document_content_patcher(
        self, *, document, content, crud, timestamp, overwrite=False, **kwargs
    ):
        document_metadata = None
        for key, value in content.items():
            if isinstance(value, dict) and key == "metadata":
                document[key] = self._document_content_patcher(
                    document=document.get(key, {}),
                    content=value,
                    crud=crud,
                    timestamp=timestamp,
                    **kwargs,
                )
                document_metadata = document.pop("metadata")

        result = super()._document_content_patcher(
            document=document,
            content=content,
            crud=crud,
            timestamp=timestamp,
            overwrite=overwrite,
            kwargs=kwargs,
        )
        # NOTE: Together with the pop("metadata") this is a workaround for an
        # overzealous patcher upstream
        if document_metadata:
            result["metadata"] = document_metadata
        return result

    def _sorting(self, key_order_map, **_):
        addFields, sort = {}, {}
        for key, order in key_order_map.items():
            if key not in ["date_created", "date_updated", "last_editor"]:
                addFields.update({key: f"$metadata.{key}"})
            sort.update({key: order})
        mediafile = []
        if addFields:
            mediafile.append({"$addFields": addFields})

        mediafile.append({"$sort": sort})
        return mediafile

    def __tenant_id_resolver(self, document):
        parent_ids = document.get("proprties", {}).get("belongs_to", {}).get("value")
        if not isinstance(parent_ids, list):
            parent_ids = [parent_ids]

        context_ids = []
        storage_manager = StorageManager()  # pyright: ignore
        config = get_object_configuration_mapper().get("media")
        for media_id in parent_ids:
            media = (
                storage_manager.get_db_engine().get_item_from_collection_by_id(
                    config.crud()["collection"], media_id
                )
                or {}
            )
            resolve_tenant_id = config.document_info()["tenant_id_resolver"]
            context_ids.extend(resolve_tenant_id(media).split(","))

        return ",".join(context_ids)

        # return "podiumnet_context"

    @classmethod
    def get_derivatives_query(cls, id):
        return [
            {"type": "type", "value": "mediafile"},
            {
                "type": "selection",
                "key": ["podiumnet:1|properties.belongs_to_parent.value"],
                "value": id,
                "match_exact": True,
            },
        ]
