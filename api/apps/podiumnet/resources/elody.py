from apps.podiumnet.resources.base_resource import PodiumnetBaseResource
from elody.policies.helpers import get_item
from flask import g, redirect, request, url_for
from flask_restful import Headers
from inuits_policy_based_auth import RequestContext
from policy_factory import authenticate, get_user_context  # pyright: ignore
from resources.elody._blueprint import api  # pyright: ignore
from resources.elody.batch import ElodyBatch  # pyright: ignore
from resources.elody.document import ElodyDocument  # pyright: ignore
from resources.elody.document_relations import ElodyDocumentRelations  # pyright: ignore
from resources.elody.filter import ElodyFilter  # pyright: ignore
from resources.elody.mediafiles.document_mediafiles import (  # pyright: ignore
    ElodyDocumentMediafiles,
)
from resources.elody.mediafiles.mediafile_derivatives import (  # pyright: ignore
    ElodyMediafileDerivatives,
)
from resources.generic_object import GenericObjectDetailV2  # pyright: ignore
from serialization.case_converter import snake_to_camel  # pyright: ignore
from storage.storagemanager import StorageManager
from werkzeug.exceptions import NotFound


class ClientDocument(ElodyDocument):
    @authenticate(RequestContext(request))
    def get(self, *, id, **kwargs):
        if request.args.get("soft", 0, int) and not request.args.get("key_to_check"):
            return "good", 200

        try:
            get_item(
                StorageManager(),  # pyright: ignore
                get_user_context().bag,
                request.view_args,
            )
        except NotFound:
            if document := self.storage.db["origins"].find_one({"_id": id}):
                return redirect(
                    url_for("elody.clientdocument", id=document["elody_id"]), code=301
                )

        return super().get(id=id, **kwargs)


class ClientDocumentMediafiles(ElodyDocumentMediafiles):
    def get(self, **kwargs):
        return super().get(
            relations_stored_on_mediafile=True,
            flat_ref_mediafiles_key="relations.isMediafileFor.key",
            **kwargs,
        )

    def post(self, **kwargs):
        return super().post(
            store_relations_on_mediafile=True,
            mediafile_relation_type="isMediafileFor",
            technical_origin_relation_type_template="isTECHNICAL_ORIGINFor",
            **kwargs,
        )


class ClientDocumentRelationsOrder(PodiumnetBaseResource, GenericObjectDetailV2):
    def get(self, id, **kwargs):
        document_type = request.args.get("return_type")
        request.method = "POST"
        request.path = "/entities/filter"
        request.headers = Headers({**request.headers, "Accept": "text/csv"})
        g.content = [
            {"type": "type", "value": document_type},
            {
                "type": "text",
                "key": f"relations.{snake_to_camel(f'is_{document_type}_for')}.key",
                "value": id,
                "match_exact": True,
            },
        ]
        g.enable_parsers = True
        return ElodyFilter().post(**kwargs)

    def post(self, **kwargs):
        response, status_code = ElodyBatch().post(force_patch_only=True, **kwargs)
        if status_code != 200:
            return response, status_code

        g.content = [
            {
                "key": document["_id"],
                "sort": {},
                "type": snake_to_camel(f"has_{document['type']}"),
            }
            for document in response["entities"]
            if document.get("_id")
        ]
        return ElodyDocumentRelations().post(**kwargs)


class ClientMediafileDerivatives(ElodyMediafileDerivatives):
    def post(self, **kwargs):
        return super().post(
            store_relations_on_mediafile=True,
            mediafile_relation_type="isMediafileFor",
            technical_origin_relation_type_template="isTECHNICAL_ORIGINFor",
            **kwargs,
        )


def resource_rules():
    return [
        {"route": "/entities/<string:id>", "resource": ClientDocument, "api": api},
        {
            "route": "/entities/<string:id>/mediafiles",
            "resource": ClientDocumentMediafiles,
            "api": api,
        },
        {
            "route": "/entities/<string:id>/order",
            "resource": ClientDocumentRelationsOrder,
            "api": api,
        },
        {
            "route": "/mediafiles/<string:id>/derivatives",
            "resource": ClientMediafileDerivatives,
            "api": api,
        },
    ]
