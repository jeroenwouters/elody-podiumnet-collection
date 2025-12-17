from apps.podiumnet.resources.base_resource import PodiumnetBaseResource
from flask import Blueprint, request
from flask_restful import Api
from inuits_policy_based_auth import RequestContext
from policy_factory import apply_policies
from resources.entity import Entity, EntityMediafiles, EntityDetail

api_bp = Blueprint("entity", __name__)
api = Api(api_bp)


class PodiumnetEntity(PodiumnetBaseResource, Entity):
    @apply_policies(RequestContext(request))
    def get(self, filters=None):
        return super().get(filters=filters)

    @apply_policies(RequestContext(request))
    def post(self):
        return super().post()


class PodiumnetEntityDetail(PodiumnetBaseResource, EntityDetail):
    @apply_policies(RequestContext(request))
    def get(self, id):
        return super().get(id)

    @apply_policies(RequestContext(request))
    def put(self, id):
        return super().put(id)

    @apply_policies(RequestContext(request))
    def patch(self, id):
        return super().patch(id)

    @apply_policies(RequestContext(request))
    def delete(self, id):
        return super().delete(id)

class PodiumnetMediafileCopyright(PodiumnetBaseResource):
    @apply_policies(RequestContext(request))
    def get(self, id):
        if request.args.get("soft", 0, int):
            return "good", 200
        return True, 200

api.add_resource(PodiumnetEntity, "/entities")
api.add_resource(PodiumnetEntityDetail, "/entities/<string:id>")
api.add_resource(PodiumnetMediafileCopyright, "/mediafiles/<string:id>/copyright")
