import re as regex

from elody.policies.helpers import get_flat_item_and_object_lists, get_item
from elody.policies.permission_handler import (
    get_permissions,
    handle_single_item_request,
)
from flask import Request
from inuits_policy_based_auth import BaseAuthorizationPolicy
from inuits_policy_based_auth.contexts.policy_context import (
    PolicyContext,
)
from inuits_policy_based_auth.contexts.user_context import (
    UserContext,
)
from storage.storagemanager import StorageManager


class MediafileCopyrightAndDownloadUrlsPolicy(BaseAuthorizationPolicy):
    def authorize(
        self, policy_context: PolicyContext, user_context: UserContext, request_context
    ):
        request: Request = request_context.http_request
        if not regex.match(
            "^(/[^/]+/v[0-9]+)?/mediafiles/[^/]+/(?:copyright|download-urls)$",
            request.path,
        ):
            return policy_context

        user_context.bag["copyright_access"] = True

        return policy_context
