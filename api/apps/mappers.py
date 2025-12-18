from apps.podiumnet.object_configurations.entity_configuration import (
    EntityConfiguration,
)
from apps.podiumnet.object_configurations.user_configuration import UserConfiguration
from apps.podiumnet.object_configurations.tenant_configuration import TenantConfiguration
from apps.podiumnet.object_configurations.mediafile_configuration import MediafileConfiguration
from apps.podiumnet.object_configurations.asset_configuration import AssetConfiguration
from apps.podiumnet.object_configurations.production_configuration import ProductionConfiguration
from apps.podiumnet.object_configurations.notification_configuration import NotificationConfiguration
from apps.podiumnet.object_configurations.podiumhuis_configuration import PodiumhuisConfiguration
from storage.arangostore import ArangoStorageManager
from storage.memorystore import MemoryStorageManager
from storage.mongostore import MongoStorageManager
from storage.httpstore import HttpStorageManager
from elody.object_configurations.saved_search_configuration import (
    SavedSearchConfiguration,
)
from elody.object_configurations.job_configuration import JobConfiguration

OBJECT_CONFIGURATION_MAPPER = {
    "entities": EntityConfiguration,
    "entity": EntityConfiguration,
    "mediafile": MediafileConfiguration,
    "tenant": TenantConfiguration,
    "user": UserConfiguration,
    "asset": AssetConfiguration,
    "saved_search": SavedSearchConfiguration,
    "production": ProductionConfiguration,
    "notification": NotificationConfiguration,
    "podiumhuis": PodiumhuisConfiguration,
    "job": JobConfiguration,
}

ROUTE_MAPPER = {
    "FilterEntities": "/entities/filter_deprecated",
    "FilterEntitiesV2": "/entities/filter",
    "FilterGenericObjectsV2": "/<string:collection>/filter",
    "FilterGenericObjects": "/<string:collection>/filter_deprecated",
    "FilterMediafiles": "/mediafiles/filter_deprecated",
    "FilterMediafilesV2": "/mediafiles/filter",
}

STORAGE_MAPPER = {
    "arango": ArangoStorageManager,
    "memory": MemoryStorageManager,
    "mongo": MongoStorageManager,
    "http": HttpStorageManager,
}

COLLECTION_MAPPER = {"tickets": "abstracts"}

FEATURES = {
    "bulk_operations": {"import": {}, "edit": {}, "export": {}},
    "specs": {"elody": {"mediafiles": {}}},
}
