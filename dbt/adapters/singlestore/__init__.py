from dbt.adapters.singlestore.connections import SingleStoreConnectionManager
from dbt.adapters.singlestore.connections import SingleStoreCredentials
from dbt.adapters.singlestore.impl import SingleStoreAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import singlestore


Plugin = AdapterPlugin(
    adapter=SingleStoreAdapter,
    credentials=SingleStoreCredentials,
    include_path=singlestore.PACKAGE_PATH)
