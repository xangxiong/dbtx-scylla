# these are mostly just exports, #noqa them so flake8 will be happy
from dbt.adapters.scylla.connections import ScyllaConnectionManager  # noqa
from dbt.adapters.scylla.connections import ScyllaCredentials
from dbt.adapters.scylla.column import ScyllaColumn  # noqa
from dbt.adapters.scylla.relation import ScyllaRelation  # noqa: F401
from dbt.adapters.scylla.impl import ScyllaAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import scylla

Plugin = AdapterPlugin(
    adapter=ScyllaAdapter, credentials=ScyllaCredentials, include_path=scylla.PACKAGE_PATH
)
