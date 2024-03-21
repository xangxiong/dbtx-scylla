# these are mostly just exports, #noqa them so flake8 will be happy
from dbt.adapters.scylla.connections import CockroachdbConnectionManager  # noqa
from dbt.adapters.scylla.connections import CockroachdbCredentials
from dbt.adapters.scylla.column import CockroachdbColumn  # noqa
from dbt.adapters.scylla.relation import CockroachdbRelation  # noqa: F401
from dbt.adapters.scylla.impl import CockroachdbAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import cockroachdb

Plugin = AdapterPlugin(
    adapter=CockroachdbAdapter, credentials=CockroachdbCredentials, include_path=cockroachdb.PACKAGE_PATH
)
