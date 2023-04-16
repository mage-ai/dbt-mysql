from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation, Policy
# This class doesn’t exist.
# from dbt.exceptions import RuntimeException


@dataclass
class MySQLQuotePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass
class MySQLIncludePolicy(Policy):
    database: bool = False
    schema: bool = True
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class MySQLRelation(BaseRelation):
    quote_policy: MySQLQuotePolicy = MySQLQuotePolicy()
    include_policy: MySQLIncludePolicy = MySQLIncludePolicy()
    quote_character: str = "`"

    def __post_init__(self):
        if self.database != self.schema and self.database:
            raise Exception(
                f"Cannot set `database` to '{self.database}' in mysql!"
                "You can either unset `database`, or make it match `schema`, "
                f"currently set to '{self.schema}'"
            )

    def render(self):
        if self.include_policy.database and self.include_policy.schema:
            raise Exception(
                "Got a mysql relation with schema and database set to "
                "include, but only one can be set"
            )
        return super().render()
