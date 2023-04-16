from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation, Policy


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
    quote_character: str = '`'

    @classmethod
    def get_default_quote_policy(cls) -> Policy:
        return MySQLQuotePolicy()

    @classmethod
    def get_default_include_policy(cls) -> Policy:
        return MySQLIncludePolicy()

    def __post_init__(self):
        if self.database != self.schema and self.database:
            raise Exception(f'Cannot set database {self.database} in mysql!')

    def render(self):
        if self.include_policy.database and self.include_policy.schema:
            raise Exception(
                "Got a mysql relation with schema and database set to "
                "include, but only one can be set"
            )
        return super().render()
