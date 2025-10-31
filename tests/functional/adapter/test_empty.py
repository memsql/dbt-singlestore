from dbt.tests.adapter.empty.test_empty import BaseTestEmpty, BaseTestEmptyInlineSourceRef


class TestSingleStoreEmpty(BaseTestEmpty):
    pass

#dbt-core generates `(subquery) _dbt_limit_subq_* as alias`, which is invalid in SingleStore
#class TestSingleStoreEmptyInlineSourceRef(BaseTestEmptyInlineSourceRef):
#    pass
