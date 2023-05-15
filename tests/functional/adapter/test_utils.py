import pytest
from dbt.tests.adapter.utils.base_utils import BaseUtils
from dbt.tests.adapter.utils.test_any_value import BaseAnyValue
from dbt.tests.adapter.utils.test_bool_or import BaseBoolOr, models__test_bool_or_yml
from dbt.tests.adapter.utils.test_cast_bool_to_text import BaseCastBoolToText
from dbt.tests.adapter.utils.test_concat import BaseConcat
from dbt.tests.adapter.utils.test_dateadd import BaseDateAdd, models__test_dateadd_yml
from dbt.tests.adapter.utils.test_datediff import BaseDateDiff, models__test_datediff_yml
from dbt.tests.adapter.utils.test_date_trunc import BaseDateTrunc, models__test_date_trunc_yml
from dbt.tests.adapter.utils.test_escape_single_quotes import BaseEscapeSingleQuotesQuote
from dbt.tests.adapter.utils.test_escape_single_quotes import BaseEscapeSingleQuotesBackslash
from dbt.tests.adapter.utils.test_except import BaseExcept
from dbt.tests.adapter.utils.test_hash import BaseHash
from dbt.tests.adapter.utils.test_intersect import BaseIntersect
from dbt.tests.adapter.utils.test_last_day import BaseLastDay
from dbt.tests.adapter.utils.test_length import BaseLength
from dbt.tests.adapter.utils.test_listagg import BaseListagg
from dbt.tests.adapter.utils.test_position import BasePosition
from dbt.tests.adapter.utils.test_replace import BaseReplace
from dbt.tests.adapter.utils.test_right import BaseRight
from dbt.tests.adapter.utils.test_safe_cast import BaseSafeCast
from dbt.tests.adapter.utils.test_split_part import BaseSplitPart, models__test_split_part_yml
from dbt.tests.adapter.utils.test_string_literal import BaseStringLiteral


class TestAnyValue(BaseAnyValue):
    pass


models__test_bool_or_sql = """
with data as (
    select * from {{ ref('data_bool_or') }}
),
data_output as (
    select * from {{ ref('data_bool_or_expected') }}
),
calculate as (
    select
        `key`,
        {{ bool_or('val1 = val2') }} as value
    from data
    group by `key`
)
select
    calculate.value as actual,
    data_output.value as expected
from calculate
left join data_output
on calculate.`key` = data_output.`key`
"""


class TestBoolOr(BaseBoolOr):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_bool_or.yml": models__test_bool_or_yml,
            "test_bool_or.sql": self.interpolate_macro_namespace(
                models__test_bool_or_sql, "bool_or"
            ),
        }
    pass


class TestCastBoolToText(BaseCastBoolToText):
    pass


class TestConcat(BaseConcat):
    pass


models__test_dateadd_sql = """
with data as (
    select * from {{ ref('data_dateadd') }}
)
select
    case
        when datepart = 'hour' then ({{ dateadd('hour', 'interval_length', 'from_time') }} :> {{ api.Column.translate_type('timestamp') }})
        when datepart = 'day' then ({{ dateadd('day', 'interval_length', 'from_time') }} :> {{ api.Column.translate_type('timestamp') }})
        when datepart = 'month' then ({{ dateadd('month', 'interval_length', 'from_time') }} :> {{ api.Column.translate_type('timestamp') }})
        when datepart = 'year' then ({{ dateadd('year', 'interval_length', 'from_time') }} :> {{ api.Column.translate_type('timestamp') }})
        else null
    end as actual,
    result as expected
from data
"""


class TestDateAdd(BaseDateAdd):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_dateadd.yml": models__test_dateadd_yml,
            "test_dateadd.sql": self.interpolate_macro_namespace(
                models__test_dateadd_sql, "dateadd"
            ),
        }
    pass


# we drop union with literals due to unsupported syntax
models__test_datediff_sql = """
with data as (
    select * from {{ ref('data_datediff') }}
)
select
    case
        when datepart = 'second' then {{ datediff('first_date', 'second_date', 'second') }}
        when datepart = 'minute' then {{ datediff('first_date', 'second_date', 'minute') }}
        when datepart = 'hour' then {{ datediff('first_date', 'second_date', 'hour') }}
        when datepart = 'day' then {{ datediff('first_date', 'second_date', 'day') }}
        when datepart = 'week' then {{ datediff('first_date', 'second_date', 'week') }}
        when datepart = 'month' then {{ datediff('first_date', 'second_date', 'month') }}
        when datepart = 'year' then {{ datediff('first_date', 'second_date', 'year') }}
        else null
    end as actual,
    result as expected
from data
"""

# in SingleStore, the number of weeks between dates
# is  calculated as number of days divided by 7,
# so the number of weeks between 2019-12-31 00:00:00 and 2019-12-27 00:00:00
# is expected to be 0, not -1 as in the base datediff test class
seeds__data_datediff_csv = """first_date,second_date,datepart,result
2018-01-01 01:00:00,2018-01-02 01:00:00,day,1
2018-01-01 01:00:00,2018-02-01 01:00:00,month,1
2018-01-01 01:00:00,2019-01-01 01:00:00,year,1
2018-01-01 01:00:00,2018-01-01 02:00:00,hour,1
2018-01-01 01:00:00,2018-01-01 02:01:00,minute,61
2018-01-01 01:00:00,2018-01-01 02:00:01,second,3601
2019-12-31 00:00:00,2019-12-27 00:00:00,week,0
2019-12-31 00:00:00,2019-12-30 00:00:00,week,0
2019-12-31 00:00:00,2020-01-02 00:00:00,week,0
2019-12-31 00:00:00,2020-01-07 02:00:00,week,1
,2018-01-01 02:00:00,hour,
2018-01-01 02:00:00,,hour,
"""


# datediff has slightly different behavior in SingleStore
class TestDateDiff(BaseDateDiff):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"data_datediff.csv": seeds__data_datediff_csv}

    @pytest.fixture(scope="class")
    def models(self):
         return {
            "test_datediff.yml": models__test_datediff_yml,
            "test_datediff.sql": self.interpolate_macro_namespace(
                models__test_datediff_sql, "datediff"
            ),
        }
    pass


models__test_date_trunc_sql = """
select
    ({{date_trunc('day', 'updated_at') }} :> date) as actual,
    day as expected
from {{ ref('data_date_trunc') }}

union all

select
    ({{ date_trunc('month', 'updated_at') }} :> date) as actual,
    month as expected
from {{ ref('data_date_trunc') }}
"""


class TestDateTrunc(BaseDateTrunc):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_date_trunc.yml": models__test_date_trunc_yml,
            "test_date_trunc.sql": self.interpolate_macro_namespace(
                models__test_date_trunc_sql, "date_trunc"
            ),
        }
    pass


class TestEscapeSingleQuotesBackslash(BaseEscapeSingleQuotesBackslash):
    pass


class TestEscapeSingleQuotesQuotes(BaseEscapeSingleQuotesQuote):
    pass


class TestExcept(BaseExcept):
    pass


class TestHash(BaseHash):
    pass


class TestIntersect(BaseIntersect):
    pass


class TestLastDay(BaseLastDay):
    pass


class TestLength(BaseLength):
    pass


class TestListagg(BaseListagg):
    pass


class TestPosition(BasePosition):
    pass


class TestReplace(BaseReplace):
    pass


class TestRight(BaseRight):
    pass


class TestSafeCast(BaseSafeCast):
    pass


models__test_split_part_sql = """
select
    {{ split_part('parts', 'split_on', 1) }} as actual,
    result_1 as expected
from {{ ref('data_split_part') }}

union all

select
    {{ split_part('parts', 'split_on', 2) }} as actual,
    result_2 as expected
from {{ ref('data_split_part') }}

union all

select
    {{ split_part('parts', 'split_on', 3) }} as actual,
    result_3 as expected
from {{ ref('data_split_part') }}
"""


class TestSplitPart(BaseSplitPart):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "test_split_part.yml": models__test_split_part_yml,
            "test_split_part.sql": self.interpolate_macro_namespace(
                models__test_split_part_sql, "split_part"
            ),
        }
    pass


class TestStringLiteral(BaseStringLiteral):
    pass
