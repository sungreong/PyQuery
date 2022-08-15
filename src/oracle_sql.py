from multiprocessing.dummy import Manager
import re


class SQL(object):
    @staticmethod
    def define_table(table_names: list):
        assert type(table_names) == list, "list로만 가능합니다."
        if len(table_names) == 0:
            raise Exception("테이블 이름을 적어주세요")
        else:
            table_names = ", ".join(table_names)
        return f"FROM {table_names}"

    @staticmethod
    def define_select_column(cols: list):
        if len(cols) == 0:
            cols = "*"
        else:
            cols = ", ".join(cols)
        return f"SELECT {cols}"

    @staticmethod
    def define_group(cols: list):
        if len(cols) == 0:
            return ""
        else:
            cols = ", ".join(cols)
            return f"GROUP BY {cols}"

    @staticmethod
    def define_order(order_cols: list, sorting_types: list = ["DESC"]):
        if len(order_cols) == 0:
            return ""

        assert len(order_cols) == len(sorting_types), "ORDER에 맞게 SORTING TYPE 정해야 함"
        return ", ".join([f"{col} {type}" for col, type in zip(order_cols, sorting_types)]).strip()

    @staticmethod
    def make_query(
        select_c,
        from_c,
        order_c="",
        group_c="",
        where_c="",
        having_c="",
        upper=True,
    ):
        if (group_c == "") and (having_c != ""):
            raise Exception("having 구문는 group 구문이 있는 상태에서만 가능합니다.")
        query = f"{select_c} {from_c} {where_c} {group_c} {having_c} {order_c}"
        if upper:
            query = query.upper()
        return re.sub(" +", " ", query).strip()

    @staticmethod
    def define_temp_table(temp_table_name, query):
        return f"WITH ({temp_table_name} AS {query})"

    @staticmethod
    def concat_query(query1, query2):
        q_cat = f"""
        {query1}
        {query2}"""
        return q_cat

    @staticmethod
    def finish_query(query):
        return f"{query};"


from abc import *


class ClauseManagement(object):
    @staticmethod
    def add_condition(query1, query2, method):
        assert method in ["AND", "OR"], "AND or OR 만 가능합니다."
        return f"{query1} {method} {query2}"

    @classmethod
    def finish_where_query(cls, query):
        return f"WHERE {query}"

    @classmethod
    def finish_having_query(cls, query):
        return f"HAVING {query}"

    @classmethod
    def finish_query(cls, query, method="where"):
        if method == "where":
            return cls.finish_where_query(query)
        elif method == "having":
            return cls.finish_having_query(query)


class ConditionQuery(object):

    _sum = lambda x: f"SUM({x})"
    _avg = lambda x: f"AVG({x})"
    _max = lambda x: f"MAX({x})"
    _min = lambda x: f"MIN({x})"
    _count = lambda x: f"COUNT({x})"
    _std = lambda x: f"STDDEV({x})"
    _var = lambda x: f"VARIANCE({x})"
    _none = lambda x: f"{x}"
    _agg_f = dict(
        sum=_sum,
        avg=_avg,
        max=_max,
        count=_count,
        std=_std,
        var=_var,
        none=_none,
    )
    agg_list = list(_agg_f.keys())

    def __init__(self, col, table_name=None):
        self._col = col
        if table_name is None:
            pass
        else:
            self._col = f"{table_name}.{self._col}"

    def q_between(self, low, high, reverse=False, agg_f="none"):
        assert agg_f in self.agg_list
        col = self._agg_f[agg_f](self._col)
        between_query = f"{col} BETWEEN {low} AND {high}"

        if reverse:
            return "NOT {between_query} "

        else:
            return between_query

    def q_greather(self, low, equal=False, agg_f="none"):
        assert agg_f in self.agg_list
        col = self._agg_f[agg_f](self._col)
        if equal:
            return f"{col} >= {low}"
        else:
            return f"{col} > {low}"

    def q_lower(self, high, equal=False, agg_f="none"):
        assert agg_f in self.agg_list
        col = self._agg_f[agg_f](self._col)
        if equal:
            return f"{col} < {high}"
        else:
            return f"{col} <= {high}"

    def q_equal(self, key, reverse=False, agg_f="none"):
        assert agg_f in self.agg_list
        col = self._agg_f[agg_f](self._col)
        if reverse:
            return f"{col} != {key}"
        else:
            return f"{col} = {key}"

    def q_is_null(self, reverse=False):
        if reverse:
            return f"{self._col} IS NOT NULL"
        else:
            return f"{self._col} IS NULL"

    def q_in(self, list_value, reverse=False):
        candidates = ", ".join(list_value)
        if reverse:
            return f"{self._col} NOT IN ({candidates})"
        else:
            return f"{self._col} IN ({candidates})"

    def q_exists(self, sub_query, reverse=False):
        if reverse:
            return f"NOT EXISTS {sub_query}"
        else:
            return f"EXISTS {sub_query}"

    def q_like(self, pattern):
        return f"{self._col} LIKE {pattern}"


if __name__ == "__main__":
    from_clause = SQL.define_table(table_names=["A_TABLE"])
    select_clause = SQL.define_select_column(cols=["A_COLUMN", "COUNT(*) AS COUNT_INFO"])
    group_clause = SQL.define_group(cols=["A_COLUMN"])
    having_sql = ConditionQuery("A_COLUMN")
    having_clause = having_sql.q_greather(5, agg_f="sum")
    having_clause = ClauseManagement.finish_query(having_clause, method="having")
    where_sql = ConditionQuery("A_COLUMN")
    where_q1 = where_sql.q_between(10, 15)
    where_q2 = where_sql.q_greather(10)
    where_q3 = ClauseManagement.add_condition(where_q1, where_q2, "AND")
    where_clause = ClauseManagement.finish_query(where_q3, method="where")

    group_count_table = SQL.make_query(
        select_c=select_clause,
        from_c=from_clause,
        group_c=group_clause,
        where_c=where_clause,
        having_c=having_clause,
    )
    print(SQL.finish_query(group_count_table))
    temp_table_query = SQL.define_temp_table("CT", group_count_table)

    from_clause = SQL.define_table(table_names=["CT"])
    select_clause = SQL.define_select_column(
        cols=["CT.A_COLUMN", "CT.COUNT_INFO", "SUM(CT.COUNT_INFO) AS TOTAL", "CT.COUNT_INFO / TOTAL AS RATIO"]
    )
    summary_query = SQL.make_query(select_c=select_clause, from_c=from_clause)

    print(SQL.finish_query(SQL.concat_query(temp_table_query, summary_query)))
