from pyspark_diff import diff_objs
from pyspark.sql import SparkSession

from pyspark_diff import Difference

spark = SparkSession.builder.appName("pyspark_diff_text").getOrCreate()


def test_validate_input_types():
    pass


def test_diff_columns():
    pass


def test_diff_schema():
    pass


def test_diff_row_count():
    data1 = [({"k1": "v1"},)]
    data2 = [({"k1": "v1"},), ({"k1": "v2"},)]
    df1 = spark.createDataFrame(data=data1, schema=["colname"])
    df2 = spark.createDataFrame(data=data2, schema=["colname"])
    differences = diff_objs(df1, df2)
    assert differences == [
        Difference(
            row_id=0,
            column_name="",
            column_name_parent="",
            left=1,
            right=2,
            reason="diff_row_count",
        )
    ]


def test_no_diff():
    data1 = [({"k1": "VALUE 1"},), ({"k1": "VALUE 2"},)]
    data2 = [({"k1": "VALUE 1"},), ({"k1": "VALUE 2"},)]
    df1 = spark.createDataFrame(data=data1, schema=["colname"])
    df2 = spark.createDataFrame(data=data2, schema=["colname"])
    differences = diff_objs(df1, df2)
    assert not differences


def test_only_first_diff__no_recursive():
    """Second difference is ignored as return_all_differences is False"""
    data1 = [({"k1": "VALUE 1"},), ({"k1": "VALUE 2"},)]
    data2 = [({"k1": "VALUE 1 DIFF"},), ({"k1": "VALUE 2 DIFF"},)]
    df1 = spark.createDataFrame(data=data1, schema=["colname"])
    df2 = spark.createDataFrame(data=data2, schema=["colname"])
    differences = diff_objs(df1, df2, return_all_differences=False, recursive=False)
    assert differences == [
        Difference(
            row_id=None,
            column_name="colname",
            column_name_parent="",
            left={"k1": "VALUE 1"},
            right={"k1": "VALUE 1 DIFF"},
            reason="diff_value",
        )
    ]


def test_all_diffs__no_recursive():
    data1 = [({"k1": "VALUE 1"},), ({"k1": "VALUE 2"},)]
    data2 = [({"k1": "VALUE 1 DIFF"},), ({"k1": "VALUE 2 DIFF"},)]
    df1 = spark.createDataFrame(data=data1, schema=["colname"])
    df2 = spark.createDataFrame(data=data2, schema=["colname"])
    differences = diff_objs(df1, df2, recursive=False)
    assert differences == [
        Difference(
            row_id=None,
            column_name="colname",
            column_name_parent="",
            left={"k1": "VALUE 1"},
            right={"k1": "VALUE 1 DIFF"},
            reason="diff_value",
        ),
        Difference(
            row_id=None,
            column_name="colname",
            column_name_parent="",
            left={"k1": "VALUE 2"},
            right={"k1": "VALUE 2 DIFF"},
            reason="diff_value",
        ),
    ]


def test_order_by__no_diff():
    """Sort rows based on col2_sort"""
    data1 = [({"k1": "v1"}, "1"), ({"k1": "v2"}, "2")]
    data2 = [({"k1": "v2"}, "2"), ({"k1": "v1"}, "1")]
    df1 = spark.createDataFrame(data=data1, schema=["col1", "col2_sort"])
    df2 = spark.createDataFrame(data=data2, schema=["col1", "col2_sort"])
    differences = diff_objs(df1, df2, order_by=["col2_sort"])
    assert not differences


def test_order_by__no_diff__multiple_sorting_levels():
    """Sort rows based on 3 columns"""
    data1 = [("1", "1", "1"), ("1", "1", "2"), ("1", "2", "3"), ("2", "3", "3")]
    data2 = [("2", "3", "3"), ("1", "2", "3"), ("1", "1", "2"), ("1", "1", "1")]
    df1 = spark.createDataFrame(data=data1, schema=["col1", "col2", "col3"])
    df2 = spark.createDataFrame(data=data2, schema=["col1", "col2", "col3"])
    differences = diff_objs(df1, df2, order_by=["col1", "col2", "col3"])
    assert not differences


def test_order_by__diff__multiple_sorting_levels():
    data1 = [("1", "1", "1"), ("1", "1", "2"), ("1", "2", "3"), ("2", "3", "3")]
    data2 = [("2", "3", "DIFF"), ("1", "2", "3"), ("1", "1", "2"), ("1", "1", "1")]
    df1 = spark.createDataFrame(data=data1, schema=["col1", "col2", "col3"])
    df2 = spark.createDataFrame(data=data2, schema=["col1", "col2", "col3"])
    differences = diff_objs(df1, df2, order_by=["col1", "col2", "col3"])
    assert differences == [
        Difference(
            row_id=None,
            column_name="col3",
            column_name_parent="",
            left="3",
            right="DIFF",
            reason="diff_value",
        )
    ]


def test_skip_n_first_rows():
    """Skip the first 2 rows with pyspark"""
    data1 = [("header1a", "header1b"), ("header2a", "header2b"), ("a", "b")]
    data2 = [
        ("header1a_diff", "header1b_diff"),
        ("header2a_diff", "header2b_diff"),
        ("a", "b"),
    ]
    df1 = spark.createDataFrame(data=data1, schema=["col1", "col2"])
    df2 = spark.createDataFrame(data=data2, schema=["col1", "col2"])
    differences = diff_objs(df1, df2, skip_n_first_rows=2)
    assert not differences


def test_order_by__and__skip_n_first_rows():
    """Sorting is not done with pyspark"""
    data1 = [
        ("header1a", "header1b"),
        ("header2a", "header2b"),
        ("1", "1"),
        ("1", "2"),
    ]
    data2 = [
        ("header1a_diff", "header1b_diff"),
        ("header2a_diff", "header2b_diff"),
        ("1", "2"),
        ("1", "1"),
    ]
    df1 = spark.createDataFrame(data=data1, schema=["col1", "col2"])
    df2 = spark.createDataFrame(data=data2, schema=["col1", "col2"])
    differences = diff_objs(df1, df2, skip_n_first_rows=2, order_by=["col1", "col2"])
    assert not differences


def test_id_field():
    data1 = [("id1", "val1"), ("id2", "val2")]
    data2 = [("id1", "val1"), ("id2", "val2diff")]
    df1 = spark.createDataFrame(data=data1, schema=["id", "col1"])
    df2 = spark.createDataFrame(data=data2, schema=["id", "col1"])
    differences = diff_objs(df1, df2, id_field="id")
    assert differences == [
        Difference(
            row_id="id2",
            column_name="col1",
            column_name_parent="",
            left="val2",
            right="val2diff",
            reason="diff_value",
        )
    ]


def test_recursive_dict():
    data1 = [
        ("id1", {"a": {"b": {"c": "value1"}}}),
        ("id2", {"a": {"b": {"c": "value2"}}}),
    ]
    data2 = [
        ("id1", {"a": {"b": {"c": "value1"}}}),
        ("id2", {"a": {"b": {"c": "value2_diff"}}}),
    ]
    df1 = spark.createDataFrame(data=data1, schema=["id", "col1"])
    df2 = spark.createDataFrame(data=data2, schema=["id", "col1"])
    differences = diff_objs(df1, df2, id_field="id")
    assert differences == [
        Difference(
            row_id="id2",
            column_name="c",
            column_name_parent="col1.a.b",
            left="value2",
            right="value2_diff",
            reason="diff_value",
        )
    ]


def test_recursive_list():
    data1 = [
        (
            "id1",
            {"a": [{"b": [{"c": [1, 2, 3]}]}], "a2": [{"b2": [{"c2": [1, 2, 3]}]}]},
        ),
        (
            "id2",
            {"a": [{"b": [{"c": [1, 2, 3]}]}], "a2": [{"b2": [{"c2": [1, 2, 3]}]}]},
        ),
        (
            "id3",
            {"a": [{"b": [{"c": [1, 2, 3]}]}], "a2": [{"b2": [{"c2": [1, 2, 3]}]}]},
        ),
    ]
    data2 = [
        (
            "id1",
            {"a": [{"b": [{"c": [1, 2, 3]}]}], "a2": [{"b2": [{"c2": [1, 2, 3]}]}]},
        ),
        (
            "id2",
            {"a": [{"b": [{"c": [1, 2, 3]}]}], "a2": [{"b2": [{"c2": [1, 2, 1337]}]}]},
        ),
        (
            "id3",
            {"a": [{"b": [{"c": [1337, 2, 3]}]}], "a2": [{"b2": [{"c2": [1, 2, 3]}]}]},
        ),
    ]
    df1 = spark.createDataFrame(data=data1, schema=["id", "col1"])
    df2 = spark.createDataFrame(data=data2, schema=["id", "col1"])
    differences = diff_objs(df1, df2, id_field="id")
    assert differences == [
        Difference(
            row_id="id2",
            column_name="[2]",
            column_name_parent="col1.a2.[0].b2.[0].c2",
            left=3,
            right=1337,
            reason="diff_value",
        ),
        Difference(
            row_id="id3",
            column_name="[0]",
            column_name_parent="col1.a.[0].b.[0].c",
            left=1,
            right=1337,
            reason="diff_value",
        ),
    ]


def test_specific_columns():
    data1 = [
        ("id1", "v1", "v2", "v3"),
        ("id2", "v1", "v2", "v3"),
        ("id3", "v1", "v2", "v3"),
        ("id4", "v1", "v2", "v3"),
    ]
    data2 = [
        ("id1", "v1", "v2diff", "v3diff"),
        ("id2", "v1", "v2", "v3diff"),
        ("id3", "v1", "v2diff", "v3"),
        ("id4", "v1diff", "v2", "v3"),
    ]
    df1 = spark.createDataFrame(data=data1, schema=["id", "col1", "col2", "col3"])
    df2 = spark.createDataFrame(data=data2, schema=["id", "col1", "col2", "col3"])
    differences = diff_objs(df1, df2, id_field="id", columns=["col1"])
    assert differences == [
        Difference(
            row_id="id4",
            column_name="col1",
            column_name_parent="",
            left="v1",
            right="v1diff",
            reason="diff_value",
        )
    ]


def test_diff_all_columns():
    data1 = [
        ("id1", "v1", "v2", "v3"),
        ("id2", "v1", "v2", "v3"),
        ("id3", "v1", "v2", "v3"),
        ("id4", "v1", "v2", "v3"),
    ]
    data2 = [
        ("id1", "v1", "v2diff", "v3diff"),
        ("id2", "v1", "v2", "v3diff"),
        ("id3", "v1", "v2diff", "v3"),
        ("id4", "v1diff", "v2", "v3"),
    ]
    df1 = spark.createDataFrame(data=data1, schema=["id", "col1", "col2", "col3"])
    df2 = spark.createDataFrame(data=data2, schema=["id", "col1", "col2", "col3"])
    differences = diff_objs(df1, df2, id_field="id")
    assert differences == [
        Difference(
            row_id="id1",
            column_name="col2",
            column_name_parent="",
            left="v2",
            right="v2diff",
            reason="diff_value",
        ),
        Difference(
            row_id="id1",
            column_name="col3",
            column_name_parent="",
            left="v3",
            right="v3diff",
            reason="diff_value",
        ),
        Difference(
            row_id="id2",
            column_name="col3",
            column_name_parent="",
            left="v3",
            right="v3diff",
            reason="diff_value",
        ),
        Difference(
            row_id="id3",
            column_name="col2",
            column_name_parent="",
            left="v2",
            right="v2diff",
            reason="diff_value",
        ),
        Difference(
            row_id="id4",
            column_name="col1",
            column_name_parent="",
            left="v1",
            right="v1diff",
            reason="diff_value",
        ),
    ]


def test_sorting_keys():
    data1 = [
        ("id1", [{"a": 1, "b": 1}, {"a": 1, "b": 2}, {"a": 2, "b": 1}]),
    ]
    data2 = [
        ("id1", [{"a": 2, "b": 1}, {"a": 1, "b": 2}, {"a": 1, "b": 1}]),
    ]
    df1 = spark.createDataFrame(data=data1, schema=["id", "col1"])
    df2 = spark.createDataFrame(data=data2, schema=["id", "col1"])
    differences = diff_objs(df1, df2, id_field="id")
    assert differences == [
        Difference(
            row_id="id1",
            column_name="a",
            column_name_parent="col1.[0]",
            left=1,
            right=2,
            reason="diff_value",
        ),
        Difference(
            row_id="id1",
            column_name="a",
            column_name_parent="col1.[2]",
            left=2,
            right=1,
            reason="diff_value",
        ),
    ]
    differences = diff_objs(
        df1, df2, id_field="id", sorting_keys={"col1": lambda x: (x["a"], x["b"])}
    )
    assert not differences
