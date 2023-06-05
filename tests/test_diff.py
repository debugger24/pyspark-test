from pyspark_diff import diff
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
    differences = diff(df1, df2)
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
    differences = diff(df1, df2)
    assert not differences


def test_return_only_first_diff():
    """Second difference is ignored as return_all_differences is False"""
    data1 = [({"k1": "VALUE 1"},), ({"k1": "VALUE 2"},)]
    data2 = [({"k1": "VALUE 1 DIFF"},), ({"k1": "VALUE 2 DIFF"},)]
    df1 = spark.createDataFrame(data=data1, schema=["colname"])
    df2 = spark.createDataFrame(data=data2, schema=["colname"])
    differences = diff(df1, df2)
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


def test_return_all_diffs():
    data1 = [({"k1": "VALUE 1"},), ({"k1": "VALUE 2"},)]
    data2 = [({"k1": "VALUE 1 DIFF"},), ({"k1": "VALUE 2 DIFF"},)]
    df1 = spark.createDataFrame(data=data1, schema=["colname"])
    df2 = spark.createDataFrame(data=data2, schema=["colname"])
    differences = diff(df1, df2, return_all_differences=True)
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
    differences = diff(df1, df2, order_by=["col2_sort"])
    assert not differences


def test_order_by__no_diff__multiple_sorting_levels():
    """Sort rows based on 3 columns"""
    data1 = [("1", "1", "1"), ("1", "1", "2"), ("1", "2", "3"), ("2", "3", "3")]
    data2 = [("2", "3", "3"), ("1", "2", "3"), ("1", "1", "2"), ("1", "1", "1")]
    df1 = spark.createDataFrame(data=data1, schema=["col1", "col2", "col3"])
    df2 = spark.createDataFrame(data=data2, schema=["col1", "col2", "col3"])
    differences = diff(df1, df2, order_by=["col1", "col2", "col3"])
    assert not differences


def test_order_by__diff__multiple_sorting_levels():
    data1 = [("1", "1", "1"), ("1", "1", "2"), ("1", "2", "3"), ("2", "3", "3")]
    data2 = [("2", "3", "DIFF"), ("1", "2", "3"), ("1", "1", "2"), ("1", "1", "1")]
    df1 = spark.createDataFrame(data=data1, schema=["col1", "col2", "col3"])
    df2 = spark.createDataFrame(data=data2, schema=["col1", "col2", "col3"])
    differences = diff(df1, df2, order_by=["col1", "col2", "col3"])
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
    pass


def test_order_by__and__skip_n_first_rows():
    """Sorting is not done with pyspark"""
    pass


def test_id_field():
    pass


def test_no_recursive():
    pass


def test_specific_columns():
    pass


def test_all_columns():
    pass


def test_sorting_keys():
    pass
