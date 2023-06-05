from pyspark_diff import diff
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("pyspark_diff_text").getOrCreate()


def test_validate_input_types():
    pass


def test_diff_columns():
    pass


def test_diff_schema():
    pass


def test_diff_row_count():
    pass


def test_no_diff():
    df1 = spark.createDataFrame(
        data=[({"dict_key": "dict_value"},)], schema=["colname"]
    )
    assert not diff(df1, df1)


def test_return_only_first_diff():
    pass


def test_return_all_diffs():
    pass


def test_order_by():
    pass


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
