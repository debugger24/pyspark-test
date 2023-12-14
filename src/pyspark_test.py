from typing import Any

import pyspark

try:
    from pyspark.sql.connect.dataframe import DataFrame as CDF

    has_connect_deps = True
except ImportError:
    has_connect_deps = False


def _check_isinstance_df(left: Any, right: Any):
    types_to_test = [pyspark.sql.DataFrame]
    msg_string = ""
    # If Spark Connect dependencies are not available, the input is not going to be a Spark Connect
    # DataFrame so we can safely skip the validation.
    if has_connect_deps:
        types_to_test.append(CDF)
        msg_string = " or {CDF}"

    left_good = any(map(lambda x: isinstance(left, x), types_to_test))
    right_good = any(map(lambda x: isinstance(right, x), types_to_test))
    assert (
        left_good
    ), f"Left expected type {pyspark.sql.DataFrame}{msg_string}, found {type(left)} instead"
    assert (
        right_good
    ), f"Right expected type {pyspark.sql.DataFrame}{msg_string}, found {type(right)} instead"

    # Check that both sides are of the same DataFrame type.
    assert type(left) == type(
        right
    ), f"Left and right DataFrames are not of the same type: {type(left)} != {type(right)}"


def _check_columns(
    check_columns_in_order: bool,
    left_df: pyspark.sql.DataFrame,
    right_df: pyspark.sql.DataFrame,
):
    if check_columns_in_order:
        assert left_df.columns == right_df.columns, "df columns name mismatch"
    else:
        assert sorted(left_df.columns) == sorted(
            right_df.columns
        ), "df columns name mismatch"


def _check_schema(
    check_columns_in_order: bool,
    left_df: pyspark.sql.DataFrame,
    right_df: pyspark.sql.DataFrame,
):
    if check_columns_in_order:
        assert left_df.dtypes == right_df.dtypes, "df schema type mismatch"
    else:
        assert sorted(left_df.dtypes, key=lambda x: x[0]) == sorted(
            right_df.dtypes, key=lambda x: x[0]
        ), "df schema type mismatch"


def _check_df_content(
    left_df: pyspark.sql.DataFrame,
    right_df: pyspark.sql.DataFrame,
):
    left_df_list = left_df.collect()
    right_df_list = right_df.collect()

    for row_index in range(len(left_df_list)):
        for column_name in left_df.columns:
            left_cell = left_df_list[row_index][column_name]
            right_cell = right_df_list[row_index][column_name]
            if left_cell == right_cell:
                assert True
            elif left_cell is None and right_cell is None:
                assert True
            # elif math.isnan(left_cell) and math.isnan(right_cell):
            #     assert True
            else:
                msg = f"Data mismatch\n\nRow = {row_index + 1} : Column = {column_name}\n\nACTUAL: {left_cell}\nEXPECTED: {right_cell}\n"
                assert False, msg


def _check_row_count(left_df, right_df):
    left_df_count = left_df.count()
    right_df_count = right_df.count()
    assert (
        left_df_count == right_df_count
    ), f"Number of rows are not same.\n\nActual Rows: {left_df_count}\nExpected Rows: {right_df_count}\n"


def assert_pyspark_df_equal(
    left_df: pyspark.sql.DataFrame,
    right_df: pyspark.sql.DataFrame,
    check_dtype: bool = True,
    check_column_names: bool = False,
    check_columns_in_order: bool = False,
    order_by: list = None,
) -> None:
    """
    Used to test if two dataframes are same or not

    Args:
        left_df (pyspark.sql.DataFrame): Left Dataframe
        right_df (pyspark.sql.DataFrame): Right Dataframe
        check_dtype (bool, optional): Comapred both dataframe have same column and colum type or not. If using check_dtype then check_column_names is not required. Defaults to True.
        check_column_names (bool, optional): Comapare both dataframes have same column or not. Defaults to False.
        check_columns_in_order (bool, optional): Check columns in order. Defaults to False.
        order_by (list, optional): List of column names if we want to sort dataframe before comparing. Defaults to None.
    """

    # Check if
    _check_isinstance_df(left_df, right_df)

    # Check Column Names
    if check_column_names:
        _check_columns(check_columns_in_order, left_df, right_df)

    # Check Column Data Types
    if check_dtype:
        _check_schema(check_columns_in_order, left_df, right_df)

    # Check number of rows
    _check_row_count(left_df, right_df)

    # Sort df
    if order_by:
        left_df = left_df.orderBy(order_by)
        right_df = right_df.orderBy(order_by)

    # Check dataframe content
    _check_df_content(left_df, right_df)
