import csv
import logging
import pprint
from typing import Any

import pyspark

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s: %(message)s", level="INFO"
)
logger = logging.getLogger("pyspark_test")

REASON_DIFF_TYPE = "diff_type"
REASON_DIFF_VALUE = "diff_value"
REASON_DIFF_LIST_LEN = "diff_list_len"


def _check_isinstance(left: Any, right: Any, cls):
    assert isinstance(
        left, cls
    ), f"Left expected type {cls}, found {type(left)} instead"
    assert isinstance(
        right, cls
    ), f"Right expected type {cls}, found {type(right)} instead"


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


def _check_row_count(left_df, right_df):
    left_df_count = left_df.count()
    right_df_count = right_df.count()
    assert (
        left_df_count == right_df_count
    ), f"Number of rows are not same.\n\nActual Rows: {left_df_count}\nExpected Rows: {right_df_count}\n"


def _diff_row(
    left_row,
    right_row,
    column_name,
    row_id,
    row_index,
    recursive,
    column_name_parent: str = "",
    sorting_keys: dict = None,
) -> list:
    differences = []
    if isinstance(left_row, pyspark.sql.types.Row):
        left_row = left_row.asDict(True)
    if isinstance(right_row, pyspark.sql.types.Row):
        right_row = right_row.asDict(True)
    if left_row and right_row and left_row != right_row:
        diff = {
            "row_id": row_id,
            "row_index": row_index + 1,
            "column_name": column_name,
            "column_name_parent": column_name_parent,
            "left_row": left_row,
            "right_row": right_row,
        }
        # If not same instance -> no need for more checks, we can't compare
        if not isinstance(left_row, type(right_row)):
            diff["diff_reason"] = REASON_DIFF_TYPE
            differences.append(diff)
        # Iterate dict recursively if requested
        elif recursive and isinstance(left_row, dict):
            for key in left_row:
                differences.extend(
                    _diff_row(
                        left_row=left_row[key],
                        right_row=right_row[key],
                        column_name=key,
                        row_id=row_id,
                        row_index=row_index,
                        recursive=recursive,
                        column_name_parent=".".join(
                            filter(bool, [column_name_parent, column_name])
                        ),
                        sorting_keys=sorting_keys,
                    )
                )
        # Iterate list recursively if requested
        elif recursive and isinstance(left_row, list):
            if len(left_row) != len(right_row):
                diff["diff_reason"] = REASON_DIFF_LIST_LEN
                differences.append(diff)
            else:
                if sorting_keys and column_name in sorting_keys:
                    left_row = sorted(left_row, key=sorting_keys[column_name])
                    right_row = sorted(right_row, key=sorting_keys[column_name])
                for i in range(len(left_row)):
                    column_name_parent = ".".join(
                        filter(bool, [column_name_parent, column_name])
                    )
                    differences.extend(
                        _diff_row(
                            left_row=left_row[i],
                            right_row=right_row[i],
                            column_name=f"[{i}]",
                            row_id=row_id,
                            row_index=row_index,
                            recursive=recursive,
                            column_name_parent=column_name_parent,
                            sorting_keys=sorting_keys,
                        )
                    )
        else:
            diff["diff_reason"] = REASON_DIFF_VALUE
            differences.append(diff)

    return differences


def _diff_df_content(
    left_df: pyspark.sql.DataFrame,
    right_df: pyspark.sql.DataFrame,
    return_all_differences: bool = False,
    id_field: str = None,
    recursive: bool = False,
    skip_n_first_rows: int = 0,
    order_by: list = None,
    columns: list = None,
    sorting_keys: dict = None,
) -> list:
    differences = []

    left_df_list = left_df.collect()[skip_n_first_rows:]
    right_df_list = right_df.collect()[skip_n_first_rows:]

    if skip_n_first_rows and order_by:
        left_df_list = sorted(left_df_list, key=lambda r: [r[s] for s in order_by])
        right_df_list = sorted(right_df_list, key=lambda r: [r[s] for s in order_by])

    if id_field and (
        id_field not in left_df.columns or id_field not in left_df.columns
    ):
        raise ValueError(f"id_field {id_field} not present in the input dataframes")

    for row_index in range(len(left_df_list)):
        row_id = None
        columns = columns or left_df.columns

        if id_field:
            row_id = left_df_list[row_index][id_field]
            if id_field not in columns:
                columns.append(id_field)

        for column_name in columns:
            left_row = left_df_list[row_index][column_name]
            right_row = right_df_list[row_index][column_name]
            diff = _diff_row(
                left_row=left_row,
                right_row=right_row,
                column_name=column_name,
                row_id=row_id,
                row_index=row_index,
                recursive=recursive,
                sorting_keys=sorting_keys,
            )
            if diff:
                differences.extend(diff)
                if not return_all_differences:
                    return differences

    return differences


def diff(
    left_df: pyspark.sql.DataFrame,
    right_df: pyspark.sql.DataFrame,
    check_dtype: bool = True,
    check_column_names: bool = True,
    check_columns_in_order: bool = False,
    order_by: list = None,
    return_all_differences: bool = False,
    id_field: str = None,
    output_differences_file: str = None,
    recursive: bool = False,
    skip_n_first_rows: int = 0,
    columns: list = None,
    sorting_keys: dict = None,
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
        return_all_differences (bool, optional): If true this method will check all the differences instead of stopping when finding the first. Defaults to False.
        id_field (str, optional): Name of the column that identifies the row, util when you need to sort the dataframes. Defaults to None.
        output_differences_file (str, optional): If provided, the differences found will be persisted to a csv in this path. Defaults to None.
        recursive (bool, optional): If provided, the check for differences will be done once the field does not contain another field inside, for example a string. Defaults to False.
        skip_n_first_rows (int, optional): If provided, the first n rows will be ignored. Defaults to 0.
        columns (list, optional): Compare only these columns. Defaults to None.
        sorting_keys (dict, optional): Sort specific columns if they are lists based on the key provided. Defaults to None.
    """

    logging.info(
        f"""
        Comparing pyspark differences. Params:\n
            left_df: {left_df}
            right_df: {right_df}
            check_dtype: {check_dtype}
            check_column_names: {check_column_names}
            check_columns_in_order: {check_columns_in_order}
            order_by: {order_by}
            return_all_differences: {return_all_differences}
            id_field: {id_field}
            output_differences_file: {output_differences_file}
            recursive: {recursive}
            skip_n_first_rows: {skip_n_first_rows}
            columns: {columns}
            sorting_keys: {sorting_keys}
    """
    )

    # Check both inputs are dataframes
    _check_isinstance(left_df, right_df, pyspark.sql.DataFrame)

    # Check Column Names
    if check_column_names:
        _check_columns(check_columns_in_order, left_df, right_df)

    # Check Column Data Types
    if check_dtype:
        _check_schema(check_columns_in_order, left_df, right_df)

    # Check number of rows
    _check_row_count(left_df, right_df)

    # Sort df
    if order_by and not skip_n_first_rows:
        left_df = left_df.orderBy(order_by)
        right_df = right_df.orderBy(order_by)

    # Check dataframe content
    differences = _diff_df_content(
        left_df=left_df,
        right_df=right_df,
        return_all_differences=return_all_differences,
        id_field=id_field,
        recursive=recursive,
        skip_n_first_rows=skip_n_first_rows,
        order_by=order_by,
        columns=columns,
        sorting_keys=sorting_keys,
    )

    if differences:
        if output_differences_file:
            logger.warning(
                f"DATA MISMATCH! {len(differences)} differences found. Saving them to {output_differences_file}..."
            )
            with open("output_differences_file", "w") as fd:
                dict_writer = csv.DictWriter(fd, fieldnames=differences[0].keys())
                dict_writer.writeheader()
                dict_writer.writerows(differences)
        else:
            logger.warning(
                f"DATA MISMATCH! {len(differences)} differences found: \n{pprint.pformat(differences)}"
            )
    else:
        logging.info("Data is the same in both dataframes")
