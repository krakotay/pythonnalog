import polars as pl
from time import time


def process_with_90(file_list: list[str], file_prefix: str, skip_lines: int):
    name = file_prefix

    columns = [
        "skip",
        "data",
        "operation",
        "also_operation",
        "x_number",
        "x_rub",
        "y_number",
        "y_rub",
        "d-k",
        "saldo",
    ]

    dfs = [
        pl.read_csv(
            file,
            separator="\t",
            has_header=False,
            encoding="utf8",
            ignore_errors=True,
            infer_schema_length=0,
            new_columns=columns,
            skip_lines=skip_lines,
        ).slice(0, -2).lazy()
        for file in file_list
    ]
    df = pl.concat(dfs)

    df = df.drop(["skip"])
    df = df.filter(
        ~pl.all_horizontal(
            pl.col("also_operation") == "<...>",
            pl.all().exclude("also_operation").is_null(),
        ),
    )
    df = df.with_columns(pl.col("data").is_not_null().cum_sum().alias("separator"))
    print(df)

    df = df.group_by("separator").agg(
        pl.col("also_operation").unique(maintain_order=True),
        pl.col(
            ["data", "operation", "x_number", "x_rub", "y_number", "y_rub", "d-k"]
        ).drop_nulls(),
    )
    df = (
        df.with_columns(
            pl.col("also_operation")
            .list.get(0, null_on_oob=True)
            .alias("operation_name"),
            pl.col("also_operation")
            .list.get(1, null_on_oob=True)
            .alias("product_name"),
            pl.col("d-k").list.get(0, null_on_oob=True),
            pl.col("x_number").list.get(0, null_on_oob=True),
            pl.col("x_rub").list.get(0, null_on_oob=True),
            pl.col("y_number").list.get(0, null_on_oob=True),
            pl.col("y_rub").list.get(0, null_on_oob=True),
            pl.col("data").list.get(0, null_on_oob=True),
            pl.col("operation").list.get(0, null_on_oob=True),
            pl.col("also_operation").list.join(" ").alias("trash"),
        )
        .drop("also_operation")
        .sort("separator")
    )
    df = (
        df.with_columns(
            pl.when(pl.col("y_rub").str.ends_with("000")).then(
                pl.col("y_rub").alias("number")
            )
        )
        .with_columns(
            pl.when(pl.col("y_rub").str.ends_with("000"))
            .then(None)
            .otherwise(pl.col("y_rub"))
            .name.keep()
        )
        .drop("d-k")
    )
    df = df.rename(
        {
            "x_number": "debet_number",
            "x_rub": "debet_rub",
            "y_number": "credit_number",
            "y_rub": "credit_rub",
        }
    )
    df = df.with_columns(
        pl.col("product_name").str.split(" ").list.get(0).alias("article")
    )

    tmp_name = f"{name}_{int(time())}.csv"
    df.sink_csv(tmp_name)
    return tmp_name
