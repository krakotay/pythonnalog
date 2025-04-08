import polars as pl
from time import time


def process_with_41(file_list: list[str], file_prefix: str, skip_lines: int):
    name = file_prefix
    columns = [
        "skip",
        "data",
        "operation",
        "also_operation",
        "debet_number",
        "debet_rub",
        "credit_number",
        "credit_rub",
        "d",
        "saldo",
    ]

    dfs = [
        pl.scan_csv(
            file,
            separator="\t",
            has_header=False,
            encoding="utf8",
            ignore_errors=True,
            infer_schema_length=0,
            new_columns=columns,
            skip_lines=skip_lines,
        )
        for file in file_list
    ]
    df = pl.concat(dfs)
    df = df.drop(["skip", "saldo", "d"])
    df = df.filter(~pl.all_horizontal(pl.col('also_operation') == "<...>"))

    df = df.filter(~pl.all_horizontal(pl.all().is_null()))
    df = df.with_columns(pl.col("data").is_not_null().cum_sum().alias("separator"))
    df = df.group_by("separator").agg(
        pl.col("also_operation").unique(maintain_order=True),
        pl.col(
            [
                "data",
                "operation",
                "debet_number",
                "debet_rub",
                "credit_number",
                "credit_rub",
            ]
        ).drop_nulls(),
    )
    df = (
        df.with_columns(
            pl.col("also_operation").list.get(0).alias("operation_name"),
            pl.col("also_operation").list.get(1).alias("product_name"),
            pl.col("also_operation").list.slice(2).list.join(" ").alias("trash"),
        )
        .drop("also_operation")
        .sort("separator")
    )
    df = df.with_columns(pl.col("credit_number").list.slice(0, 1))
    df = df.explode(["credit_number", "data", "operation", "debet_number"]).sort(
        "separator"
    )
    df = df.with_columns(
        pl.max_horizontal(
            pl.col("credit_rub").list.get(1, null_on_oob=True),
            pl.col("debet_rub").list.get(1, null_on_oob=True),
        ).alias("number"),
        pl.col("credit_rub").list.get(0, null_on_oob=True).alias("credit_rub"),
        pl.col("debet_rub").list.get(0, null_on_oob=True).alias("debet_rub"),
    )
    df = df.with_columns(
        pl.col("debet_rub").str.replace_all(r"\s+", ""),
        pl.col("credit_rub").str.replace_all(r"\s+", ""),
    )
    df = df.with_columns(
        pl.col("product_name").str.split(" ").list.get(0).alias("article")
    )
    tmp_name = f"{name}_{int(time())}.csv"
    df.sink_csv(tmp_name)
    print("ready!")
    return tmp_name
