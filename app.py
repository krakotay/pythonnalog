# %%
import polars as pl
name = input('Название файла: ')
columns = ['skip', 'data', 'operation', 'also_operation', 'debet_number', 'debet_rub', 'credit_number', 'credit_rub', 'd', 'saldo']

df = pl.read_csv(name + '.txt', separator='\t', has_header=False, encoding='utf8', ignore_errors=True, infer_schema_length=0, new_columns=columns)
df

# %%
df = df.drop(['skip', 'saldo', 'd'])
df

# %%
df['also_operation'].value_counts().sort('count')

# %%
df = df.filter(~pl.all_horizontal(pl.col('also_operation') == "<...>"))
df

# %%
df.filter(
    pl.col('also_operation') == 'Реализация'
)

# %%
df = df.filter(~pl.all_horizontal(pl.all().is_null()))
df

# %%
df = df.with_columns(
    pl.col('data').is_not_null().cum_sum().alias('separator')
)
df

# %%
new_df = df.group_by('separator').agg(
    pl.col('also_operation').unique(maintain_order=True), pl.col(['data', 'operation', 'debet_number', 'debet_rub', 'credit_number', 'credit_rub']).drop_nulls()
)
new_df

# %%
new_df.filter(
    pl.any_horizontal(pl.col(['also_operation']).list.contains('г. Новокузнецк (Планета) Обособленное подразделениене не использовать с 03.06.2024'))
)

# %%
new_df = new_df.with_columns(
    pl.when(pl.col("also_operation").list[1].str.starts_with("Прочие внереализационные"))
    .then(pl.col("also_operation").list.slice(1))
    .otherwise(pl.col("also_operation"))
).with_columns(
    pl.when(pl.col("also_operation").list[1].str.starts_with("Прочие расходы"))
    .then(pl.col("also_operation").list.slice(2))
    .otherwise(pl.col("also_operation")),
    # pl.col('also_operation').list.get(1).str.starts_with("Прочие") pl.col('also_operation').list.shift(1)
)
new_df

# %%
new_df.filter(
    pl.any_horizontal(pl.col(['also_operation']).list.contains('г. Новокузнецк (Планета) Обособленное подразделениене не использовать с 03.06.2024'))
)

# %%
new_df = new_df.with_columns(
    pl.col('also_operation').list.get(0).alias('operation_name'),
    pl.col('also_operation').list.get(1).alias('product_name'),
    pl.col('also_operation').list.slice(2).list.join(' ').alias('trash')

).drop('also_operation').sort('separator')
new_df

# %%
new_df = new_df.explode(['credit_number', 'data', 'operation', 'debet_number']).sort('separator')
new_df

# %%
pre_final_df = new_df.with_columns(
    pl.max_horizontal(pl.col('credit_rub').list.get(1, null_on_oob=True),pl.col('debet_rub').list.get(1, null_on_oob=True) ).alias('number'),
    pl.col('credit_rub').list.get(0, null_on_oob=True).alias('credit_rub'),
    pl.col('debet_rub').list.get(0, null_on_oob=True).alias('debet_rub'),
)
pre_final_df

# %%
final = pre_final_df.with_columns(
    pl.col('debet_rub').str.replace_all(r"\s+", "").str.replace(',', '.').cast(pl.Float64),
    pl.col('credit_rub').str.replace_all(r"\s+", "").str.replace(',', '.').cast(pl.Float64),
)
print("суммы: ", final['debet_rub'].sum(), final['credit_rub'].sum())

# %%
final.write_excel(name + ".xlsx")
final.write_csv(name + ".csv")



