# sample_silver_runner.py
# Example snippet showing how this dataset would be processed in the orchestrator

def process_sales_orders(bronze_df, schema):
    casted_df = apply_cast_plan(bronze_df, schema["columns"])
    deduped_df = dedupe_lww(
        casted_df,
        keys=schema["primary_key"],
        order_col=schema["order_column"]
    )
    merge_to_silver(
        deduped_df,
        table="silver_sales.sales_orders",
        keys=schema["primary_key"]
    )
