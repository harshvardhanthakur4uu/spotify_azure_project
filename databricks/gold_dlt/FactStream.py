import dlt

@dlt.table
def factstream_stg():
    df = spark.readStream.table("spotify_catalog.silver.factstream")
    return df


dlt.create_streaming_table("factstream")

dlt.create_auto_cdc_flow(
    target= "factstream",
    source= "factstream_stg",
    keys= ["stream_id"],
    sequence_by= "stream_timestamp",
    apply_as_deletes = None,
    apply_as_truncates = None,
    except_column_list = None,
    stored_as_scd_type = 1,
    track_history_column_list = None,
    track_history_except_column_list = None
)