if __name__ = "__main__":
    from etl_manager import meta
    db_meta = meta.DatabaseMeta("glue/meta_data/occupeye_db/", db_suffix = "")
    db_meta.create_glue_database()