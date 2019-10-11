if __name__ == "__main__":
    from etl_manager import meta

    db_meta = meta.read_database_folder("glue/meta_data/occupeye_db/")
    db_meta.create_glue_database()
