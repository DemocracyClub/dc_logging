from models.models import GlueDatabase

dc_wide_logs_db = GlueDatabase(database_name="dc-wide-logs")
polling_stations_public_data_db = GlueDatabase(
    database_name="pollingstations.public.data"
)
