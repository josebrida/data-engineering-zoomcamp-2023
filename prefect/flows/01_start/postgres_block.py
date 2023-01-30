from prefect_sqlalchemy import DatabaseCredentials
from prefect_sqlalchemy import SqlAlchemyConnector, SyncDriver

# alternative to creating GCP blocks in the UI
# insert your own service_account_file path or service_account_info dictionary from the json file
# IMPORTANT - do not store credentials in a publicly available repository!


credentials_block = DatabaseCredentials(driver=SyncDriver.POSTGRESQL_PSYCOPG2,
                                        database="ny_taxi",
                                        username = "root",
                                        password = "root",
                                        host = "localhost",
                                        port = "5432")  # enter your credentials info or use the file method.
credentials_block.save("zoom-postgres-creds", overwrite=True)


postgres_block = SqlAlchemyConnector(
    connection_info=DatabaseCredentials.load("zoom-postgres-creds")
)

postgres_block.save("zoom-postgres", overwrite=True)