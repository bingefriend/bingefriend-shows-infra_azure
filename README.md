# BingeFriend Shows Ingestion Service for Azure Functions

Azure Functions to maintain a complete cache of all TV show, season, and episode data from the [TV Maze API](https://www.tvmaze.com/api). Data is stored in an Azure SQL database.

This project is part of the [BingeFriend](https://github.com/bingefriend) suite, which provides tools and services for TV watchers to track and manage their viewing.

## Features

* Ingest HTTP trigger initates a fetch of all show index data page by page.
* Update Timer trigger initiates a fetch of all shows that have been updated since the last ingestion.
* Processes individual show records.
* For each show, orchestrates the fetching and processing of its seasons and episodes.
* Stores processed data (shows, genres, seasons, episodes) in a SQL database.
* Uses Alembic for database schema migrations.

## Setup

1.  **Clone the repository.**
2.  **Install dependencies:**
    ```bash
    # Dependencies are managed using Poetry.
    poetry install
    ```
3.  **Configure Environment Variables:** Create a `local.settings.json` file (or set environment variables directly) with the following values:

    * Direct dependencies:

      * `FUNCTIONS_WORKER_RUNTIME`: Set to `python`.
      * `AzureWebJobsStorage`: Connection string for Azure Storage (used by Durable Functions). For local development, `UseDevelopmentStorage=true` can be used if Azurite is running.
      * `AZURE_SQL_CONNECTION_STRING`: SQLAlchemy-compatible connection string for your Azure SQL or SQL Server database (e.g., `mssql+pymssql://USER:PASSWORD@SERVER:PORT/DATABASE`).
      * `SHOWS_TABLE`: Name of the database table for shows (e.g., `dbo.shows`).
      * `SHOW_GENRES_TABLE`: Name of the database table for show-genre relationships (e.g., `dbo.show_genre`).
      * `SEASONS_TABLE`: Name of the database table for seasons (e.g., `dbo.seasons`).
      * `EPISODES_TABLE`: Name of the database table for episodes (e.g., `dbo.episodes`).
 
    * bingefriend-shows-client_tvmaze:

      * `TVMAZE_API_BASE_URL`: Base URL for the TVMaze API (e.g., `https://api.tvmaze.com`).
      * `MAX_API_RETRIES`: Maximum number of retries for API calls.
      * `API_RETRY_BACKOFF_FACTOR`: Backoff factor for API retry attempts.

4. **Database Migrations:** Ensure the database schema is up-to-date using Alembic:
    ```bash
    # (Optional) Generate a new migration if models changed
    # alembic revision --autogenerate -m "Describe changes"

    # Apply migrations
    alembic upgrade head
    ```
    
5.  **Run the Azure Functions Host:**
    ```bash
    func start
    ```

## Dependencies

### BingeFriend libraries
* `bingefriend-shows-core`: Core models and utilities for handling TV show data.
* `bingefriend-shows-client_tvmaze`: API client for fetching data from TVMaze.

### Azure Functions
* `azure-functions`
* `azure-durable-functions`: Orchestration functions for managing the ingestion/update process.

### Database
* `sqlalchemy`: ORM and database interactions.
* `pymssql`: Azure SQL database driver.
* `alembic`: Database schema migrations.
* `python-dotenv`: Load `AZURE_SQL_CONNECTION_STRING` from `.env.alembic` for Alembic migrations.

### Testing
* `pytest`: Unit testing.
* `python-dotenv`: Load mock `AZURE_SQL_CONNECTION_STRING` from `.env.test` for testing.

## Local Development

* [Azure Functions Core Tools](https://learn.microsoft.com/en-us/azure/azure-functions/functions-run-local) for local function execution.
* [Azurite](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=visual-studio%2Cblob-storage) for local Azure Storage emulation.
* [Azure Storage Explorer](https://learn.microsoft.com/en-us/azure/storage/common/storage-explorer-install) for managing Azure Storage resources.
* `docker-compose.yml` included to run a local Azure SQL database.

## License

This project is licensed under the MIT License. See the (`LICENSE`)[LICENSE] file for details.

## Attribution

This client uses the TV Maze API but is not endorsed or certified by TV Maze. Data provided by [TVmaze.com](https://www.tvmaze.com/).