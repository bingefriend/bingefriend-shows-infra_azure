"""Binge Friends Shows Function App."""

# noinspection PyPackageRequirements
import azure.functions as func
from src.bingefriend.shows.infra_azure.blueprints.bp_ingest import bp as ingest_bp
from src.bingefriend.shows.infra_azure.blueprints.bp_update import bp as update_bp

app = func.FunctionApp()

app.register_blueprint(ingest_bp)
app.register_blueprint(update_bp)
