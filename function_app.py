"""Binge Friends Shows Function App."""

import os
import sys

current_dir = os.path.dirname(__file__)
parent_dir = os.path.abspath(os.path.join(current_dir, '..'))

core_src_path = os.path.join(parent_dir, 'bingefriend-shows-core', 'src')
sys.path.insert(0, core_src_path)

application_path = os.path.join(parent_dir, 'bingefriend-shows-application', 'src')
sys.path.insert(0, application_path)

client_tvmaze_path = os.path.join(parent_dir, 'bingefriend-shows-client_tvmaze', 'src')
sys.path.insert(0, client_tvmaze_path)

# noinspection PyPackageRequirements
import azure.functions as func
from src.bingefriend.shows.infra_azure.blueprints.bp_ingest import bp as ingest_bp
# from src.binge_friend.shows.blueprints.bp_update import bp as update_bp

app = func.FunctionApp()

app.register_blueprint(ingest_bp)
# app.register_blueprint(update_bp)
