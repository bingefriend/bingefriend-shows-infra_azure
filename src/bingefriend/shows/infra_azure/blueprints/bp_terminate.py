"""Terminate all running orchestrators in Azure Durable Functions."""

import logging
# noinspection PyPackageRequirements
import azure.functions as func
# noinspection PyPackageRequirements
import azure.durable_functions as df
from typing import List

bp_admin = df.Blueprint()


# noinspection PyUnusedLocal
@bp_admin.route(route="admin/terminate-all-running", auth_level="admin", methods=["POST"])
@bp_admin.durable_client_input(client_name="client")
async def terminate_all_running_orchestrators(
        req: func.HttpRequest, client: df.DurableOrchestrationClient
) -> func.HttpResponse:
    """
    Terminates all currently 'Running' or 'Pending' orchestration instances.
    IMPORTANT: Secure this endpoint appropriately (e.g., auth_level="admin" or Azure AD).
    """
    logging.info("Attempting to terminate all running/pending orchestrators.")

    try:
        # Define the statuses you want to target for termination
        statuses_to_terminate: List[df.OrchestrationRuntimeStatus] = [
            df.OrchestrationRuntimeStatus.Running,
            df.OrchestrationRuntimeStatus.Pending,
            df.OrchestrationRuntimeStatus.ContinuedAsNew
        ]

        # Get instances with the specified runtime statuses
        # You might want to add other filters like created_time_from/to if needed.
        instances = await client.get_status_by(runtime_status=statuses_to_terminate)

        terminated_ids = []
        failed_to_terminate_ids = []

        if not instances:
            logging.info("No running or pending orchestrators found to terminate.")
            return func.HttpResponse("No running or pending orchestrators found.", status_code=200)

        logging.info(f"Found {len(instances)} orchestrators to terminate.")

        for instance in instances:
            try:
                await client.terminate(instance.instance_id, "Terminated by admin 'terminate-all' action.")
                terminated_ids.append(instance.instance_id)
                logging.info(f"Successfully sent termination request for instance_id: {instance.instance_id}")
            except Exception as e:
                logging.error(f"Failed to send termination request for instance_id: {instance.instance_id}. Error: {e}")
                failed_to_terminate_ids.append({"id": instance.instance_id, "error": str(e)})

        response_message = {
            "message": "Termination process initiated.",
            "terminated_successfully_requested": terminated_ids,
            "failed_to_request_termination": failed_to_terminate_ids
        }
        return func.HttpResponse(
            body=str(response_message),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.exception("Error during the 'terminate all' process.")
        return func.HttpResponse(f"An unexpected error occurred: {e}", status_code=500)
