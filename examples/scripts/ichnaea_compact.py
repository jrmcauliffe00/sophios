"""Canonical end-to-end example: cwl_builder -> Workflow -> compute payload."""

from argparse import ArgumentParser
import copy
from datetime import datetime
import json
from pathlib import Path
from typing import Dict, cast

from sophios.apis.python.api import Workflow
from sophios.apis.python.airflow_bridge import (
    get_airflow_dag,
    submit_airflow_dag_via_api,
    write_airflow_dag_file,
)
from sophios.wic_types import Json

from sophios.apis.python.cwl_builder import CommandLineTool, Input, Inputs, Output, Outputs, cwl
from sophios.compute_payload import ComputeWorkflowPayload, ComputeConfig, ToilConfig, OutputConfig, SlurmConfig
from sophios.compute_submit import submit_compute_payload


def build_autoseg_CLT() -> CommandLineTool:
    """Create the SAM3 autosegmentation CLT using the declarative API."""
    inputs = Inputs(
        input=Input(cwl.directory, position=1).label(
            "Input Zarr dataset").doc("Path to input zarr dataset"),
        output=Input(cwl.directory, position=2).label("Output segmentation Zarr").doc(
            "Path for output segmentation zarr"
        ),
        model=Input(cwl.file, flag="--model", required=False)
        .label("Model override file")
        .doc("Path containing sam3.pt to override baked-in models/sam3"),
        tile_size=Input(cwl.int, flag="--tile-size", required=False)
        .label("Tile size")
        .doc("Tile size for large slices (default 1024)"),
        overlap=Input(cwl.int, flag="--overlap", required=False)
        .label("Tile overlap")
        .doc("Overlap between adjacent tiles in pixels (default 128)"),
        iou_threshold=Input(cwl.float, flag="--iou-threshold", required=False)
        .label("IoU threshold")
        .doc("IoU threshold for matching labels across tiles (default 0.5)"),
        batch_size=Input(cwl.int, flag="--batch-size", required=False)
        .label("Batch size")
        .doc("Number of tiles per GPU forward pass (default 8)"),
        lora_weights=Input(cwl.file, flag="--lora-weights", required=False)
        .label("LoRA weights")
        .doc("Path to LoRA adapter weights (.pt file) - optional"),
        lora_rank=Input(cwl.int, flag="--lora-rank", required=False)
        .label("LoRA rank")
        .doc("LoRA rank used when lora_weights is set (default 16)"),
        lora_alpha=Input(cwl.int, flag="--lora-alpha", required=False)
        .label("LoRA alpha")
        .doc("LoRA alpha scaling factor used when lora_weights is set (default 32)"),
    )
    outputs = Outputs(output=Output(
        cwl.directory, from_input=inputs.output).label("Output segmentation Zarr"))

    return (
        CommandLineTool("sam3_ome_zarr_autosegmentation", inputs, outputs)
        .describe(
            "SAM3 OME Zarr autosegmentation",
            "Run SAM3 autosegmentation on a zarr volume.\n",
        )
        .edam()
        .gpu(cuda_version_min="11.7", compute_capability="3.0", device_count_min=2)
        .docker("polusai/ichnaea-api:latest")
        .stage(inputs.output, writable=True)
        .stage(inputs.input)
        .resources(cores=4, ram=64000)
        .base_command(
            "/backend/.venv/bin/python",
            "/backend/dagster_pipelines/jobs/autosegmentation/logic.py",
        )
    )


def workflow(input_dicts: Dict[str, str], workflow_name: str) -> Workflow:
    """Build the test workflow and return Workflow object"""
    # =========== BUILD CLT ==========================
    autoseg_clt = build_autoseg_CLT()  # directly building the CLT in memory
    # =========== CREATE A STEP ======================
    # converting the built CLT into a workflow step
    autoseg = autoseg_clt.to_step(step_name='autoseg')
    # assign input values to the step
    autoseg.output = input_dicts['output_dir']
    autoseg.input = input_dicts['input_dir']
    autoseg.model = input_dicts['model_file']
    # =========== CREATE A SOPHIOS WORKFLOW ==========
    # arrange steps
    steps = [autoseg]
    # create workflow from steps DAG
    wkflw = Workflow(steps, workflow_name)
    # =========== RETURN SOPHIOS WORKFLOW OBJECT =====
    return wkflw


def create_compute_payload(workflow_id: str, cwl_workflow: Json, cwl_job_inputs: Json) -> ComputeWorkflowPayload:
    """Returns a compute payload object"""
    # =========== BUILD COMPUTE PAYLOAD OBJECT =======
    # Build the compute payload object here
    compute_object = ComputeWorkflowPayload(
        workflow_id=workflow_id,
        cwl_workflow=cwl_workflow,
        cwl_job_inputs=cwl_job_inputs,
        compute_config=ComputeConfig(
            toil=ToilConfig(log_level="INFO"),
            output=OutputConfig.workflow_declared(),
            slurm=SlurmConfig(partition="normal_gpu", cpus_per_task=4)
        )
    )
    # =========== RETURN COMPUTE PAYLOAD OBJECT ======
    return compute_object


def main() -> int:
    """Build the workflow, create the compute payload, and optionally submit it."""
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "--submit-url",
        help="Optional compute create-workflow endpoint. If omitted, only build the payload in memory.",
    )
    parser.add_argument(
        "--write-airflow-dag",
        action="store_true",
        help="Write a generated Airflow DAG Python module to disk.",
    )
    parser.add_argument(
        "--airflow-dags-dir",
        default="./dags",
        help="Directory where generated Airflow DAG files are written.",
    )
    parser.add_argument(
        "--airflow-dag-filename",
        help="Optional filename override for the generated Airflow DAG module.",
    )
    parser.add_argument(
        "--trigger-airflow-run",
        action="store_true",
        help="Trigger a DAG run via Airflow REST API (requires --airflow-api-base-url).",
    )
    parser.add_argument("--airflow-api-base-url", help="Airflow base URL, e.g. http://localhost:8080")
    parser.add_argument("--airflow-api-token", help="Bearer token for Airflow API auth.")
    parser.add_argument("--airflow-api-username", help="Username for basic-auth Airflow API auth.")
    parser.add_argument("--airflow-api-password", help="Password for basic-auth Airflow API auth.")
    parser.add_argument("--airflow-run-id", help="Optional DAG run id for Airflow trigger requests.")
    parser.add_argument("--airflow-conf-json", help="Optional JSON object to pass as DAG run conf.")
    parser.add_argument(
        "--airflow-insecure",
        action="store_true",
        help="Disable TLS verification for Airflow API calls (testing only).",
    )
    args = parser.parse_args()

    # ========== INPUTS TO WORKFLOW ==================
    # The main directory constants
    inputs_dir = Path('/projects/collabs/mock_common/')
    input_dicts = {}
    input_dicts['input_dir'] = str(inputs_dir / 'input_directory')
    input_dicts['output_dir'] = str(inputs_dir / 'output_directory_compact')
    input_dicts['model_file'] = str(inputs_dir / 'model_directory' / 'sam3.pt')

    # ========== BUILD WORKFLOW ======================
    autoseg_workflow = workflow(input_dicts, "autoseg_workflow")
    workflow_json = autoseg_workflow.get_cwl_workflow()
    airflow_dag = get_airflow_dag(autoseg_workflow, compiled_workflow=workflow_json)
    print(f"Built Airflow DAG in memory: dag_id={airflow_dag.dag_id}")
    if args.write_airflow_dag:
        dag_file_path = write_airflow_dag_file(
            autoseg_workflow,
            dags_dir=Path(args.airflow_dags_dir),
            dag_filename=args.airflow_dag_filename,
            dag_id=airflow_dag.dag_id,
            compiled_workflow=workflow_json,
        )
        print(f"Wrote Airflow DAG file: {dag_file_path.resolve()}")
    if args.trigger_airflow_run:
        if not args.airflow_api_base_url:
            raise ValueError("--airflow-api-base-url is required with --trigger-airflow-run")
        dag_conf: Json | None = None
        if args.airflow_conf_json:
            parsed_conf = json.loads(args.airflow_conf_json)
            if not isinstance(parsed_conf, dict):
                raise ValueError("--airflow-conf-json must be a JSON object")
            dag_conf = cast(Json, parsed_conf)
        trigger_response = submit_airflow_dag_via_api(
            autoseg_workflow,
            airflow_api_base_url=args.airflow_api_base_url,
            dag_id=airflow_dag.dag_id,
            run_id=args.airflow_run_id,
            conf=dag_conf,
            api_token=args.airflow_api_token,
            api_username=args.airflow_api_username,
            api_password=args.airflow_api_password,
            verify_ssl=not args.airflow_insecure,
            compiled_workflow=workflow_json,
        )
        print(f"Triggered Airflow DAG run: {trigger_response.get('dag_run_id', '<unknown>')}")

    # ========== COMPUTE INPUT =======================
    # workflow Name
    workflow_name = workflow_json['name']
    # adjust workflow name/id to distinguish after submit using a timestamp
    workflow_name = workflow_name + '__' + \
        datetime.now().strftime('%Y_%m_%d_%H.%M.%S') + '__'
    # workflow Inputs
    workflow_inputs = copy.deepcopy(workflow_json['yaml_inputs'])
    # workflow CWL
    workflow_json.pop('name')
    workflow_json.pop('yaml_inputs')
    compiled_cwl_workflow = copy.deepcopy(workflow_json)

    # ========== CONSTRUCT COMPUTE OBJECT ============
    compute_object = create_compute_payload(
        workflow_name, compiled_cwl_workflow, workflow_inputs)

    if args.submit_url is None:
        print("Built compute payload object in memory. Pass --submit-url to submit it.")
        return 0

    # =========  SUBMIT TO COMPUTE ===================
    submission_status: int = submit_compute_payload(
        compute_object, args.submit_url)
    return submission_status


if __name__ == '__main__':
    raise SystemExit(main())
