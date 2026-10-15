import os
import subprocess
import sys


def invoke_dbt(project, *args, env=None, python=sys.executable):
    project_args = (
        []
        if args == ("--version",)
        else ["--project-dir", str(project), "--profiles-dir", str(project)]
    )
    result = subprocess.run(
        [
            python,
            "-c",
            "from dbt.cli.main import cli; cli()",
            *args,
            *project_args,
        ],
        cwd=project,
        capture_output=True,
        text=True,
        timeout=30,
        env={**os.environ, "DBT_SEND_ANONYMOUS_USAGE_STATS": "false", **(env or {})},
    )
    return result.returncode, result.stdout + result.stderr
