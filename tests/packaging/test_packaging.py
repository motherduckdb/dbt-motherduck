import os
import subprocess
import sys
from pathlib import Path

from tests.unit.helpers import invoke_dbt


def test_installed_wheel_runs_dbt(cli_project, tmp_path):
    repository = Path(__file__).resolve().parents[2]
    wheels = tmp_path / "wheels"
    installed = tmp_path / "installed"
    result = subprocess.run(
        [
            "uv",
            "build",
            "--offline",
            "--wheel",
            "--out-dir",
            str(wheels),
            str(repository),
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    (wheel,) = wheels.glob("*.whl")
    result = subprocess.run(
        ["uv", "pip", "install", "--no-deps", "--target", str(installed), str(wheel)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    env = {"PYTHONPATH": str(installed)}
    # Dependencies come from the test environment, but the adapter and its
    # bundled macros must come from the built wheel, not the editable checkout.
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import dbt.adapters.motherduck as adapter; import dbt.include.motherduck as bundled; "
            "print(adapter.__file__); print(bundled.__file__)",
        ],
        cwd=cli_project,
        capture_output=True,
        text=True,
        check=True,
        env={**os.environ, **env},
        timeout=30,
    )
    assert all(
        Path(path).is_relative_to(installed) for path in result.stdout.splitlines()
    )
    (cli_project / "models" / "model.sql").write_text(
        "select value: {{ dbt.dateadd('day', 1, \"date '2025-01-01'\") }}"
    )
    code, output = invoke_dbt(
        cli_project, "compile", "--no-populate-cache", "--no-introspect", env=env
    )
    assert code == 0, output
    code, output = invoke_dbt(cli_project, "--version", env=env)
    assert code == 0, output
    assert "motherduck:" in output
