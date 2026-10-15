import subprocess
import sys
from pathlib import Path

from tests.unit.helpers import invoke_dbt


def test_installed_wheel_runs_dbt(cli_project, tmp_path):
    repository = Path(__file__).resolve().parents[2]
    wheels = tmp_path / "wheels"
    venv = tmp_path / "venv"
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
        ["uv", "venv", "--python", sys.executable, str(venv)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    python = venv / ("Scripts/python.exe" if sys.platform == "win32" else "bin/python")
    result = subprocess.run(
        ["uv", "pip", "install", "--python", str(python), str(wheel)],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    result = subprocess.run(
        [
            str(python),
            "-c",
            "import dbt.adapters.motherduck as adapter; import dbt.include.motherduck as bundled; "
            "print(adapter.__file__); print(bundled.__file__)",
        ],
        cwd=cli_project,
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    assert all(Path(path).is_relative_to(venv) for path in result.stdout.splitlines())
    (cli_project / "models" / "model.sql").write_text(
        "select value: {{ dbt.dateadd('day', 1, \"date '2025-01-01'\") }}"
    )
    code, output = invoke_dbt(
        cli_project,
        "compile",
        "--no-populate-cache",
        "--no-introspect",
        python=str(python),
    )
    assert code == 0, output
    code, output = invoke_dbt(cli_project, "--version", python=str(python))
    assert code == 0, output
    assert "motherduck:" in output
