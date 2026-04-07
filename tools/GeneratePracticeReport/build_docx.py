# -*- coding: utf-8 -*-
"""Создаёт отчет.docx в папке отчет@practic24022026/интерфейс (относительно корня веб-проекта)."""
from pathlib import Path
import subprocess
import sys

here = Path(__file__).resolve().parent
out = (here.parent.parent / "отчет@practic24022026" / "интерфейс").resolve()
out.mkdir(parents=True, exist_ok=True)
proj = here / "GeneratePracticeReport.csproj"
r = subprocess.run(
    ["dotnet", "run", "--project", str(proj), "--", str(out)],
    cwd=str(here),
)
sys.exit(r.returncode)
