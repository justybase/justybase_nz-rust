"""Copy local library sources into benchmark artifacts, keeping its checkout untouched."""
import shutil
import sys
from pathlib import Path
source, destination = map(Path, sys.argv[1:3])
project = source / "src/JustyBase.NetezzaDriver"
target = destination / "src/JustyBase.NetezzaDriver"
for file in project.rglob("*"):
    relative = file.relative_to(project)
    if file.is_file() and not {"obj", "bin"}.intersection(relative.parts) and file.suffix in {".cs", ".csproj", ".props", ".targets", ".resx"}:
        output = target / relative
        output.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(file, output)
for name in ["README.md", "LICENSE"]:
    if (source / name).exists():
        shutil.copyfile(source / name, destination / name)
print(target / "JustyBase.NetezzaDriver.csproj")
