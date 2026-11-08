#!/usr/bin/env python3
"""Generate and verify C#/Go contracts using the pinned Docker toolchain."""

import argparse
from pathlib import Path
import shutil
import subprocess
import sys


def verify(repo_root: Path, update_generated: bool = False) -> None:
    command = ["docker", "build"]
    for line in (repo_root / "contracts/toolchain.env").read_text(
        encoding="utf-8-sig"
    ).splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            command.extend(["--build-arg", line])
    command.extend(["--build-arg", f"UPDATE_LOCKS={str(update_generated).lower()}"])
    command.extend([
        "--file", "contracts/Dockerfile.verify",
        "--target", "export",
        "--output", "type=local,dest=.artifacts/contracts", ".",
    ])
    subprocess.run(command, cwd=repo_root, check=True)

    artifacts = repo_root / ".artifacts/contracts"
    copies = [
        (artifacts / "go/go.mod", repo_root / "contracts/go/go.mod"),
        (artifacts / "go/go.sum", repo_root / "contracts/go/go.sum"),
        (artifacts / "csharp/packages.lock.json", repo_root / "contracts/csharp/packages.lock.json"),
        (artifacts / "checks/packages.lock.json", repo_root / "tests/contracts/csharp/packages.lock.json"),
        (artifacts / "checks/compat_test.go", repo_root / "tests/contracts/go/compat_test.go"),
    ]
    generated_root = artifacts / "go/gen"
    if not generated_root.is_dir():
        raise RuntimeError(f"Missing generated directory: {generated_root}")
    copies.extend(
        (source, repo_root / "contracts/go/gen" / source.relative_to(generated_root))
        for source in sorted(generated_root.rglob("*")) if source.is_file()
    )
    for source, target in copies:
        if update_generated:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
        else:
            if not target.is_file():
                raise RuntimeError(f"Missing generated/locked file: {target}")
            # Text mode normalizes CRLF so Windows checkouts compare with Linux exports.
            if source.read_text(encoding="utf-8-sig") != target.read_text(encoding="utf-8-sig"):
                raise RuntimeError(
                    f"Generated/locked file differs: {target}. "
                    "Run with --update-generated and review the diff."
                )
    print("PASS: C#/Go compatibility, generation, formatting and dependency locks.")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--update-generated", action="store_true",
        help="update generated files and dependency locks; review the resulting diff",
    )
    args = parser.parse_args()
    try:
        verify(Path(__file__).resolve().parent.parent, args.update_generated)
    except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
