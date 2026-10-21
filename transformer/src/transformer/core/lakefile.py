import re
from pathlib import Path


def write(frontier: Path, content: str) -> tuple[Path, int]:
    """Adds version name.vN.ext and repoints the name.ext symlink to it."""
    frontier.parent.mkdir(parents=True, exist_ok=True)
    version = _next_version(frontier.parent, frontier.stem, frontier.suffix)
    versioned = frontier.with_name(
        f"{frontier.stem}.v{version}{frontier.suffix}"
    )
    versioned.write_text(content)

    # Relative target, so the link resolves in the container and on the host.
    tmp = frontier.with_name(frontier.name + ".tmp")
    tmp.symlink_to(versioned.name)
    tmp.replace(frontier)
    return versioned, version


def _next_version(directory: Path, stem: str, ext: str) -> int:
    pattern = re.compile(rf"{re.escape(stem)}\.v(\d+){re.escape(ext)}")
    versions = [
        int(m.group(1))
        for p in directory.iterdir()
        if (m := pattern.fullmatch(p.name))
    ]
    return max(versions, default=0) + 1
