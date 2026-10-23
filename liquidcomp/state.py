"""Portable tar snapshots, without following files outside the source tree."""

import os
import stat
import tarfile
from io import BytesIO
from pathlib import Path
from typing import List, NoReturn, Optional

from pathspec import GitIgnoreSpec

from liquidcomp.errors import StateError


def _walk_error(error: OSError) -> NoReturn:
    raise error


def read_state(path: Path, exclude: Optional[List[str]], limit: int) -> bytes:
    spec = GitIgnoreSpec.from_lines(exclude or [])
    buffer = BytesIO()

    def portable(info: tarfile.TarInfo) -> tarfile.TarInfo:
        if info.issym():
            item = root / info.name
            target = item.resolve(strict=True)
            if os.path.commonpath((str(root), str(target))) != str(root):
                raise StateError('A state symlink points outside the source tree.')
            info.linkname = os.path.relpath(target, item.parent).replace(os.sep, '/')
        return info

    try:
        if not path.is_dir():
            raise StateError('The state root must be an existing directory.')
        root = path.resolve()
        with tarfile.open(fileobj=buffer, mode='w') as archive:
            for directory, dirs, files in os.walk(root, followlinks=False, onerror=_walk_error):
                for name in sorted(dirs + files):
                    item = Path(directory) / name
                    relative = item.relative_to(root).as_posix()
                    kind = item.lstat().st_mode
                    is_directory = stat.S_ISDIR(kind)
                    if not spec.match_file(relative + ('/' if is_directory else '')):
                        if not is_directory and not stat.S_ISREG(kind) and not stat.S_ISLNK(kind):
                            raise StateError('State contains an unsupported special file.')
                        if stat.S_ISREG(kind) and item.stat().st_size + buffer.tell() > limit:
                            raise StateError('Initial state exceeds the configured artifact limit.')
                        archive.add(item, arcname=relative, recursive=False, filter=portable)
                        if buffer.tell() > limit:
                            raise StateError('Initial state exceeds the configured artifact limit.')
                    elif name in dirs:
                        dirs.remove(name)
    except (OSError, RuntimeError, ValueError, tarfile.TarError):
        raise StateError('Could not read the initial state safely.') from None
    state = buffer.getvalue()
    if len(state) > limit:
        raise StateError('Initial state exceeds the configured artifact limit.')
    return state
