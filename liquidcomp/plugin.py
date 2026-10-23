"""Installing liquidcomp registers the liquid throng plugin automatically."""

from pathlib import Path
from typing import List, Optional, Union

from throng import throng

from liquidcomp.manager import LiquidManager


@throng.plugin('liquid', unique=True)
def liquid(path: Union[str, Path] = '.', exclude: Optional[List[str]] = None, prepare: Optional[List[str]] = None, packages: Optional[List[str]] = None) -> LiquidManager:
    return LiquidManager(path, exclude, prepare, packages)
