from dataclasses import dataclass
from typing import Callable

from dagster import AssetCheckSpec


@dataclass
class CheckSpec:
    acp: AssetCheckSpec
    condition: Callable[[any], bool]
