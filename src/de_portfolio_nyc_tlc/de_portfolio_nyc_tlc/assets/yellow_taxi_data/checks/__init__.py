from dataclasses import dataclass
from typing import Callable

from dagster import AssetCheckSpec
from pandas import DataFrame


@dataclass
class CheckSpec:
    acp: AssetCheckSpec
    condition: Callable[[any], bool]
