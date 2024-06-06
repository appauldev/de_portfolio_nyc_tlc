from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable

from dagster import AssetCheckSpec
from pandas import DataFrame


@dataclass
class CheckSpec(ABC):
    AssetCheckSpec: AssetCheckSpec
    condition: Callable[[any], bool]
