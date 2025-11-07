# domain package marker + shared types
from typing import TypedDict, Optional, List, Dict, Any
import pandas as pd

class Profile(TypedDict):
    journey: int
    attack: int
    midfield: int
    defence: int
    budget: float
    dragflick: int
    player_type: str
    priority: str
    preferred_bow: str
    length: float | None

Frame = pd.DataFrame
Row = pd.Series
