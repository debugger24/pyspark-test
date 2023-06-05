from typing import Any, Optional

from pydantic import BaseModel


class Difference(BaseModel):
    row_id: int
    column_name: str
    column_name_parent: str
    left: Any
    right: Any
    reason: Optional[str]
