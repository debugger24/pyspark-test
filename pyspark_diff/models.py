from typing import Any, Optional

from pydantic import BaseModel


class Difference(BaseModel):
    row_id: Optional[Any]
    column_name: Optional[str]
    column_name_parent: Optional[str]
    left: Any
    right: Any
    reason: Optional[str]
