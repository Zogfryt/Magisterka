from dataclasses import dataclass, field
from typing import List, Tuple

@dataclass
class Document():
    url: str
    entry_number: int
    title: str
    content: str
    lead_content: str
    recipe_label: str
    tags: List[str]
    entities: List[Tuple[str,str]] = field(default_factory=list)