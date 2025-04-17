from dataclasses import dataclass, field
from typing import List, Tuple

@dataclass
class Document():
    url: str
    title: str
    content: str
    lead_content: str
    recipe_label: str
    tags: List[str]
    entities: List[Tuple[str,str]] = field(default_factory=list)
    
@dataclass()
class LinkVector:
    doc1: Document
    doc2: Document
    cosinus: float
    jaccard: float