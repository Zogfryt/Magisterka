from dataclasses import dataclass, field
from collections import Counter
from itertools import combinations

@dataclass
class Document():
    url: str
    title: str
    content: str
    lead_content: str
    recipe_label: str
    tags: list[str]
    entities: dict[tuple[str,str],int] = field(default_factory=dict)

    def neo4j_json_list(self) -> list[dict[str,str|int]]:
        return [self._format_json(ent,count) for ent, count in self.entities.items()]
        
    def _format_json(self, ent: tuple[str,str], count: int) -> dict[str,str]:
        return {
            "url": self.url,
            "title": self.title,
            "content": self.content,
            "lead_content": self.lead_content,
            "recipe_label": self.recipe_label,
            "tags": self.tags,
            "ent_name": ent[0],
            "ent_type": ent[1],
            "count": count
        }
    
    def return_tuple_connections(self) -> Counter:
        return Counter({frozenset({tup1,tup2}):(self.entities[tup1]+self.entities[tup2])/2 for tup1, tup2 in combinations(self.entities.keys(),2)})
    
@dataclass()
class LinkVector:
    doc1: Document
    doc2: Document
    cosinus: float
    jaccard: float