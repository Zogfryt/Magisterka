from dataclasses import dataclass, field
from collections import Counter
from itertools import combinations
from pathlib import Path

CONFIGURATION_FOLDER = Path(__file__).absolute().parent / 'configurations' 

@dataclass
class Blacklist:
    ent_types: list[str]
    ent_names: list[str]

@dataclass
class Matches:
    matching: list[str]
    non_matching: list[str]

    def __str__(self) -> str:
        buffer = ['[matches]','matching = [']
        for match in self.matching:
            buffer.append(f'"{match}",')
        buffer.extend([']','','non_matching = ['])
        for n_match in self.non_matching:
            buffer.append(f'"{n_match}",')
        buffer.append(']')
        return '\n'.join(buffer)
    

type EntTypeDictionary = dict[str,str]

@dataclass(frozen=True)
class Entity:
    name: str
    type_ : str

@dataclass
class Document():
    url: str
    title: str
    content: str
    lead_content: str
    recipe_label: str
    tags: list[str]
    entities: dict[Entity,int] = field(default_factory=dict)

    def neo4j_json_list(self) -> list[dict[str,str|int]]:
        return [self._format_json(ent,count) for ent, count in self.entities.items()]
        
    def _format_json(self, ent: Entity, count: int) -> dict[str,str]:
        return {
            "url": self.url,
            "title": self.title,
            "content": self.content,
            "lead_content": self.lead_content,
            "recipe_label": self.recipe_label,
            "tags": self.tags,
            "ent_name": ent.name,
            "ent_type": ent.type_,
            "count": count
        }
    
    def return_tuple_connections(self) -> Counter[tuple[Entity,Entity]]:
        return Counter({tuple(sorted((tup1,tup2),key=lambda x: x.name)):(self.entities[tup1]+self.entities[tup2])/2 for tup1, tup2 in combinations(self.entities.keys(),2)})
    
@dataclass()
class LinkVector:
    url1: str
    url2: str
    cosinus: float
    jaccard: float