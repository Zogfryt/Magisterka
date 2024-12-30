import json
from typing import List, Tuple
import logging
from dataclasses_custom import Document
from spacy.tokens import Span
from spacy import Language

logging.basicConfig(level=logging.INFO)

def json_to_dict(content: str) -> List[Document]:
    data = json.loads(content)
    texts: List[Document] = []

    logging.info('Processing texts')
    for text in data:
        result_data = text["resultData"]

        url = result_data["url"]
        recipe_label = result_data["recipeLabel"]
        for index, text_data in enumerate(result_data["results"]):
            texts.append(Document(
                url = url,
                title = text_data["title"],
                content=text_data["content"],
                lead_content=text_data['leadContent'],
                tags=[tag_row['tag'] for tag_row in text_data["tags"]],
                recipe_label=recipe_label,
                entry_number=index
            ))
    return texts


def get_ners(doc: Document, nlp: Language) -> List[Tuple[str,str]]:
    logging.info('Extracting entities')
    lead_content_ents = nlp(doc.lead_content).ents
    content_ents = nlp(doc.content).ents

    return _list_and_filter_entities(lead_content_ents) + _list_and_filter_entities(content_ents)

def _list_and_filter_entities(ents: List[Span]) -> List[Tuple[str,str]]:
    out: List[Tuple[str,str]] = []
    for ent in ents:
        if len(ent.lemma_) > 2 and ent.label_ not in ['date','time']:
            out.append((ent.lemma_, ent.label_))
    return out