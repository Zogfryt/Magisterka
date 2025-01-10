import json
from typing import List, Tuple, Dict
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

def json_with_ner_to_dict(content: str) -> List[Document]:
    data = json.loads(content)
    texts: List[Document] = []
    
    logging.info('Processing text with NER')
    for text in data:
        texts.append(Document(
        url = text['url'],
        recipe_label = text['objectType'],
        title = text['title'],
        entry_number=0,
        tags=text['sourceTags'] if text['sourceTags'] is not None else [],
        content=text['content'],
        lead_content='',
        entities=_extract_ents_from_dict(text['nerObjectCollection']['values'])
        ))
    return texts
        
def _extract_ents_from_dict(ents: List[Dict[str,Dict[str,int]|str]]) -> List[Tuple[str,str]]:
    final_list = []
    for ent in ents:
        final_list.extend([(ent['name'],ent['category'].lower())]*len(ent['locations']))
    return final_list
        
        


def get_ners(doc: Document, nlp: Language) -> List[Tuple[str,str]]:
    logging.info('Extracting entities')
    lead_content_ents = nlp(doc.lead_content).ents if doc.lead_content != '' else []
    content_ents = nlp(doc.content).ents

    return _list_and_filter_entities(lead_content_ents) + _list_and_filter_entities(content_ents)

def _list_and_filter_entities(ents: List[Span]) -> List[Tuple[str,str]]:
    out: List[Tuple[str,str]] = []
    for ent in ents:
        if len(ent.lemma_) > 2 and ent.label_ not in ['date','time']:
            out.append((ent.lemma_, ent.label_))
    return out