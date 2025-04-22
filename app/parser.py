import json
import logging
from dataclasses_custom import Document
from spacy.tokens import Span
from spacy import Language
from collections import Counter

logging.basicConfig(level=logging.INFO)

ENTITY_TYPE_DICT = {
    'orgName' : 'organization',
    'persName' : 'person',
    'geogName' : 'geogname',
    'placeName' : 'location'
}

BLACK_LIST = {
    'm.in.',
    'm.in.,'
}

def json_to_dict(content: str) -> list[Document]:
    data = json.loads(content)
    texts: list[Document] = []

    logging.info('Processing texts')
    for text in data:
        result_data = text["resultData"]

        url = result_data["url"]
        recipe_label = result_data["recipeLabel"]
        for text_data in result_data["results"]:
            texts.append(Document(
                url = url,
                title = text_data["title"],
                content=text_data["content"],
                lead_content=text_data['leadContent'],
                tags=[tag_row['tag'] for tag_row in text_data["tags"]],
                recipe_label=recipe_label,
            ))
    return texts

def json_with_ner_to_dict(content: str) -> list[Document]:
    data = json.loads(content)
    texts: list[Document] = []
    
    logging.info('Processing text with NER')
    for text in data:
        texts.append(Document(
        url = text['url'],
        recipe_label = text['objectType'],
        title = text['title'],
        tags=text['sourceTags'] if text['sourceTags'] is not None else [],
        content=text['content'],
        lead_content='',
        entities=_extract_ents_from_dict(text['nerObjectCollection']['values'])
        ))
    return texts
        
def _extract_ents_from_dict(ents: list[dict[str,dict[str,int]|str]]) -> dict[tuple[str,str],int]:
    final_list = dict()
    for ent in ents:
        entity = ent['name'].lower().strip()
        if entity not in BLACK_LIST:
            final_list[(entity,ent['category'].lower())] = len(ent['locations'])
    return final_list
        
def get_ners(doc: Document, nlp: Language) -> dict[tuple[str,str],int]:
    logging.info('Extracting entities')
    lead_content_ents = nlp(doc.lead_content).ents if doc.lead_content != '' else []
    content_ents = nlp(doc.content).ents

    return dict(Counter(_list_and_filter_entities(lead_content_ents) + _list_and_filter_entities(content_ents)))

def _list_and_filter_entities(ents: list[Span]) -> list[tuple[str,str]]:
    out: list[tuple[str,str]] = []
    for ent in ents:
        entity = ent.lemma_.lower().strip()
        if len(entity) > 2 and ent.label_ not in ['date','time'] and entity not in BLACK_LIST:
            out.append((entity, ENTITY_TYPE_DICT[ent.label_]))
    return out