import json
import logging
from dataclasses_custom import Document, Blacklist, EntTypeDictionary, Matches, Entity
from spacy.tokens import Span
from spacy import Language
from collections import Counter
import tomllib

logging.basicConfig(level=logging.INFO)

def toml_to_config(conf_content: str) -> tuple[Matches, Blacklist, EntTypeDictionary]:
    config = tomllib.loads(conf_content)
    blacklist = config['blacklists']
    matches = config['matches']
    dictionary = config['dictionary']
    return (
        Matches(matching=matches['matching'], non_matching=matches['non_matching']),
        Blacklist(ent_types=blacklist['ent_types'], ent_names=blacklist['ent_names']),
        dictionary
    )
        
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

def json_with_ner_to_dict(content: str, blacklist: Blacklist) -> list[Document]:
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
        entities=_extract_ents_from_dict(text['nerObjectCollection']['values'], blacklist)
        ))
    return texts
        
def _extract_ents_from_dict(ents: list[dict[str,dict[str,int]|str]],
                            blacklist: Blacklist,
                            ) -> dict[tuple[str,str],int]:
    final_list = dict()
    for ent in ents:
        entity = ent['name'].lower().strip()
        category = ent['category'].lower()
        if entity not in blacklist.ent_names and category not in blacklist.ent_types:
            final_list[Entity(name=entity,type_=category)] = len(ent['locations'])
    return final_list
        
def get_ners(doc: Document, nlp: Language, dictionary: EntTypeDictionary, blacklist: Blacklist) -> dict[tuple[str,str],int]:
    logging.info('Extracting entities')
    lead_content_ents = nlp(doc.lead_content).ents if doc.lead_content != '' else []
    content_ents = nlp(doc.content).ents

    return dict(Counter(_list_and_filter_entities(lead_content_ents, dictionary,blacklist) +
                        _list_and_filter_entities(content_ents,dictionary,blacklist)))

def _list_and_filter_entities(ents: list[Span], dictionary: EntTypeDictionary, blacklist: Blacklist) -> list[tuple[str,str]]:
    out: list[tuple[str,str]] = []
    for ent in ents:
        entity = ent.lemma_.lower().strip()
        if len(entity) > 2 and ent.label_ not in blacklist.ent_types and entity not in blacklist.ent_names:
            out.append(Entity(name=entity, type_=dictionary[ent.label_]))
    return out