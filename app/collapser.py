from dataclasses_custom import Document, LinkVector, Entity
from collections import Counter
from itertools import chain, product, combinations
import numpy as np
from tqdm import tqdm
import logging

def __ner_vector(counts: Counter, map: dict[Entity,int]) -> np.ndarray:
    shape = len(map)
    vec = np.zeros(shape,dtype=float)
    
    for key, value in counts.items():
        vec[map[key]] = value
        
    return vec / (np.sqrt(np.sum(vec**2)) + 1e-10) 

def __calculate_cosine(count1: Counter, count2: Counter) -> float:
    index = dict()
    
    for idx, key in enumerate(set(chain(count1.keys(),count2.keys()))):
            index[key] = idx
            
    vec1 = __ner_vector(count1, index)
    vec2 = __ner_vector(count2, index)
    return vec1 @ vec2

def __calculate_jaccard(count1: Counter, count2: Counter) -> float:
    top, bottom = 0, 0
    bottom = sum(count2.values())
    for key in count1:
        top += (count1[key] + count2[key]) * (key in count2)
        bottom += count1[key]
        
    return top / (bottom + 1e5)


def calculate_distances(ents1: dict[Entity,int], ents2: dict[Entity,int]) -> tuple[float,float]:
    counter_ents1 = Counter(ents1)
    counter_ents2 = Counter(ents2)
            
 
    return __calculate_jaccard(counter_ents1, counter_ents2), \
    __calculate_cosine(counter_ents1, counter_ents2)
    
def create_similarity_links(documents: list[Document]) -> list[LinkVector]:
    logging.info("Calculating Distances")
    vectors = []
    for doc1, doc2 in tqdm(combinations(documents,2)):
        jacc, cos = calculate_distances(doc1.entities,doc2.entities)
        if jacc * cos > 0:
            vectors.append(
                LinkVector(
                    url1=doc1.url,
                    url2=doc2.url,
                    cosinus=cos,
                    jaccard=jacc
                )
            )
    return vectors

def create_similarity_links_between_files(documents: list[Document], other_docs: dict[str,dict[Entity,int]]) -> list[LinkVector]:
    vectors = []
    for doc, url in product(documents, other_docs.keys()):
        jacc, cos = calculate_distances(doc.entities, other_docs[url])
        if jacc * cos > 0:
            vectors.append(
                LinkVector(
                    url1=doc.url,
                    url2=url,
                    cosinus=cos,
                    jaccard=jacc
                )
            )
    return vectors
            
    

    