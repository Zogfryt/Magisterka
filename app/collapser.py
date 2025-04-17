from dataclasses_custom import Document, LinkVector
from typing import List, Tuple, Dict
from collections import Counter
from itertools import chain, product
import numpy as np
from tqdm import tqdm
import logging

def __ner_vector(counts: Counter, map: Dict[Tuple[str,str],int]) -> np.ndarray:
    shape = len(map)
    vec = np.zeros(shape,dtype=float)
    
    for key, value in counts.items():
        vec[map[key]] = value
        
    return vec / (np.sqrt(np.sum(vec**2)) + 1e-10) 

def __calculate_cosine(count1: Counter, count2: Counter) -> float:
    index = dict()
    increment = 0
    
    for key in chain(count1.keys(),count2.keys()):
        if key not in index:
            index[key] = increment
            increment += 1
            
    vec1 = __ner_vector(count1, index)
    vec2 = __ner_vector(count2, index)
    return vec1 @ vec2

def __calculate_jaccard(count1: Counter, count2: Counter) -> float:
    top, bottom = 0, 0
    bottom = sum(count2.values())
    for key in count1:
        top += (count1[key] + count2[key]) * (key in count2)
        bottom += count1[key]
        
    return top / bottom


def calculate_distances(ents1: List[Tuple[str,str]], ents2: List[Tuple[str,str]]) -> Tuple[float,float]:
    counter_ents1 = Counter(ents1)
    counter_ents2 = Counter(ents2)
            
 
    return __calculate_jaccard(counter_ents1, counter_ents2), \
    __calculate_cosine(counter_ents1, counter_ents2)
    
def create_similarity_links(documents: List[Document]) -> List[LinkVector]:
    logging.info("Calculating Distances")
    vectors = []
    for doc1, doc2 in tqdm(product(documents,documents)):
        if doc1.url != doc2.url:
            jacc, cos = calculate_distances(doc1.entities,doc2.entities)
            if jacc * cos > 0:
                vectors.append(
                    LinkVector(
                        doc1=doc1,
                        doc2=doc2,
                        cosinus=cos,
                        jaccard=jacc
                    )
                )
    return vectors
            
    

    