from neo4j import Result, Driver
import logging
from os import getenv
from dataclasses_custom import Document, LinkVector
from typing import List, Tuple, Dict, Tuple

logging.basicConfig(level=logging.INFO)

EDGE_STRING = """MATCH (a:Article {url: $url, entry_number: $entry_number, filename: $filename})
MATCH (e:Entity {entity: $entity, type: $type, filename: $filename})
MERGE (e)-[r:USED_IN]-(a)
ON CREATE SET r.count = 1
ON MATCH SET r.count = r.count + 1"""

SIMILARITY_EDGE_STRING = """MATCH (a:Article {url: $url1, entry_number: $entry_number1, filename: $filename})
MATCH (b:Article {url: $url2, entry_number: $entry_number2, filename: $filename})
MERGE (b)-[r:SIMILARITY {cosinus: $cosinus, jaccard: $jaccard}]-(a)
"""

class Neo4jExecutor:
    
    driver: Driver
    
    def __init__(self, driver: Driver):
        self.driver = driver 
        try:
            self.driver.verify_connectivity()
        except Exception:
            logging.error("connection error")

    def get_files(self) -> List[str]:
        with self.driver.session() as session:
            def list_json_files(tx) -> Result:
                return tx.run(
                'MATCH (a:Article) RETURN COLLECT(DISTINCT a.filename) AS file'
                )
            
            return [record.value('file') for record in list_json_files(session)][0]
        
    def get_all_ners(self, json_names: List[str]) -> List[str]:
        with self.driver.session() as session:
            def list_entities(tx) -> Result:
                return tx.run(
                '''MATCH (e:Entity)
                WHERE e.filename in $json_names
                RETURN e.entity as entity
                ''',
                json_names=json_names)
            
            return [record.value('entity') for record in list_entities(session)]


    def load_data(self, docs: List[Document], similarity_edges: List[LinkVector], filename: str):
        logging.info("Loading to database") 
        with self.driver.session() as session:
            def create_article(tx, doc: Document):
                return tx.run(
                    "Merge (:Article {url: $url, title: $title, content: $content, lead_content: $lead_content, recipe_label: $recipe_label, tags: $tags, entry_number: $entry_number, filename: $filename})",
                    url = doc.url,
                    title=doc.title,
                    content=doc.content,
                    lead_content=doc.lead_content,
                    recipe_label=doc.recipe_label,
                    tags=doc.tags,
                    entry_number=doc.entry_number,
                    filename=filename
                )
            def create_entity(tx, ent: Tuple[str,str]):
                return tx.run(
                    "Merge (:Entity {entity: $entity, type: $type, filename: $filename})",
                    entity=ent[0],
                    type=ent[1],
                    filename=filename
                )
                
            def create_edge(tx, url: str, entity: Tuple[str,str], entry_number: int):
                return tx.run(
                    EDGE_STRING,
                    url=url,
                    entity=entity[0],
                    entry_number=entry_number,
                    type=entity[1],
                    filename=filename
                )
                
            def create_similarity_links(tx, url1: str, url2: str, entry_number1: str, entry_number2: str, filename: str, cosinus: float, jaccard: float):
                return tx.run(
                    SIMILARITY_EDGE_STRING,
                    url1=url1,
                    url2=url2,
                    entry_number1=entry_number1,
                    entry_number2=entry_number2,
                    filename=filename,
                    cosinus=cosinus,
                    jaccard=jaccard
                )
            
            for doc in docs:
                session.execute_write(create_article,doc)
                for ent in doc.entities:
                    session.execute_write(create_entity,ent)
                    session.execute_write(create_edge,doc.url,ent,doc.entry_number)
            
            for linkvector in similarity_edges:
                session.execute_write(create_similarity_links,
                                      linkvector.doc1.url,
                                      linkvector.doc2.url,
                                      linkvector.doc1.entry_number,
                                      linkvector.doc2.entry_number,
                                      filename,
                                      linkvector.cosinus,
                                      linkvector.jaccard)

                    
                
    def delete_json(self, json_name: str):
        with self.driver.session() as session:
            def delete_articles(tx):
                return tx.run(
                    "MATCH (a:Article {filename: $filename}) DETACH DELETE a",
                    filename = json_name
                )
            
            def delete_entities(tx):
                return tx.run(
                    "MATCH (e:Entity {filename: $filename}) DETACH DELETE e",
                    filename = json_name
                )
            
            session.execute_write(delete_articles)
            session.execute_write(delete_entities)
            
    def get_linked_ners(self, entity: str, files: List[str]):
        with self.driver.session() as session:
            def list_json_files(tx) -> Result:
                return tx.run(
                '''MATCH (e:Entity {entity: $entity})--(a:Article)
WHERE (e.filename IN $files) 
WITH a, e
MATCH (a)-[r:USED_IN]-(e1:Entity)
WHERE e.entity <> e1.entity
RETURN e1, r
''',
                entity=entity,
                files=files
                )
            return_dict: Dict[Tuple[str,str], int]= dict()
            
            for record in list_json_files(session):
                key=(record[0]['entity'],record[0]['type'])
                return_dict[key] = return_dict.get(key,0) + record[1]['count']
            return return_dict
        
    def update_with_communities(self, communities: List[Dict[str,str]]):
        records, summary, keys  = self.driver.execute_query(
            """UNWIND $communities as data
               MATCH (a: Article)
               WHERE id(a) = toInteger(data.nodeId)
               SET a.communityId = data.communityId""",
            database_="neo4j",
            communities=communities
        )
        logging.info(f"Updating copmmunity nodes status: {summary.counters}")
    