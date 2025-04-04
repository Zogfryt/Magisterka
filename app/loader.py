from neo4j import Result, Driver
import logging
from os import getenv
from dataclasses_custom import Document, LinkVector
from typing import List, Tuple, Dict, Tuple
from itertools import chain

logging.basicConfig(level=logging.INFO)

EDGE_STRING = """UNWIND $edges as edge
MATCH (a:Article {url: edge.url, entry_number: edge.entry_number, filename: $filename})
MATCH (e:Entity {entity: edge.entity, type: edge.type, filename: $filename})
MERGE (e)-[r:USED_IN]-(a)
ON CREATE SET r.count = 1
ON MATCH SET r.count = r.count + 1"""

SIMILARITY_EDGE_STRING = """UNWIND $edges as edge
MATCH (a:Article {url: edge.url1, entry_number: edge.entry_number1, filename: $filename})
MATCH (b:Article {url: edge.url2, entry_number: edge.entry_number2, filename: $filename})
MERGE (b)-[r:SIMILARITY {cosinus: edge.cosinus, jaccard: edge.jaccard}]-(a)
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
                RETURN e.entity as entity, e.type as type
                ''',
                json_names=json_names)
            
            return [f"{record.value('entity')} ({record.value('type')})" for record in list_entities(session)]


    def load_data(self, docs: List[Document], similarity_edges: List[LinkVector], filename: str):
        logging.info("Loading to database") 
        docs_list = [{"url" : doc.url, 'title': doc.title, "content": doc.content, 'lead_content': doc.lead_content, "tags": doc.tags, 'entry_number': doc.entry_number, 'recipe_label': doc.recipe_label} for doc in docs]
        ents, edges = [], []
        for doc in docs:
            for ent in doc.entities:
                ents.append({"entity": ent[0], "type": ent[1]})
                edges.append({"url": doc.url, 'entity': ent[0], 'type': ent[1], 'entry_number': doc.entry_number})
        links = [{"url1": sim.doc1.url, "url2": sim.doc2.url, 'entry_number1': sim.doc1.entry_number, "entry_number2": sim.doc2.entry_number, "cosinus": sim.cosinus, 'jaccard': sim.jaccard} for sim in similarity_edges]

            
        _, summary, _  = self.driver.execute_query(
            """UNWIND $documents AS docs
            Merge (:Article {url: docs.url, title: docs.title, content: docs.content, lead_content: docs.lead_content, recipe_label: docs.recipe_label, tags: docs.tags, entry_number: docs.entry_number, filename: $filename})""",
            database_="neo4j",
            documents=docs_list,
            filename=filename
        )
        logging.info(f"Uploading documents summary: {summary.counters}")

        _, summary, _ = self.driver.execute_query(
            """UNWIND $ents as ent
            Merge (:Entity {entity: ent.entity, type: ent.type, filename: $filename})""",
            database_="neo4j",
            ents=ents,
            filename=filename
        )
        logging.info(f"Uploading entities summary: {summary.counters}")

        _, summary, _ = self.driver.execute_query(
            EDGE_STRING,
            database_="neo4j",
            edges=edges,
            filename=filename
        )

        logging.info(f"Uploading entity edges summary: {summary.counters}")

        _, summary, _ = self.driver.execute_query(
            SIMILARITY_EDGE_STRING,
            database_="neo4j",
            edges=links,
            filename=filename
        )

        logging.info(f"Uploading document similarity edges summary: {summary.counters}")

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
    