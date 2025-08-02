from neo4j import Result, Driver, Session
import logging
from dataclasses_custom import Document, LinkVector, Matches, Entity
from typing import Literal
from pandas import DataFrame
from itertools import chain
from collections import Counter
from pathlib import Path
import os
from timeit import default_timer
from collapser import create_similarity_links, create_similarity_links_between_files
from collections import defaultdict

logging.basicConfig(level=logging.INFO)

DB_NAME = 'neo4j'

FILES_QUERY = 'MATCH (a:Article) RETURN COLLECT(DISTINCT a.filename) AS file'

SIMILARITY_EDGE_STRING = """CALL {
UNWIND $edges as edge
MATCH (a:Article {url: edge.url1})
MATCH (b:Article {url: edge.url2})
MERGE (b)-[r:SIMILARITY {cosinus: edge.cosinus, jaccard: edge.jaccard}]-(a)
} IN CONCURRENT TRANSACTIONS
"""

CONNECTION_BETWEEN_ENTS_STRING = """
CALL {{
UNWIND $edges as edge
MATCH (e:Entity {{entity: edge.name1, type: edge.type1}})
MATCH (ee:Entity {{entity: edge.name2, type: edge.type2}})
MERGE (e)-[a:APPEARANCE {{{key}: edge.count}}]-(ee)
}} IN CONCURRENT TRANSACTIONS
"""

GET_DOCUMENT_BY_FILENAME_QUERY = '''
MATCH (a: Article)-[r:USED_IN]-(e: Entity)
WHERE a.filename = $filename
RETURN a.url as url, r.count as count, e.entity as entity, e.type as type
'''

INDEX_ARTICLE_URL = """
CREATE INDEX article_index_url IF NOT EXISTS FOR (a:Article) on (a.url)"""

INDEX_ENTITY_NAME_TYPE = """
CREATE INDEX entity_index_entitiy_type IF NOT EXISTS FOR (e:Entity) on (e.entity,e.type)"""

INDEX_ARTICLE_URL_FILENAME = '''
CREATE INDEX article_index_url_filename IF NOT EXISTS FOR (a:Article) on (a.url,a.filename)'''

INDEX_ENTITY_INDEX = '''
CREATE INDEX entity_index_index IF NOT EXISTS FOR (e:Entity) on (e.index)'''

class Neo4jExecutor:
    
    driver: Driver
    conf_path: Path
    
    def __init__(self, driver: Driver, conf_path: Path):
        self.driver = driver 
        self.conf_path = conf_path
        try:
            self.driver.verify_connectivity()
        except Exception:
            logging.error("connection error")
        
        try:
            with self.driver.session(database=DB_NAME) as session:
                session.run(INDEX_ARTICLE_URL)
                session.run(INDEX_ARTICLE_URL_FILENAME)
                session.run(INDEX_ENTITY_NAME_TYPE)
                session.run(INDEX_ENTITY_INDEX)
        except Exception:
            logging.error(f"Indexes creation failure")
            
        if not os.path.exists(self.conf_path):
            os.mkdir(self.conf_path)

    def get_files(self) -> list[str]:
        with self.driver.session(database=DB_NAME) as session:
            result = session.run(FILES_QUERY)
            return next(result)['file']
        
    def _get_files(self,session: Session) -> list[str]:
        result = session.run(FILES_QUERY)
        return next(result)['file']
        
    def get_ners_count(self, json_names: list[str]) -> DataFrame:
        records = self.driver.execute_query(
            """MATCH (e: Entity)-[r: USED_IN]->(a:Article)
            WHERE a.filename in $json_names
            RETURN e.entity AS entity, SUM(r.count) AS count, e.type as type""",
            database_=DB_NAME,
            json_names=json_names,
        )[0]

        return DataFrame([record.data() for record in records])


    def load_data(self, docs: list[Document], filename: str):
        logging.info("Loading to database new") 
        start = default_timer()

        with self.driver.session(database=DB_NAME) as session:
            result = session.run(
                """CALL {
                UNWIND $documents AS docs
                Merge (e:Entity {entity: docs.ent_name, type: docs.ent_type})
                ON CREATE SET e.index = docs.ent_name + "_" + docs.ent_type
                Merge (a:Article {url: docs.url, title: docs.title, content: docs.content, lead_content: docs.lead_content, recipe_label: docs.recipe_label, tags: docs.tags, filename: $filename})
                Merge (e)-[:USED_IN {count: docs.count}]-(a)
                } IN CONCURRENT TRANSACTIONS
                """,
                database_="neo4j",
                documents=list(chain.from_iterable(doc.neo4j_json_list() for doc in docs)),
                filename=filename
            )
        
            logging.info(f"Uploading docs, ents nodes and count edges summary: {result.consume().counters}")
            logging.info(f"Calculating and uploading similarities between articles")

            result = session.run(
                SIMILARITY_EDGE_STRING,
                edges=self._prepare_similarity_links(create_similarity_links(docs))
            )

            logging.info(f"Uploading document similarity edges summary : {result.consume().counters}")

            files = self._get_files(session)
            files.remove(filename)
            for file in files:
                logging.info(f"Calculating and uploading similarities for {file}")
                other_docs = self._get_documents(session,file)
                links = create_similarity_links_between_files(docs,other_docs)
                result = session.run(SIMILARITY_EDGE_STRING,edges=self._prepare_similarity_links(links))
                logging.info(f"Uploading document similarity edges summary for {file} : {result.consume().counters}")
         
            result = session.run(
                CONNECTION_BETWEEN_ENTS_STRING.format(key=filename.replace('.json','')),
                database_="neo4j",
                edges=self._get_entity_links(docs) ,
                filename=filename,
            )        

            logging.info(f"Uploading entity co-appearance edges summary: {result.consume().counters}")
            stop = default_timer()
            logging.info(f"Upload took {stop-start}s")

    def _prepare_similarity_links(self, similarity_links: list[LinkVector]) -> dict[str,str|int]:
        return [
                {"url1": sim.url1,
                "url2": sim.url2,
                "cosinus": sim.cosinus,
                'jaccard': sim.jaccard}
                for sim in similarity_links]
    
    def delete_json(self, json_name: str):
        with self.driver.session() as session:
            def delete_articles(tx):
                return tx.run(
                    "MATCH (a:Article {filename: $filename}) DETACH DELETE a",
                    filename = json_name
                )
            
            def delete_entities(tx):
                return tx.run(
                    "MATCH (e:Entity) WHERE not (e)-[:USED_IN]-() DETACH DELETE e",
                    filename = json_name
                )
            
            session.execute_write(delete_articles)
            session.execute_write(delete_entities)
            
    def get_linked_ners(self, entity: str, ent_type: str, files: list[str]):
        with self.driver.session() as session:
            def list_json_files(tx) -> Result:
                return tx.run(
                '''MATCH (e:Entity {entity: $entity, type: $ent_type})--(a:Article)
WHERE (a.filename IN $files) 
WITH a, e
MATCH (a)-[r:USED_IN]-(e1:Entity)
WHERE e.entity <> e1.entity
RETURN e1, r
''',
                entity=entity,
                ent_type=ent_type,
                files=files
                )
            return_dict: dict[tuple[str,str], int]= dict()
            
            for record in list_json_files(session):
                key=(record[0]['entity'],record[0]['type'])
                return_dict[key] = return_dict.get(key,0) + record[1]['count']
            return return_dict
        
    def update_with_communities(self, communities: list[dict[str,str]], key: str,mode: Literal['articles','entities']):
        if mode == 'articles':
            _, summary, _  = self.driver.execute_query(
                """UNWIND $communities as data
                MATCH (a: Article)
                WHERE id(a) = toInteger(data.nodeId)
                SET a[$key] = data.communityId""",
                database_="neo4j",
                communities=communities,
                key=key
            )
            logging.info(f"Updating community nodes in {mode}. Status: {summary.counters}")
        else:
            _, summary, _  = self.driver.execute_query(
                """UNWIND $communities as data
                MATCH (e: Entity)
                WHERE id(e) = toInteger(data.nodeId)
                SET e[$key] = data.communityId""",
                database_="neo4j",
                communities=communities,
                key=key
            )
            logging.info(f"Updating community nodes in {mode}. Status: {summary.counters}")

    def _get_documents(self, session: Session, filename: str) -> dict[str,dict[Entity,int]]:
        result_dict = defaultdict(dict)
        result = session.run(GET_DOCUMENT_BY_FILENAME_QUERY, filename=filename)

        for record in result:
            url = record['url']
            ent = Entity(name=record['entity'],type_=record['type'])
            result_dict[url][ent] = record['count']

        return result_dict

    def _get_entity_links(self, docs: list[Document]) -> list[dict[str,str|float]]:
        summed_counter = sum((doc.return_tuple_connections() for doc in docs),Counter())
        summed_dict = dict(summed_counter)

        edges = []
        for rec, count in summed_dict.items():
            tup1, tup2 = rec
            edges.append({'name1': tup1.name, 'type1': tup1.type_, 'name2': tup2.name, 'type2': tup2.type_, 'count': count})
        return edges
    
    def check_ent_types_integrity(self, matches: Matches, documents: list[Document]) -> set[str]:
        all_ent_types = set(chain.from_iterable([ent.type_ for ent in document.entities] for document in documents))
        all_matches_types = set(chain(matches.matching,matches.non_matching))

        ent_not_in_conf = all_ent_types - all_matches_types
        return ent_not_in_conf
        
    def save_matches_config(self, matches: Matches, filename: str):
        conf_path = self.conf_path / filename
        
        with open(conf_path,'w',encoding='utf-8') as file:
            file.write(str(matches))
        
    
    