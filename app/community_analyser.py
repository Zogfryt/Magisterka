from neo4j import Driver, Result
from graphdatascience import GraphDataScience, Graph
from typing import List
from itertools import chain
from pandas import DataFrame

ENTITY_GROUP_QUERY="""
MATCH (e:Entity)-[r:USED_IN]->(a:Article)
MATCH (e)-[rr:USED_IN]->(b:Article)
WHERE id(a) <> id(b) and id(a) in $communityNodes and id(b) in $communityNodes
WITH e, sum(r.count) as entityCount
RETURN e{.entity, entityCount}
"""

class Analyzer:
    neo4j_driver: Driver
    gds_driver: GraphDataScience

    def __init__(self, neo4j_driver: Driver, gds_driver: GraphDataScience):
        self.neo4j_driver = neo4j_driver
        self.gds_driver = gds_driver

        try:
            self.neo4j_driver.verify_connectivity()
        except Exception:
            print("connection error")

    def get_ents_from_community(self, node_list: List[int]) -> DataFrame:
        def aggregate_entities(tx, node_list: List[int]) -> DataFrame:
            result: Result =  tx.run(
                ENTITY_GROUP_QUERY,
                communityNodes=node_list
            )
            all_records =  list(chain(*[record.values() for record in result]))
            return DataFrame(all_records).groupby('entity').sum()
    
        
        with self.neo4j_driver.session() as session:
            values: DataFrame = session.execute_read(aggregate_entities, node_list)
        
        return values
    
    def _create_modularity_projection(self, selections: List[str]) -> Graph:
        query = """
            MATCH (source: Article)-[r:SIMILARITY]-(target: Article)
            WHERE source.filename IN $selections AND source.communityId IS NOT NULL AND target.communityId IS NOT NULL
            RETURN gds.graph.project('Modularity',source,target,{ relationshipProperties: r { .cosinus } }, {undirectedRelationshipTypes: ['*'], nodeProperties: 'communityId'})"""
        try:
            graph, _ = self.gds_driver.graph.cypher.project(
                query,
                database='neo4j',
                selections=selections
            )
        except Exception:
            self.gds_driver.graph.drop('Modularity')
            graph, _ = self.gds_driver.graph.cypher.project(
                query,
                database='neo4j',
                selections=selections
            )
        return graph
    
    def calculate_modularity(self, selections: List[str]) -> DataFrame:
        graph = self._create_modularity_projection(selections)
        result = self.gds_driver.modularity.stream(graph,communityProperty='communityId', relationshipWeightProperty='cosinus')
        self.gds_driver.graph.drop(graph)
        return result
    