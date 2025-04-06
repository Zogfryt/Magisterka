from neo4j import Driver, Result
from graphdatascience import GraphDataScience, Graph
from typing import List
from itertools import chain
from pandas import DataFrame
import logging

ENTITY_GROUP_QUERY="""
MATCH (e:Entity)-[r:USED_IN]->(a:Article)
WHERE id(a) in $communityNodes
WITH e, sum(r.count) as entityCount
RETURN e{.entity, entityCount}
"""

GRAPH_PROJECTION_FOR_MODULARITY_QUERY = '''
MATCH (source: Article)-[r:SIMILARITY]-(target: Article)
WHERE source.filename IN ["output.json"] AND source.communityId IS NOT NULL AND target.communityId IS NOT NULL
RETURN gds.graph.project('Modularity',
source,
target,
{
    sourceNodeProperties: source { .communityId },
    targetNodeProperties: target { .communityId },
    relationshipProperties: r { .cosinus }
}, {undirectedRelationshipTypes: ['*']})'''

class Analyzer:
    neo4j_driver: Driver
    gds_driver: GraphDataScience

    def __init__(self, neo4j_driver: Driver, gds_driver: GraphDataScience):
        self.neo4j_driver = neo4j_driver
        self.gds_driver = gds_driver

        try:
            self.neo4j_driver.verify_connectivity()
        except Exception:
            logging.error("connection error")

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
        try:
            graph, _ = self.gds_driver.graph.cypher.project(
                GRAPH_PROJECTION_FOR_MODULARITY_QUERY,
                database='neo4j',
                selections=selections
            )
        except Exception:
            self.gds_driver.graph.drop('Modularity')
            graph, _ = self.gds_driver.graph.cypher.project(
                GRAPH_PROJECTION_FOR_MODULARITY_QUERY,
                database='neo4j',
                selections=selections
            )
        return graph
    
    def calculate_modularity(self, selections: List[str]) -> DataFrame:
        graph = self._create_modularity_projection(selections)
        result = self.gds_driver.modularity.stream(graph,communityProperty='communityId', relationshipWeightProperty='cosinus')
        self.gds_driver.graph.drop(graph)
        return result.set_index('communityId',drop=True)
    