from graphdatascience import GraphDataScience, Graph
from pandas import DataFrame
import logging
from typing import List

class GraphClusterer:
    gds_driver: GraphDataScience
    graph: Graph
    def __init__(self, gds_driver: GraphDataScience):
        self.gds_driver = gds_driver

    def _create_graph_projection(self, selections: List[str]) -> Graph:
        graph, _ = self.gds_driver.graph.cypher.project(
            """
            MATCH (source: Article)-[r:SIMILARITY]-(target: Article)
            WHERE source.filename IN $selections
            RETURN gds.graph.project('DocumentWithDistance',source,target,{ relationshipProperties: r { .cosinus } }, {undirectedRelationshipTypes: ['*']})""",
            database='neo4j',
            selections=selections
        )
        return graph

    def delete_graph_projection(self):
        result = self.gds_driver.graph.drop(graph='DocumentWithDistance', dbName='neo4j')
        logging.info(result)

    def create_graph_projection(self, selections: List[str]) -> Graph:
        try:
            return self._create_graph_projection(selections)
        except Exception:
            self.delete_graph_projection()
            return self._create_graph_projection(selections)
        
    def leiden_cluster(self, graph: Graph) -> DataFrame:
        return self.gds_driver.leiden.stream(graph)