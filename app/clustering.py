from graphdatascience import GraphDataScience, Graph
from pandas import DataFrame
import logging
from typing import List, Literal

class GraphClusterer:
    gds_driver: GraphDataScience
    def __init__(self, gds_driver: GraphDataScience):
        self.gds_driver = gds_driver

    def _create_graph_projection_articles(self, selections: List[str]) -> Graph:
        graph, _ = self.gds_driver.graph.cypher.project(
            """
            MATCH (source: Article)-[r:SIMILARITY]-(target: Article)
            WHERE source.filename IN $selections
            RETURN gds.graph.project('DocumentWithDistance',source,target,{ relationshipProperties: r { .cosinus } }, {undirectedRelationshipTypes: ['*']})""",
            database='neo4j',
            selections=selections
        )
        return graph
    
    def _create_graph_projection_entities(self, selections: List[str]) -> Graph:
        graph, _ = self.gds_driver.graph.cypher.project(
            """
            MATCH (source: Entity)-[r:APPEARANCE]-(target: Entity)
            WHERE source.filename IN $selections
            RETURN gds.graph.project('EntitiesWithCoExistance',source,target,{ relationshipProperties: r { .count } }, {undirectedRelationshipTypes: ['*']})""",
            database='neo4j',
            selections=selections
        )
        return graph

    def delete_graph_projection(self, graph_name: Literal['DocumentWithDistance','EntitiesWithCoExistance']):
        result = self.gds_driver.graph.drop(graph=graph_name, dbName='neo4j')
        logging.info(result)

    def _create_graph_projection_with_type(self,selections: List[str], graph_name: Literal['DocumentWithDistance','EntitiesWithCoExistance']) -> Graph:
        if graph_name == 'DocumentWithDistance':
            return self._create_graph_projection_articles(selections)
        elif graph_name == 'EntitiesWithCoExistance':
            return self._create_graph_projection_entities(selections)
        else:
            raise AttributeError("For graph name you have to choose one of: DocumentWithDistance, EntitiesWithCoExistance")

    def create_graph_projection(self, selections: List[str], graph_name: Literal['DocumentWithDistance','EntitiesWithCoExistance']) -> Graph:
        try:
            return self._create_graph_projection_with_type(selections, graph_name)
        except AttributeError as e:
            raise e
        except Exception:
            self.delete_graph_projection(graph_name)
            return self._create_graph_projection_with_type(selections, graph_name)
        
    def leiden_cluster(self, graph: Graph) -> DataFrame:
        return self.gds_driver.leiden.stream(graph)