from neo4j import Driver, Result
from graphdatascience import GraphDataScience, Graph
from dataclasses_custom import Matches, Mode, Distance
from itertools import chain
from pandas import DataFrame
import logging
from numpy import select
from pathlib import Path
import tomllib

logging.basicConfig(level=logging.INFO)

CLUSTER_DISTRIBUTION_QUERY="""
MATCH (a:{node_type})
WHERE a.{key} is not NULL
RETURN a.{key} AS cluster, count(a) AS nodeCount
ORDER BY nodeCount DESC"""

ENTITY_GROUP_QUERY="""
MATCH (e:Entity)-[r:USED_IN]->(a:Article)
WHERE a[$key] = $communityId
WITH e, sum(r.count) as entityCount
RETURN e{.entity, .type, entityCount}
"""

ENTITY_GROUP_QUERY_BY_ENTITY="""
MATCH (e:Entity)-[r:USED_IN]->(a:Article)
WHERE e[$key] = $communityId
WITH e, sum(r.count) as entityCount
RETURN e{.entity, .type, entityCount}
"""

TAG_COUNTER_FOR_ENTITY_COMMUNITY="""
MATCH (e:Entity)-[r:USED_IN]->(a:Article)
WHERE e[$key] = $communityId
WITH DISTINCT a
UNWIND a.tags as tag
RETURN tag, count(tag) as tagCount
"""

TAG_COUNTER_FOR_ARTICLE_COMMUNITY="""
MATCH (a:Article)
WHERE a[$key] = $communityId
UNWIND a.tags as tag
RETURN tag, count(tag) as tagCount
"""

GRAPH_PROJECTION_FOR_MODULARITY_QUERY = '''
MATCH (source: Article)-[r:SIMILARITY]-(target: Article)
WHERE source.url < target.url AND source.{communityId} IS NOT NULL AND target.{communityId} IS NOT NULL
RETURN gds.graph.project('Modularity_Articles',
source,
target,
{{
    sourceNodeProperties: source {{ .{communityId} }},
    targetNodeProperties: target {{ .{communityId} }},
    relationshipProperties: r {{ .{metric} }}
}}, {{undirectedRelationshipTypes: ['*']}})'''

GRAPH_PROJECTION_FOR_MODULARITY_QUERY_ENTS = '''
MATCH (source:Entity)-[r:APPEARANCE]-(target:Entity)
WHERE source.index < target.index AND source.{communityId} IS NOT NULL AND target.{communityId} IS NOT NULL
WITH source, target, {equation} as count
WHERE count > 0
RETURN gds.graph.project('Modularity_Entities',
source,
target,
{{
    sourceNodeProperties: source {{ .{communityId} }},
    targetNodeProperties: target {{ .{communityId} }},
    relationshipProperties: {{ count: count }}
}}, {{undirectedRelationshipTypes: ['*']}})'''

ARTICLE_TAGS_COMMUNITY_MINING_QUERY = '''
MATCH (a:Article)
WHERE a.filename IN $selections 
UNWIND a.tags as tag
RETURN tag, count(a[$key]) as n_appearances, count(DISTINCT a[$key]) as n_communities
'''

ARTICLE_TAGS_COMMUNITY_MINING_QUERY_ENTITY = '''
MATCH (a:Article)-[r:USED_IN]-(e:Entity)
WHERE a.filename IN $selections
WITH a, COLLECT(DISTINCT e[$key]) as articleCommunities
UNWIND a.tags as tag
WITH tag, COUNT(*) as n_appearances, COLLECT(articleCommunities) as allCommunitiesPerTag
UNWIND allCommunitiesPerTag as communities
UNWIND communities as community
WITH tag, n_appearances, COLLECT(DISTINCT community) as distinctCommunities
RETURN tag, SIZE(distinctCommunities) as n_communities, n_appearances
'''

MATCHING_ENTS_QUERY_ARTICLE = '''
MATCH (a:Article)-[r:USED_IN]-(e:Entity)
WHERE a[$key] = $communityId and e.type in $matching and a.filename = $selection
RETURN sum(r.count) AS counts
'''

MATCHING_ENTS_QUERY_ENTITY = '''
MATCH (a:Article)-[r:USED_IN]-(e:Entity)
WHERE e[$key] = $communityId and e.type in $matching and a.filename = $selection
RETURN sum(r.count) AS counts
'''

NON_MATCHING_ENTS_QUERY_ENTITY = '''
MATCH (a:Article)-[r:USED_IN]-(e:Entity)
WHERE e[$key] = $communityId and e.type in $non_matching and a.filename = $selection
RETURN sum(r.count) AS counts
'''

NON_MATCHING_ENTS_QUERY_ARTICLE = '''
MATCH (a:Article)-[r:USED_IN]-(e:Entity)
WHERE a[$key] = $communityId and e.type in $non_matching and a.filename = $selection
RETURN sum(r.count) AS counts
'''


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

    def get_ents_from_community(self, communityId: int, key: str, mode: Mode) -> DataFrame:    
        records, _, _ = self.neo4j_driver.execute_query(
            ENTITY_GROUP_QUERY if mode == Mode.articles else ENTITY_GROUP_QUERY_BY_ENTITY,
            communityId=communityId,
            database_='neo4j',
            key=key
        )
    
        all_records =  list(chain(*[record.values() for record in records]))
        return DataFrame(all_records).groupby(['entity','type'],as_index=False).sum()
    
    def get_article_tags_from_community(self, communityId: int, key: str, mode: Mode) -> DataFrame:
        records, _, _  = self.neo4j_driver.execute_query(
            TAG_COUNTER_FOR_ARTICLE_COMMUNITY if mode == Mode.articles else TAG_COUNTER_FOR_ENTITY_COMMUNITY,
            communityId=communityId,
            database_='neo4j',
            key=key
        )
        return DataFrame([record.data() for record in records])
    
    def _create_modularity_projection(self, 
                                      selections: list[str], 
                                      key: str, 
                                      mode: Mode, 
                                      distance: Distance | None = None) -> Graph:
        if mode == Mode.articles:
            if distance is None:
                raise AttributeError('With articles mode distance is mandatory')
            query = GRAPH_PROJECTION_FOR_MODULARITY_QUERY.format(communityId=key, metric=distance.name) 
        else:
            keys = [k.replace('.json','') for k in selections]
            equation = ' + '.join([f"coalesce(r.{key},0)" for key in keys])
            query = GRAPH_PROJECTION_FOR_MODULARITY_QUERY_ENTS.format(communityId=key,equation=equation) 
        logging.info(f"{mode.name}: {query}")
        graph_name = 'Modularity_Articles' if mode == Mode.articles else 'Modularity_Entities'
        try:
            graph, _ = self.gds_driver.graph.cypher.project(
                query,
                database='neo4j',
                selections=selections
            )
        except Exception:
            self.gds_driver.graph.drop(graph_name)
            graph, _ = self.gds_driver.graph.cypher.project(
                query,
                database='neo4j',
                selections=selections
            )
        return graph
    
    
    def calculate_modularity(self, selections: list[str], key: str, mode: Mode, distance: Distance | None) -> DataFrame:
        graph = self._create_modularity_projection(selections, key, mode, distance=distance)
        if mode == Mode.articles and distance is not None:
            result = self.gds_driver.modularity.stream(graph,communityProperty=key, relationshipWeightProperty=distance.name)
        elif mode == Mode.entities:
            result = self.gds_driver.modularity.stream(graph,communityProperty=key, relationshipWeightProperty='count')
        elif mode == Mode.articles and distance is None:
            raise AttributeError("Distance is required paremeter while using articles mode")
        else:
            raise AttributeError(f"In calculating modularity you can choose only: {[opt.name for opt in Mode]}")
        self.gds_driver.graph.drop(graph)
        return result.set_index('communityId',drop=True)
    

    def get_article_tags_class(self, selections: list[str], key: str, mode: Mode) -> DataFrame:
        records, _, _ = self.neo4j_driver.execute_query(
            ARTICLE_TAGS_COMMUNITY_MINING_QUERY if mode == Mode.articles else ARTICLE_TAGS_COMMUNITY_MINING_QUERY_ENTITY,
            selections=selections,
            key=key,
            database_='neo4j'
        )
        
        df = DataFrame([record.data() for record in records])
        conditions = [
            (df['n_appearances'] > 1) & (df['n_communities'] > 1),
            (df['n_appearances'] > 1) & (df['n_communities'] == 1)
        ]
        df['class'] = select(conditions,['B','A'], default='C')
        return df
    
    def get_matches_criteria(self, selections: list[str], conf_path: Path) -> dict[str,Matches]:
        return_dict = {}
        for selection in selections:
            filename = conf_path / selection.replace('.json','.toml')
            with open(filename, 'rb') as file:
                data = tomllib.load(file)
                return_dict[selection] = Matches(
                    matching=data['matches']['matching'],
                    non_matching=data['matches']['non_matching']
                )
        return return_dict
    
    def calcalate_matching_ent_metric(self,
                                      matches: dict[str,Matches],
                                      communityId: int,
                                      cluster_key: str,
                                      mode: Mode
                                      ) -> tuple[float,float]:
        matching_scores, non_matching_scores = [], []
        query_match = MATCHING_ENTS_QUERY_ARTICLE if mode == Mode.articles else MATCHING_ENTS_QUERY_ENTITY
        query_non_match = NON_MATCHING_ENTS_QUERY_ARTICLE if mode == Mode.articles else NON_MATCHING_ENTS_QUERY_ENTITY
        for key, matches_ in matches.items():
            records, _, _ = self.neo4j_driver.execute_query(
            query_match,
            selection=key,
            communityId=communityId,
            matching=matches_.matching,
            database_='neo4j',
            key=cluster_key
            )
            matching_scores.append(records[0].data()['counts'])
            records, _, _ = self.neo4j_driver.execute_query(
            query_non_match,
            selection=key,
            communityId=communityId,
            non_matching=matches_.non_matching,
            database_='neo4j',
            key=cluster_key
            )
            non_matching_scores.append(records[0].data()['counts'])
        matching = sum(matching_scores)
        non_matching = sum(non_matching_scores)

        return matching / (matching+non_matching+1e-5), non_matching / (matching+non_matching+1e-5)
    
    def is_clustering_needed(self, key: str, mode: Mode) -> bool:
        node_type = 'Article' if mode == Mode.articles else 'Entity'
        records, _, _ = self.neo4j_driver.execute_query(
            f"""
            MATCH (e:{node_type})
            WHERE e.{key} IS NOT NULL
            RETURN COUNT(e) as counts
            """,
            database_='neo4j',
            key=key
        )
        return records[0].data()['counts'] == 0
    
    def get_community_nodes(self, key: str, mode: Mode) -> DataFrame:
        if mode == Mode.entities:
            records, _, _ = self.neo4j_driver.execute_query(
                """
                MATCH (e:Entity)
                WHERE e[$key] IS NOT NULL
                RETURN id(e) AS nodeId, e[$key] AS communityId
                """,
                database_='neo4j',
                key=key
            )

        else:
            records, _, _ = self.neo4j_driver.execute_query(
                """
                MATCH (a:Article)
                WHERE a[$key] IS NOT NULL
                RETURN id(a) AS nodeId, a[$key] AS communityId
                """,
                database_='neo4j',
                key=key
            )
        return DataFrame([record.data() for record in records])
    # df.drop(columns=['n_appearances','n_communities'])
    def analyse_cluster_sizes_distribution(self, key: str, mode: Mode) -> DataFrame:
        node_type = 'Article' if mode == Mode.articles else 'Entity'
        records, _, _ = self.neo4j_driver.execute_query(
            CLUSTER_DISTRIBUTION_QUERY.format(
                node_type=node_type,
                key=key
            ),
            database_='neo4j'
        )
        return DataFrame([record.data() for record in records])