from neo4j import Driver, Result
from graphdatascience import GraphDataScience, Graph
from typing import Literal
from itertools import chain
from pandas import DataFrame
import logging
from numpy import select
from streamlit import dataframe

ENTITY_GROUP_QUERY="""
MATCH (e:Entity)-[r:USED_IN]->(a:Article)
WHERE a.communityId = $communityId
WITH e, sum(r.count) as entityCount
RETURN e{.entity, .type, entityCount}
"""

ENTITY_GROUP_QUERY_BY_ENTITY="""
MATCH (e:Entity)-[r:USED_IN]->(a:Article)
WHERE e.communityId = $communityId
WITH e, sum(r.count) as entityCount
RETURN e{.entity, .type, entityCount}
"""

TAG_COUNTER_FOR_ENTITY_COMMUNITY="""
MATCH (e:Entity)-[r:USED_IN]->(a:Article)
WHERE e.communityId = $communityId
WITH DISTINCT a
UNWIND a.tags as tag
RETURN tag, count(tag) as tagCount
"""

TAG_COUNTER_FOR_ARTICLE_COMMUNITY="""
MATCH (a:Article)
WHERE a.communityId = $communityId
UNWIND a.tags as tag
RETURN tag, count(tag) as tagCount
"""

GRAPH_PROJECTION_FOR_MODULARITY_QUERY = '''
MATCH (source: Article)-[r:SIMILARITY]-(target: Article)
WHERE source.filename IN $selections AND source.communityId IS NOT NULL AND target.communityId IS NOT NULL
RETURN gds.graph.project('Modularity_Articles',
source,
target,
{
    sourceNodeProperties: source { .communityId },
    targetNodeProperties: target { .communityId },
    relationshipProperties: r { .cosinus }
}, {undirectedRelationshipTypes: ['*']})'''

GRAPH_PROJECTION_FOR_MODULARITY_QUERY_ENTS = '''
MATCH (source: Entity)-[r:APPEARANCE]-(target: Entity)
WHERE source.filename IN $selections AND source.communityId IS NOT NULL AND target.communityId IS NOT NULL
RETURN gds.graph.project('Modularity_Entities',
source,
target,
{
    sourceNodeProperties: source { .communityId },
    targetNodeProperties: target { .communityId },
    relationshipProperties: r { .count }
}, {undirectedRelationshipTypes: ['*']})'''

ARTICLE_TAGS_COMMUNITY_MINING_QUERY = '''
MATCH (a:Article)
WHERE a.filename IN $selections 
UNWIND a.tags as tag
RETURN tag, count(a.communityId) as n_appearances, count(DISTINCT a.communityId) as n_communities
'''

ARTICLE_TAGS_COMMUNITY_MINING_QUERY_ENTITY = '''
MATCH (a:Article)-[r:USED_IN]-(e:Entity)
WHERE a.filename IN $selections AND e.filename IN $selections
WITH a, COLLECT(DISTINCT e.communityId) as articleCommunities
UNWIND a.tags as tag
WITH tag, COUNT(*) as n_appearances, COLLECT(articleCommunities) as allCommunitiesPerTag
UNWIND allCommunitiesPerTag as communities
UNWIND communities as community
WITH tag, n_appearances, COLLECT(DISTINCT community) as distinctCommunities
RETURN tag, SIZE(distinctCommunities) as n_communities, n_appearances
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

    def get_ents_from_community(self, communityId: int, mode: Literal['articles','entities']) -> DataFrame:    
        records, _, _ = self.neo4j_driver.execute_query(
            ENTITY_GROUP_QUERY if mode == 'articles' else ENTITY_GROUP_QUERY_BY_ENTITY,
            communityId=communityId,
            database_='neo4j'
        )
    
        all_records =  list(chain(*[record.values() for record in records]))
        return DataFrame(all_records).groupby(['entity','type'],as_index=False).sum()
    
    def get_article_tags_from_community(self, communityId: int, mode: Literal['articles','entities']) -> DataFrame:
        records, _, _  = self.neo4j_driver.execute_query(
            TAG_COUNTER_FOR_ARTICLE_COMMUNITY if mode == 'articles' else TAG_COUNTER_FOR_ENTITY_COMMUNITY,
            communityId=communityId,
            database_='neo4j'
        )
        return DataFrame([record.data() for record in records])
    
    def _create_modularity_projection(self, selections: list[str], mode: Literal['articles','entities']) -> Graph:
        query = GRAPH_PROJECTION_FOR_MODULARITY_QUERY if mode == 'articles' else GRAPH_PROJECTION_FOR_MODULARITY_QUERY_ENTS
        graph_name = 'Modularity_Articles' if mode == 'articles' else 'Modularity_Entities'
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
    
    
    def calculate_modularity(self, selections: list[str], mode: Literal['articles','entities']) -> DataFrame:
        graph = self._create_modularity_projection(selections, mode)
        if mode == 'articles':
            result = self.gds_driver.modularity.stream(graph,communityProperty='communityId', relationshipWeightProperty='cosinus')
        elif mode == 'entities':
            result = self.gds_driver.modularity.stream(graph,communityProperty='communityId', relationshipWeightProperty='count')
        else:
            raise AttributeError("In calculating modularity you can choose only: 'articles','entities'")
        self.gds_driver.graph.drop(graph)
        return result.set_index('communityId',drop=True)
    

    def get_article_tags_class(self, selections: list[str], mode: Literal['articles','entities']) -> DataFrame:
        records, _, _ = self.neo4j_driver.execute_query(
            ARTICLE_TAGS_COMMUNITY_MINING_QUERY if mode == 'articles' else ARTICLE_TAGS_COMMUNITY_MINING_QUERY_ENTITY,
            selections=selections,
            database_='neo4j'
        )
        
        df = DataFrame([record.data() for record in records])
        conditions = [
            (df['n_appearances'] > 1) & (df['n_communities'] > 1),
            (df['n_appearances'] > 1) & (df['n_communities'] == 1)
        ]
        
        df['class'] = select(conditions,['B','A'], default='C')
        return df
    # df.drop(columns=['n_appearances','n_communities'])