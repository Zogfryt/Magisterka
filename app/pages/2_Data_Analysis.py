from streamlit import multiselect, session_state, text_input, button, plotly_chart, tabs, dataframe, selectbox, metric, columns
from shared import init
from loader import Neo4jExecutor
import plotly.express as px
from typing import Dict, Tuple
from pandas import Series, DataFrame
from clustering import GraphClusterer
from community_analyser import Analyzer
import logging
from typing import Set

logging.basicConfig(level=logging.INFO)

def entity_plot(data: Dict[Tuple[str,str],int]):
    entity_dict = dict()
    for key, value in data.items():
        entity_dict[key[0]] = entity_dict.get(key[0],0) + value
    dfs = Series(entity_dict)
    dfs.sort_values(inplace=True,ascending=False)
    fig = px.bar(dfs.iloc[:20],title='Most frequent entities in the same document as searched entity')
    plotly_chart(fig)
    
def category_plot(data: Dict[Tuple[str,str],int]):
    category_dict = dict()
    for key, value in data.items():
        category_dict[key[1]] = category_dict.get(key[1],0) + value
    dfs = Series(category_dict)
    dfs.sort_values(inplace=True,ascending=False)
    fig = px.bar(dfs.iloc[:20],title='Most frequent types of entities in the same document as searched entity')
    plotly_chart(fig)
    
def combined_plot(data: Dict[Tuple[str,str],int]):
    dfs = Series({f"{key[0]}({key[1]})": val for key, val in data.items()})
    print(dfs.head())
    dfs.sort_values(inplace=True,ascending=False)
    fig = px.bar(dfs.iloc[:20],title='Most frequent entities with types in the same document as searched entity')
    plotly_chart(fig)

def has_files_changed(selections: Set[str]) -> bool:
    if len(session_state['analyzed_files']) == 0 and len(selections) > 0:
        return True
    union = selections.union(session_state['analyzed_files'])
    intersection = selections.intersection(session_state['analyzed_files'])
    return len(union - intersection) > 0

def show_statistics(n_nodes: int, modularity_score: float):
    col1, col2 = columns(2)
    col1.metric("Number of nodes", n_nodes)
    col2.metric("Modularity score", f"{modularity_score: .3f}")


init()

loader: Neo4jExecutor = session_state['loader']
selections = multiselect('Ask data from json file',loader.get_files(),[])
analyzed_files_changed = has_files_changed(set(selections))
select_btn = button('Select')
entities, clustering = tabs(['entities', 'clustering'])
with entities:
    if select_btn:
        session_state['ents'] = loader.get_all_ners(selections)
    entity = multiselect('Write entity you want to search', session_state.get('ents',[]),disabled=len(session_state.get('ents',[]))==0,max_selections=1)
    search_button = button('Search',disabled=len(session_state.get('ents',[]))==0)
    if len(entity) == 1 and search_button:
        counts = loader.get_linked_ners(entity[0],selections)
        entity_plot(counts)
        category_plot(counts)
        combined_plot(counts)
with clustering:
    cluster: GraphClusterer = session_state['cluster_driver']
    analyzer: Analyzer = session_state['analyzer']
    if select_btn and analyzed_files_changed:
        session_state['analyzed_files'] = set(selections)
        session_state['graph'] = cluster.create_graph_projection(selections)
        session_state['leiden_result'] = cluster.leiden_cluster(session_state['graph'])
        cluster.delete_graph_projection()
        loader.update_with_communities(session_state['leiden_result'][['nodeId','communityId']].to_dict(orient='records'))    
        session_state['modularities'] = analyzer.calculate_modularity(selections)
    df = session_state.get('leiden_result',DataFrame({'communityId': [], 'nodeId': []}))
    aggregated_df = df.groupby('communityId').aggregate({'nodeId': list}) 
    choice = selectbox(label='Choose community ID', options=aggregated_df.index)
    community_search_button = button('Search Community')
    if choice is not None:
        df_modularities: DataFrame = session_state['modularities']
        show_statistics(len(aggregated_df.loc[choice,'nodeId']),df_modularities.loc[choice, 'modularity'])
        result = analyzer.get_ents_from_community(aggregated_df.loc[choice,'nodeId'])
        result = result.sort_values('entityCount',ascending=False)
        fig = px.bar(result.iloc[:20],title='Top 20 entities')
        plotly_chart(fig)
    