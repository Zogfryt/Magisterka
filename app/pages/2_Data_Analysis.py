from streamlit import multiselect, session_state, text_input, button, plotly_chart, tabs, dataframe, selectbox
from shared import init
from loader import Neo4jExecutor
import plotly.express as px
from typing import Dict, Tuple
from pandas import Series, DataFrame
from clustering import GraphClusterer
from community_analyser import Analyzer

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

init()

loader: Neo4jExecutor = session_state['loader']
selections = multiselect('Ask data from json file',loader.get_files(),[])
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
    if select_btn:
        if 'graph' not in session_state:
            session_state['graph'] = cluster.create_graph_projection(selections)
            session_state['leiden_result'] = cluster.leiden_cluster(session_state['graph'])
            print(session_state['leiden_result'])
    df = session_state.get('leiden_result',DataFrame({'communityId': [], 'nodeId': []}))
    aggregated_df = df.groupby('communityId').aggregate({'nodeId': list}) 
    choice = selectbox(label='Choose community ID', options=aggregated_df.index)
    community_search_button = button('Search Community')
    loader.update_with_communities(df[['nodeId','communityId']].to_dict(orient='records'))
    analyzer: Analyzer = session_state['analyzer']
    df = analyzer.calculate_modularity(selections)
    df.to_csv('sample_data.csv',index=False,encoding='utf-8')
    if community_search_button:
        
        result = analyzer.get_ents_from_community(aggregated_df.loc[choice,'nodeId'])
        result = result.sort_values('entityCount',ascending=False)
        fig = px.bar(result.iloc[:20],title='Top 20 entities')
        plotly_chart(fig)
    