from streamlit import multiselect, session_state, text_input, button, plotly_chart, tabs, dataframe, selectbox, metric, columns, error, slider
from shared import init
from loader import Neo4jExecutor
import plotly.express as px
from typing import Literal
from pandas import Series, DataFrame
from clustering import GraphClusterer
from community_analyser import Analyzer
import logging
from typing import Set

logging.basicConfig(level=logging.INFO)

def entity_plot(data: dict[tuple[str,str],int]):
    entity_dict = dict()
    for key, value in data.items():
        entity_dict[key[0]] = entity_dict.get(key[0],0) + value
    dfs = Series(entity_dict)
    dfs.sort_values(inplace=True,ascending=False)
    fig = px.bar(dfs.iloc[:20],title='Most frequent entities in the same document as searched entity')
    plotly_chart(fig)
    
def category_plot(data: dict[tuple[str,str],int]):
    category_dict = dict()
    for key, value in data.items():
        category_dict[key[1]] = category_dict.get(key[1],0) + value
    dfs = Series(category_dict)
    dfs.sort_values(inplace=True,ascending=False)
    fig = px.bar(dfs.iloc[:20],title='Most frequent types of entities in the same document as searched entity')
    plotly_chart(fig)
    
def combined_plot(data: dict[tuple[str,str],int]):
    dfs = Series({f"{key[0]}({key[1]})": val for key, val in data.items()})
    print(dfs.head())
    dfs.sort_values(inplace=True,ascending=False)
    fig = px.bar(dfs.iloc[:20],title='Most frequent entities with types in the same document as searched entity')
    plotly_chart(fig)

def has_files_changed(selections: Set[str], mode: Literal['articles','entities']) -> bool:
    if len(session_state[f'analyzed_files_{mode}']) == 0 and len(selections) > 0:
        return True
    union = selections.union(session_state[f'analyzed_files_{mode}'])
    intersection = selections.intersection(session_state[f'analyzed_files_{mode}'])
    return len(union - intersection) > 0

def show_statistics(n_nodes: int, modularity_score: float):
    col1, col2 = columns(2)
    col1.metric("Number of nodes", n_nodes)
    col2.metric("Modularity score", f"{modularity_score: .3f}")

def calculate_and_show_chart(mode: Literal['articles','entities'], files_changed: bool):
    analyzer: Analyzer = session_state['analyzer']
    cluster: GraphClusterer = session_state['cluster_driver']
    graph_name = 'DocumentWithDistance' if mode == 'articles' else 'EntitiesWithCoExistance'
    if select_btn and files_changed:
        session_state[f'analyzed_files_{mode}'] = set(selections)
        session_state[f'graph_{mode}'] = cluster.create_graph_projection(selections,graph_name)
        session_state[f'leiden_result_{mode}'] = cluster.leiden_cluster(session_state[f'graph_{mode}'])
        cluster.delete_graph_projection(graph_name)
        loader.update_with_communities(session_state[f'leiden_result_{mode}'][['nodeId','communityId']].to_dict(orient='records'),mode)    
        session_state[f'modularities_{mode}'] = analyzer.calculate_modularity(selections, mode)
    df = session_state.get(f'leiden_result_{mode}',DataFrame({'communityId': [], 'nodeId': []}))
    aggregated_df = df.groupby('communityId').aggregate({'nodeId': list}) 
    choice = selectbox(label='Choose community ID', options=aggregated_df.index, key=f'community_selectbox_{mode}')
    if choice is not None:
        df_modularities: DataFrame = session_state[f'modularities_{mode}']
        show_statistics(len(aggregated_df.loc[choice,'nodeId']),df_modularities.loc[choice, 'modularity'])
        result = analyzer.get_ents_from_community(int(choice), mode)
        result = result.sort_values('entityCount',ascending=False)
        result_tags = analyzer.get_article_tags_from_community(int(choice), mode)
        fig = px.bar(result.iloc[:20],x='entity',y='entityCount',title='Top 20 entities')
        plotly_chart(fig)
        res = result[['type','entityCount']].groupby('type', as_index=False).sum().sort_values('entityCount',ascending=False)
        fig = px.bar(res,x='type',y='entityCount',title='Top entity types')
        fig.update_xaxes(tickangle=45)
        plotly_chart(fig, key=f'plot_entity_type_{mode}')
        fig = px.bar(result_tags.sort_values('tagCount',ascending=False).iloc[:30],x='tag',y='tagCount',title='Top 30 tags in the comminuty')
        fig.update_xaxes(tickangle=45)
        plotly_chart(fig, key=f'plot_tag_type_{mode}')
        

init()
loader: Neo4jExecutor = session_state['loader']
selections = multiselect('Ask data from json file',loader.get_files(),[])
files_changed_articles, files_changed_ents = has_files_changed(set(selections),'articles'), has_files_changed(set(selections),'entities')
select_btn = button('Select')
entities, clustering_articles, clustering_ents = tabs(['entities', 'clustering - articles', 'clustering - ents'])
with entities:
    if select_btn:
        session_state['ents_all'] = loader.get_ners_count(selections)
    ents_all = session_state.get('ents_all', DataFrame({"entityName": [], 'count': []})).sort_values('count',ascending=False)
    ents_all['entityName'] = ents_all['entity'] + '(' + ents_all['type'] + ')'
    fig = px.bar(ents_all.iloc[:30], x='entityName', y='count', title="Most frequent entities in the database")
    fig = fig.update_xaxes(tickangle=45)
    plotly_chart(fig)
    
    ents_type = ents_all[['type','count']].groupby('type',as_index=False).sum()
    fig = px.bar(ents_type,x='type',y='count',title="Counts of entities type across entire corpus.")
    plotly_chart(fig)
    
    ents_min, ents_max = ents_all['count'].min(), ents_all['count'].max()
    selected_min, selected_max = slider("Choose max and min for entities counts.",
                              min_value=ents_min,
                              max_value=ents_max,
                              value=(ents_min,ents_max)
                              )
    fig = px.histogram(ents_all.loc[(ents_all['count'] >= selected_min) & (ents_all['count'] <= selected_max)],
                       x='count',
                       title='Selected slice of histogram of entities count')
    plotly_chart(fig)
     
    entity = multiselect('Write entity you want to search', session_state.get('ents',[]),disabled=len(session_state.get('ents',[]))==0,max_selections=1)
    search_button = button('Search',disabled=len(session_state.get('ents',[]))==0)
    
    if len(entity) == 1 and search_button:
        counts = loader.get_linked_ners(entity[0].split()[0],selections)
        entity_plot(counts)
        category_plot(counts)
        combined_plot(counts)
with clustering_articles:
    calculate_and_show_chart('articles',files_changed_articles)
with clustering_ents:
    calculate_and_show_chart('entities',files_changed_ents)