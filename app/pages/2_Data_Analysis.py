from streamlit import multiselect, session_state, write, button, plotly_chart, tabs, dataframe, selectbox, metric, columns, error, slider, text
from shared import init
from loader import Neo4jExecutor
import plotly.express as px
from plotly.graph_objects import Pie, Figure, Histogram
from pandas import Series, DataFrame
from clustering import GraphClusterer
from community_analyser import Analyzer
import logging
from typing import Set
from re import sub, search
from dataclasses_custom import Mode, Distance, GraphName

logging.basicConfig(level=logging.INFO)

def enlarge_axes(figure: Figure, range: tuple[float,float] = None, range_y: tuple[float,float] = None) -> Figure:
    xaxis = dict(
        title_font=dict(size=20),     
        tickfont=dict(size=16),  
    )
    if range is not None:
        xaxis['range'] = range
    yaxis = dict(
        title_font=dict(size=20),     
        tickfont=dict(size=16),       
    )
    if range_y is not None:
        yaxis['range'] = range_y
    return figure.update_layout(xaxis=xaxis,yaxis=yaxis)

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
    dfs.sort_values(inplace=True,ascending=False)
    fig = px.bar(dfs.iloc[:20],title='Most frequent entities with types in the same document as searched entity')
    plotly_chart(fig)

def has_files_changed(selections: Set[str], mode: Mode) -> bool:
    if len(session_state[f'analyzed_files_{mode}']) == 0 and len(selections) > 0:
        return True
    union = selections.union(session_state[f'analyzed_files_{mode}'])
    intersection = selections.intersection(session_state[f'analyzed_files_{mode}'])
    return len(union - intersection) > 0

def show_statistics(n_nodes: int, modularity_score: float):
    col1, col2 = columns(2)
    col1.metric("Number of nodes", n_nodes)
    col2.metric("Modularity score", f"{modularity_score: .3f}")

def show_graph_statistics(mode: Mode, suffix: str, communities: list[int]):
    modularities = session_state[f'modularities_{suffix}']
    analyzer: Analyzer = session_state['analyzer']
    tag_map: DataFrame = session_state[f'tag_class_mapping_{suffix}']
    class_A_ents, all_ents = 0, 0
    matching_scores = []
    
    for curr_community in communities:
        result_tags = analyzer.get_article_tags_from_community(int(curr_community), session_state[f'key_{suffix}'],mode)
        ent_matching_score = analyzer.calcalate_matching_ent_metric(
            session_state[f"match_conf_{suffix}"],
            int(curr_community),
            session_state[f'key_{suffix}'],
            mode)
        matching_scores.append(ent_matching_score[0])
        if not result_tags.empty:
            classified_tags = result_tags.merge(tag_map,left_on='tag',right_on='tag',how='left')
            all_ents += classified_tags['tagCount'].sum()
            class_A_ents += classified_tags.loc[classified_tags['class'] == 'A','tagCount'].sum()

    col1, col2, col3 = columns(3)
    col1.metric("Clustering modularity score", f"{modularities['modularity'].sum(): .2f}")
    col2.metric("Average percentage of A tag", f"{class_A_ents * 100 / all_ents : .2f}%")
    col3.metric("Average entity match", f"{sum(matching_scores)/len(matching_scores) : .2f}")

    graph_size_distribution = analyzer.analyse_cluster_sizes_distribution(session_state[f'key_{suffix}'], mode)
    fig = Figure(data=[Histogram(
        x=graph_size_distribution['nodeCount'],
        xbins=dict(
            start=0,
            end=graph_size_distribution['nodeCount'].max(),
            size=100
        ),
        marker=dict(
        line=dict(
            width=1, 
            color='white'
            )
        )
    )])
    write(f"Number of communities: {graph_size_distribution.shape[0]}")
    fig.update_layout(
        xaxis=dict(
            title='Number of Nodes',
            title_font=dict(size=20),     
            tickfont=dict(size=16),
            range=[0,(graph_size_distribution['nodeCount'].max() // 100 + 1) * 100]   
        ),
        yaxis=dict(
            title='Frequency',
            title_font=dict(size=20),     
            tickfont=dict(size=16),       
            range=[0, None]
        ),
        title='Distribution of community sizes',
)
    plotly_chart(fig, key=f'distribution_{session_state[f'key_{suffix}']}')

    

def generate_key(selections: str, suffix: str | None = None) -> str:
    base = '_'.join(selection.replace('.json','') for selection in sorted(selections)) 
    return base if suffix is None else base + '_' + suffix

def collect_mappings(suffix: str, key: str, mode: Mode, distance: Distance | None = None):
    session_state[f'tag_class_mapping_{suffix}'] = analyzer.get_article_tags_class(selections, key, mode)
    session_state[f'modularities_{suffix}'] = analyzer.calculate_modularity(selections, key, mode, distance)
    session_state[f"match_conf_{suffix}"] = analyzer.get_matches_criteria(selections,session_state['conf_path'])
    session_state[f'entity_list_{suffix}'] = analyzer.get_ents_with_key(key,mode)

def cluster_graph(loader: Neo4jExecutor, 
                  cluster: GraphClusterer, 
                  suffix: str, 
                  mode: Mode, 
                  metric: Distance | None = None):
    graph_name = GraphName.EntitiesWithCoExistance if 'entit' in suffix else GraphName.DocumentWithDistance
    logging.info('Generating graph projection')
    session_state[f'graph_{suffix}'] = cluster.create_graph_projection(selections,graph_name, metric)
    logging.info('Undergoing Laiden Clasterization')
    session_state[f'leiden_result_{suffix}'] = cluster.leiden_cluster(session_state[f'graph_{suffix}'])
    logging.info('Deleting graph projection')
    cluster.delete_graph_projection(graph_name)
    logging.info('Updating graph with new communities')
    loader.update_with_communities(
        session_state[f'leiden_result_{suffix}'][['nodeId','communityId']].to_dict(orient='records'),
        session_state[f'key_{suffix}'],
        mode)

def collect_data_ents(loader: Neo4jExecutor, cluster: GraphClusterer, analyzer: Analyzer, selections: list[str]):
    mode = Mode.entities
    key = generate_key(selections)
    session_state['key_entities'] = key
    if analyzer.is_clustering_needed(key,mode):
        cluster_graph(loader,cluster,mode.name, mode)
    else:
        session_state['leiden_result_entities'] = analyzer.get_community_nodes(key,mode)
    collect_mappings(mode.name,key,mode)

def collect_data_articles(loader: Neo4jExecutor, cluster: GraphClusterer, analyzer: Analyzer, selections: list[str]):
    mode = Mode.articles
    for option in Distance:
        suffix = mode.name + "_" + option.name
        key = generate_key(selections,suffix)
        session_state[f'key_{suffix}'] = key
        if analyzer.is_clustering_needed(key,mode):
            cluster_graph(loader,cluster,suffix,mode,option)
        else:
            session_state[f'leiden_result_{suffix}'] = analyzer.get_community_nodes(key,mode)
        collect_mappings(suffix,key,mode,option)

def _clear_cached_data(suffix: str, mode: Mode):
    session_state[f'analyzed_files_{mode.name}'] = set(selections)
    session_state[f'key_{suffix}'] = ''
    if f'leiden_result_{suffix}' in session_state:
        del session_state[f'leiden_result_{suffix}']
    if f'tag_class_mapping_{suffix}' in session_state:
        del session_state[f'tag_class_mapping_{suffix}']
    if f'modularities_{suffix}' in session_state:
        del session_state[f'modularities_{suffix}']

def clear_cached_data(mode: Mode):
    if mode == Mode.entities:
        _clear_cached_data(mode, mode)
    else:
        _clear_cached_data(mode.name + "_" + Distance.cosinus.name, mode)
        _clear_cached_data(mode.name + "_" + Distance.jaccard.name, mode)
    
def calculate_and_show_chart(mode: Mode, files_changed: bool, select_btn: bool):
    analyzer: Analyzer = session_state['analyzer']
    cluster: GraphClusterer = session_state['cluster_driver']
    loader: Neo4jExecutor = session_state['loader']
    if select_btn and files_changed and len(selections) > 0:
        session_state[f'analyzed_files_{mode.name}'] = set(selections)
        if mode == Mode.articles:
            collect_data_articles(loader,cluster,analyzer,selections)
        else: 
            collect_data_ents(loader,cluster,analyzer,selections)
    elif select_btn and files_changed:
        clear_cached_data(mode)
    if mode == Mode.articles:
        metric = selectbox(label='Choose clustering metric', options=[Distance.cosinus.name,"modified" + Distance.jaccard.name])
        distance = Distance.jaccard if 'modified' in metric else Distance.cosinus
        suffix = mode.name + "_" + distance.name
    else:
        suffix = mode.name
    df = session_state.get(f'leiden_result_{suffix}',DataFrame({'communityId': [], 'nodeId': []}))
    aggregated_df = df.groupby('communityId').aggregate({'nodeId': list}) 
    show_graph_statistics(mode,suffix,aggregated_df.index)
    choice = selectbox(label='Choose community ID', options=aggregated_df.index, key=f'community_selectbox_{suffix}')
    if choice is not None:
        df_modularities: DataFrame = session_state[f'modularities_{suffix}']
        show_statistics(len(aggregated_df.loc[choice,'nodeId']),df_modularities.loc[choice, 'modularity'])
        result = analyzer.get_ents_from_community(int(choice), session_state[f'key_{suffix}'],mode)
        result = result.sort_values('entityCount',ascending=False)
        result_top20 = result[['entity','entityCount']].groupby('entity',as_index=False).agg({'entityCount':'sum'}).sort_values('entityCount',ascending=False)
        result_tags = analyzer.get_article_tags_from_community(int(choice), session_state[f'key_{suffix}'],mode)
        fig = px.bar(result_top20.iloc[:20],x='entity',y='entityCount',title='Top 20 entities')

        plotly_chart(fig)
        res = result[['type','entityCount']].groupby('type', as_index=False).sum().sort_values('entityCount',ascending=False)
        fig = px.bar(res,x='type',y='entityCount',title='Top entity types')
        fig.update_xaxes(tickangle=45)
        plotly_chart(fig, key=f'plot_entity_type_{suffix}')
        fig = px.bar(result_tags.sort_values('tagCount',ascending=False).iloc[:20],x='tag',y='tagCount',title='Top 20 tags in the comminuty')
        fig.update_xaxes(tickangle=45)
        plotly_chart(fig, key=f'plot_tag_type_{suffix}')
        tag_map: DataFrame = session_state[f'tag_class_mapping_{suffix}']
        classified_tags = result_tags.merge(tag_map,left_on='tag',right_on='tag',how='left')
        col1, col2, col3, col4 = columns(4)
        fig = px.pie(classified_tags,values='tagCount',names='class',title='Tags distribution in the community')
        col1.plotly_chart(fig, key=f'plot_tag_class_{suffix}')
        col2.text('A class tags within cluster')
        col2.dataframe(classified_tags.loc[classified_tags['class'] == 'A'].sort_values('tagCount',ascending=False).drop(columns=['class']))
        col3.text('B class tags within cluster')
        col3.dataframe(classified_tags.loc[classified_tags['class'] == 'B'].sort_values('tagCount',ascending=False).drop(columns=['class']))
        col4.text('C class tags within cluster')
        col4.dataframe(classified_tags.loc[classified_tags['class'] == 'C'].sort_values('tagCount',ascending=False).drop(columns=['class']))
        # col1, col2 = columns([1,3])
        # colors = {'A':'#80C9FF', 'B':'#0066CC','C':'#FF0000'}
        # fig = px.pie(classified_tags,values='tagCount',names='class',title='Tags distribution in the community',
        #              color_discrete_map=colors, color='class')
        # col1.plotly_chart(fig, key=f'plot_tag_class_{suffix}')  
        # fig = px.bar(classified_tags.loc[classified_tags['class'] == 'A'].sort_values('tagCount',ascending=False).iloc[:20]
        #              ,x='tag',y='tagCount',title='Most frequent tags of class A')
        # col2.plotly_chart(fig, key=f'plot_tag_class_A_{suffix}')
        match_stats = analyzer.calcalate_matching_ent_metric(session_state[f"match_conf_{suffix}"],int(choice),session_state[f'key_{suffix}'],mode)
        figure = Figure(data=[Pie(labels=["Matching","Not Matching"], values=list(match_stats),title="Matching ents ratio")])
        plotly_chart(figure)

    entity_choice: str = selectbox(label='Choose entity for connection check', options=session_state[f'entity_list_{suffix}'], key=f'entity_selection_{suffix}')
    if entity_choice is not None:
        name = sub(r'\(.*\)','',entity_choice)
        type_ = entity_choice.replace(name,"",1)
        type_ = type_[1:-1]
        if mode == Mode.articles:
            session_state[f'ents_connection_{suffix}'] = analyzer.analyse_entity_connection_articles(session_state[f'key_{suffix}'],name+'_'+type_,selections)
        else:
            session_state[f'ents_connection_{suffix}'] = analyzer.analyse_entity_connection_entities(session_state[f'key_{suffix}'],name+'_'+type_, selections)
        df_ents : DataFrame = session_state[f'ents_connection_{suffix}']
        df_ents['entity'] = df_ents['name'] + '(' + df_ents['type'] + ')'
        df_ents_matching = df_ents.loc[df_ents['sameCluster'] > 0]
        df_ents_matching['Matching'] = df_ents_matching['sameCluster'] # / df_ents_matching['sameCluster'].sum() * 100
        df_ents_non_matching = df_ents.loc[df_ents['differentCluster'] > 0]
        df_ents_non_matching['Non_matching'] = df_ents_non_matching['differentCluster']# / df_ents_non_matching['differentCluster'].sum() * 100
        df_ents_matching = df_ents_matching.sort_values('Matching',ascending=False)
        write(f'Overall matching connections: {df_ents_matching.shape[0]} with total strength of {df_ents_matching['Matching'].sum()}' )
        figure = px.bar(df_ents_matching[:20], x='entity', y='Matching',labels={'Matching': "Connection Strength", "entity": "Entity Name (Type)"})
        plotly_chart(figure, key=f'20_top_ents_matching_{suffix}')
        df_ents_non_matching = df_ents_non_matching.sort_values('Non_matching',ascending=False)
        write(f'Overall non matching connections: {df_ents_non_matching.shape[0]} with total strength of {df_ents_non_matching['Non_matching'].sum()}' )
        figure = px.bar(df_ents_non_matching[:20],x='entity',y='Non_matching',labels={'Non_matching': "Connection Strength", "entity": "Entity Name (Type)"})
        plotly_chart(figure, key=f'20_top_ents_non_matching_{suffix}')
        figure = px.histogram(df_ents_matching,x='Matching')
        plotly_chart(figure,key=f'ents_matching_hist_{suffix}')
        figure = px.histogram(df_ents_non_matching,x='Non_matching')
        colors = ['#80C9FF', '#0066CC']
        plotly_chart(figure,key=f'ents_non_matching_hist_{suffix}')
        pie_df = DataFrame([{"Match": "Inner-connections", "value": df_ents_matching['Matching'].sum(), "text": f"{df_ents_matching['Matching'].sum()}\n({df_ents_matching.shape[0]})"},
                            {"Match": "External connections", "value": df_ents_non_matching['Non_matching'].sum(), "text": f"{df_ents_non_matching['Non_matching'].sum()}\n({df_ents_non_matching.shape[0]})"}])
        figure = px.pie(pie_df,names='Match',values='value',color=colors)
        figure.update_traces(text=pie_df["text"],textinfo='text')
        plotly_chart(figure, key=f'pie_chart_ents_{suffix}')



        
init()
loader: Neo4jExecutor = session_state['loader']
analyzer: Analyzer = session_state['analyzer']
selections = multiselect('Ask data from json file',loader.get_files(),[])
files_changed_articles, files_changed_ents = has_files_changed(set(selections),'articles'), has_files_changed(set(selections),'entities')
select_btn = button('Select')
entities, clustering_articles, clustering_ents = tabs(['entities', 'clustering - articles', 'clustering - ents'])
with entities:
    if select_btn and len(selections) > 0:
        session_state['ents_all'] = loader.get_ners_count(selections)
    elif select_btn and len(selections) == 0 and 'ents_all' in session_state:
        del session_state['ents_all']
    if 'ents_all' in session_state:
        ents_all = session_state.get('ents_all', DataFrame({k: Series(dtype=v)for k,v in [('entity','str'),('type','str'),('count','int')]})).sort_values('count',ascending=False)
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
        write(ents_all['count'].quantile(.9))
        fig = px.histogram(ents_all.loc[(ents_all['count'] >= selected_min) & (ents_all['count'] <= selected_max)] \
                           .rename(columns={'count': 'Entity Count'}),
                        x='Entity Count',
                        title='Selected slice of histogram of entities count')
        plotly_chart(fig)
        
        entity = multiselect('Write entity you want to search', ents_all['entityName'],disabled=len(ents_all['entityName'])==0,max_selections=1)
        search_button = button('Search',disabled=len(ents_all['entityName'])==0)
        
        if len(entity) == 1 and search_button:
            search_entity = sub(r"\([^\(\)]*\)$","",entity[0])
            entity_type = search(r"(?<=\()[^\(\)]+(?=\)$)",entity[0]).group()
            counts = loader.get_linked_ners(search_entity,entity_type,selections)
            entity_plot(counts)
            category_plot(counts)
            combined_plot(counts)
with clustering_articles:
    calculate_and_show_chart(Mode.articles,files_changed_articles, select_btn)
with clustering_ents:
    calculate_and_show_chart(Mode.entities,files_changed_ents, select_btn)