from streamlit import multiselect, session_state, text_input, button, plotly_chart
from shared import init
from loader import Neo4jExecutor
import plotly.express as px
from typing import Dict, Tuple
from pandas import Series

def entity_plot(data: Dict[Tuple[str,str],int]):
    entity_dict = dict()
    for key, value in data.items():
        entity_dict[key[0]] = entity_dict.get(key[0],0) + value
    dfs = Series(entity_dict)
    dfs.sort_values(inplace=True,ascending=False)
    fig = px.bar(dfs.iloc[:20])
    plotly_chart(fig)
    
def category_plot(data: Dict[Tuple[str,str],int]):
    category_dict = dict()
    for key, value in data.items():
        category_dict[key[1]] = category_dict.get(key[1],0) + value
    dfs = Series(category_dict)
    dfs.sort_values(inplace=True,ascending=False)
    fig = px.bar(dfs.iloc[:20])
    plotly_chart(fig)
    
def combined_plot(data: Dict[Tuple[str,str],int]):
    dfs = Series({f"{key[0]}({key[1]})": val for key, val in data.items()})
    print(dfs.head())
    dfs.sort_values(inplace=True,ascending=False)
    fig = px.bar(dfs.iloc[:20])
    plotly_chart(fig)

init()
db_driver: Neo4jExecutor = session_state['db_driver']
selections = multiselect('Ask data from json file',db_driver.get_files(),[])
select_btn = button('Select')
if select_btn:
    session_state['ents'] = db_driver.get_all_ners(selections)
entity = multiselect('Write entity you want to search', session_state.get('ents',[]),disabled=len(session_state.get('ents',[]))==0,max_selections=1)
search_button = button('Search',disabled=len(session_state.get('ents',[]))==0)
if len(entity) == 1 and search_button:
    counts = db_driver.get_linked_ners(entity[0],selections)
    entity_plot(counts)
    category_plot(counts)
    combined_plot(counts)
    