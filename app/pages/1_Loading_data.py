from streamlit import file_uploader, status, button, selectbox, session_state, rerun, toggle, dataframe
from parser import json_to_dict, get_ners, json_with_ner_to_dict
from shared import init
from loader import Neo4jExecutor
from tqdm import tqdm
from collapser import create_similarity_links


def load_data_action(content: str, filename: str, ner_format: bool):
    loader: Neo4jExecutor = session_state['loader']
    status_ = status('Loading data, please wait',expanded=True)
    status_.write('Extracting information from json...')
    if ner_format:
        documents = json_with_ner_to_dict(content)
    else:
        documents = json_to_dict(content)
    status_.write('Extracting Named Entities')
    for doc in tqdm(documents):
        if len(doc.entities) == 0:
            doc.entities = get_ners(doc, session_state['nlp'])
    status_.write('Calculating distances')
    vectors = create_similarity_links(documents)
    status_.write('Sending to database...')
    loader.load_data(documents, vectors, filename)
    status_.update(label='Loading complete!', state='complete', expanded=False)
    rerun()
    

init()
loader: Neo4jExecutor = session_state['loader']
toggle_with_ner = toggle('Format with extracted NERs',value=False)
file = file_uploader(label='Drop here scraped web database', type='json', accept_multiple_files=False)
load_button = button('Load')
delete_choice = selectbox('Remove json file form db', loader.get_files(),index=None)
delete_button = button('Delete')
if file is not None and load_button:
    content = file.getvalue().decode('utf-8')
    load_data_action(content, file.name,toggle_with_ner)
if delete_choice is not None and delete_button:
    loader.delete_json(delete_choice)
    rerun()
  
        
        