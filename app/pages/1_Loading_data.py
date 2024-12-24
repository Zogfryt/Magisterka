from streamlit import file_uploader, status, button, selectbox, session_state, rerun
from parser import json_to_dict, get_ners
from shared import init
from loader import Neo4jExecutor
from tqdm import tqdm


def load_data_action(content: str, filename: str):
    db_driver: Neo4jExecutor = session_state['db_driver']
    status_ = status('Loading data, please wait',expanded=True)
    status_.write('Extracting information from json...')
    documents = json_to_dict(content)
    status_.write('Extracting Named Entities')
    for doc in tqdm(documents):
        doc.entities = get_ners(doc)
    status_.write('Sending to database...')
    db_driver.load_data(documents, filename)
    status_.update(label='Loading complete!', state='complete', expanded=False)
    rerun()
    

init()
db_driver: Neo4jExecutor = session_state['db_driver']
file = file_uploader(label='Drop here scraped web database', type='json', accept_multiple_files=False)
load_button = button('Load')
delete_choice = selectbox('Remove json file form db', db_driver.get_files(),index=None)
delete_button = button('Delete')
if file is not None and load_button:
    content = file.getvalue().decode('utf-8')
    load_data_action(content, file.name)
if delete_choice is not None and delete_button:
    db_driver.delete_json(delete_choice)
    rerun()
    
    

    
        
        
        