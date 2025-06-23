from streamlit import file_uploader, status, button, selectbox, session_state, rerun, toggle, dataframe
from parser import json_to_dict, get_ners, json_with_ner_to_dict, toml_to_config
from shared import init
from loader import Neo4jExecutor
from tqdm import tqdm
from collapser import create_similarity_links

def load_data_action(content: str, conf_content: str, filename: str, ner_format: bool):
    loader: Neo4jExecutor = session_state['loader']
    status_ = status('Loading data, please wait',expanded=True)
    status_.write('Extracting configuration from toml...')
    matches, blacklist, dictionary = toml_to_config(conf_content)
    status_.write('Extracting information from json...')
    if ner_format:
        documents = json_with_ner_to_dict(content, blacklist)
    else:
        documents = json_to_dict(content)
    status_.write('Extracting Named Entities')
    for doc in tqdm(documents):
        if len(doc.entities) == 0:
            doc.entities = get_ners(doc, session_state['nlp'], dictionary,blacklist)
    non_matching = loader.check_ent_types_integrity(matches,documents)
    if len(non_matching) == 0:
        status_.write('Sending to database...')
        loader.load_data(documents, filename)
        status_.write('Saving Configuration')
        loader.save_matches_config(matches,filename.replace('.json','.toml'))
        status_.update(label='Loading complete!', state='complete', expanded=False)
    else:
        status_.update(label=f'Loading failed! There are types in "matches" section in your configuration that are not in detected types. You need to add: {non_matching}', state='complete', expanded=False)
    

init()
loader: Neo4jExecutor = session_state['loader']
toggle_with_ner = toggle('Format with extracted NERs',value=False)
file = file_uploader(label='Drop here scraped web database', type='json', accept_multiple_files=False)
conf_file = file_uploader(label='Drop here toml file pointing appropriate entity types', type='toml', accept_multiple_files=False)
load_button = button('Load')
delete_choice = selectbox('Remove json file form db', loader.get_files(),index=None)
delete_button = button('Delete')
if file is not None and conf_file is not None and load_button:
    load_data_action(content = file.getvalue().decode('utf-8'),
                    conf_content = conf_file.getvalue().decode('utf-8'),
                    filename=file.name,
                    ner_format=toggle_with_ner)
if delete_choice is not None and delete_button:
    loader.delete_json(delete_choice)
    rerun()
  
        
        