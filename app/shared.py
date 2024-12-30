from streamlit import session_state
from loader import Neo4jExecutor
from dotenv import load_dotenv
from spacy_download import load_spacy

def init():
    load_dotenv()

    if 'db_driver' not in session_state:
        session_state['db_driver'] = Neo4jExecutor()
    if 'nlp' not in session_state:
        try:
            import torch
            import spacy
            print(torch.cuda.is_available())
            spacy.require_gpu()
        except ImportError:
            print('No GPU support, using cpu only')
        session_state['nlp'] = load_spacy("pl_core_news_lg")