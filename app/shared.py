from streamlit import session_state
from loader import Neo4jExecutor
from spacy import load
from clustering import GraphClusterer
from os import getenv
from neo4j import GraphDatabase
from graphdatascience import GraphDataScience
from community_analyser import Analyzer

def init():
    if 'db_driver' not in session_state:
        session_state['db_driver'] = GraphDatabase.driver(getenv('DATABASE_URL'),auth=(getenv('DATABASE_USR'),getenv('DATABASE_PASSWORD'))) 
    if 'gds_driver' not in session_state:
        session_state['gds_driver'] =  GraphDataScience(getenv('DATABASE_URL'),auth=(getenv('DATABASE_USR'),getenv('DATABASE_PASSWORD')))
    if 'loader' not in session_state:
        session_state['loader'] = Neo4jExecutor(session_state['db_driver'])
    if 'cluster_driver' not in session_state:
        session_state['cluster_driver'] = GraphClusterer(session_state['gds_driver'])
    if 'analyzer' not in session_state:
        session_state['analyzer'] = Analyzer(session_state['db_driver'], session_state['gds_driver'])
    if 'nlp' not in session_state:
        try:
            import torch
            import spacy
            print(torch.cuda.is_available())
            spacy.require_gpu()
        except ImportError:
            print('No GPU support, using cpu only')
        session_state['nlp'] = load('pl_core_news_lg')