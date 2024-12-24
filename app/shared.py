from streamlit import session_state
from loader import Neo4jExecutor
from dotenv import load_dotenv


def init():
    load_dotenv()
    if 'db_driver' not in session_state:
        session_state['db_driver'] = Neo4jExecutor()