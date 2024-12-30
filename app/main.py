from streamlit import write, markdown
from shared import init

if __name__ == '__main__':
    init()
    write('# NER Analysis tool in Graph Database')
    markdown('''This tool was design to do three major things:
1. Load extracted articles in json format.
2. Extract Named Entities from them.
3. Load them into Neo4j database to create graph.
4. Analyse the graph by choosing Named Entities and connections.''')