# How to run

1. Download poetry and start repository in main folder using
```
poetry install
```
2. Create **.env** file in root folder with following specification:
```
DATABASE_URL=neo4j://localhost:7687
DATABASE_USR=neo4j
DATABASE_PASSWORD=AGH_2025
```
3. Run Neo4j database using 
```
docker compose up
```

4. Run NER loading page with
```
streamlit run ./app/main.py
```

After those steps you will be able to use the app from localhost and port specified by streamlit.