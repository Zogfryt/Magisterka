# How to run

1. Using [pyenv](https://github.com/pyenv/pyenv) activate the shell with python >3.9.0,<3.13. (without support for 3.9.7 because of streamlit)
```
pyenv shell <python_version>
```
If you doesn't have one, you can download it using
```
pyenv install <python_version>
```

2. Download [poetry](https://python-poetry.org/). Firstly set poetry to create local project environment.
```
poetry config virtualenvs.in-project true
```
Then in project root folder create new environment using.
```
poetry install --with cuda
```

If you don't have nvidia GPU, you can use
```
poetry install --without cuda
```

This will install spacy on CPU. Unfortunately it will be much slower.

3. Create **.env** file in root folder with following specification:
```
DATABASE_URL=neo4j://localhost:7687
DATABASE_USR=neo4j
DATABASE_PASSWORD=AGH_2025
```
4. Run Neo4j database using (First you have to comment streamlit container creation -- will be fixed next update)
```
docker compose up
```

5. Activate poetry environment using:
```
poetry shell
```
Then run NER loading page with
```
streamlit run ./app/main.py
```

After those steps you will be able to use the app from localhost and port specified by streamlit.

# Running only docker

1. Create **.env** file in app folder with following specification:
```
DATABASE_URL=neo4j://172.25.0.2:7687
DATABASE_USR=neo4j
DATABASE_PASSWORD=AGH_2025
```

2. Build containers
```
docker compose up
```

This will spawn both streamlit and neo4j containers. This is solution **without** cuda support.
