# DB_Python

This is the project from the lecture ```Database``` in SNU.
Implemented Database Management System in Python.

### Development Environment
- Python 3.12
- Lark API
- Oracle Berkeley DB API (w/o SQL API)

## Run 
First you should install the requirements.

        >> brew install berkeley-db@5
        >> BERKELEYDB_DIR=$(brew --prefix berkeley-db@5) pip install berkeleydb
        >> pip install -r requirements.txt

Execute below from the terminal:

        >> python run.py
