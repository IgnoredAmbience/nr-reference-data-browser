# Network Rail Reference Data Browser

Project to enable easy browsing of various Open Rail Data reference datasets.

Initially, this project uses [Datasette](https://github.com/simonw/datasette) to provide a quick and
easy frontend to query the datasets.
Tools are provided to convert various NR data formats into sqlite databases for use with Datasette.

## Installation and Usage
1. Python, SQLite and SpatiaLite are native dependencies.
2. `pip install datasette`
3. `./bplan.py bplanfile`
4. `datasette bplanfile.sqlite --load-extension /usr/lib/mod_spatialite.so -m metadata.json`
5. Open http://localhost:8001/
