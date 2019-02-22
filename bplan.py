#!/usr/bin/python
import csv
from datetime import datetime
import functools
import gzip
import itertools
import json
import os
from pathlib import Path
import re
import sqlite3

record_fields = {
    'PIF': ['version', 'source_system', 'toc', 'start_date', 'end_date',
            'cycle_type', 'cycle_stage', 'creation_date', 'sequence_number'],
    'REF': ['type', 'code', 'description'],
    'TLD': ['traction', 'trailing_load', 'speed', 'ra_gauge', 'description',
            'itps_power_type', 'itps_load', 'limiting_speed'],
    'LOC': ['tiploc', 'name', 'start_date', 'end_date', 'easting', 'northing',
            'timing_point_type', 'zone', 'stanox', 'off_network_indicator', 'force_lpb'],
    'PLT': ['tiploc', 'platform_id', 'start_date', 'end_date', 'length',
            'power_supply', 'doo_passenger', 'doo_non_passenger'],
    'NWK': ['origin_location', 'destination_location', 'running_line_code',
            'running_line_desc', 'start_date', 'end_date', 'initial_direction',
            'final_direction', 'distance', 'doo_passenger', 'doo_non_passenger',
            'retb', 'zone', 'reversible_line', 'power_supply', 'ra', 'maximum_train_length'],
    'TLK': ['origin_location', 'destination_location', 'running_line_code',
            'traction', 'trailing_load', 'speed', 'ra_gauge', 'entry_speed',
            'exit_speed', 'start_date', 'end_date', 'sectional_running_time', 'description']
}

def create_db(dbpath):
    db = sqlite3.connect(dbpath)

    with db:
        # Initialse spatialite
        db.enable_load_extension(True)
        db.load_extension('mod_spatialite')
        db.execute('SELECT InitSpatialMetadata(1)')

        # BPLAN Database Schema
        db.executescript('''
            CREATE TABLE REF (
              type TEXT,
              code TEXT,
              description TEXT,
              type_code_type TEXT DEFAULT 'REF',
              PRIMARY KEY (type, code),
              FOREIGN KEY (type_code_type, type) REFERENCES REF
            );
            CREATE TABLE TLD (
              traction TEXT,
              trailing_load TEXT,
              speed INTEGER,
              ra_gauge TEXT,
              description TEXT,
              itps_power_type TEXT,
              itps_load TEXT,
              limiting_speed INTEGER,
              PRIMARY KEY (traction, trailing_load, speed, ra_gauge)
            );
            CREATE TABLE LOC (
              tiploc TEXT PRIMARY KEY,
              name TEXT,
              start_date TEXT,
              end_date TEXT,
              easting INTEGER,
              northing INTEGER,
              timing_point_type TEXT,
              zone TEXT,
              stanox INTEGER,
              off_network_indicator TEXT,
              force_lpb TEXT,
              zone_ref_type TEXT DEFAULT 'ZNE',
              FOREIGN KEY (zone_ref_type, zone) REFERENCES REF
            );
            CREATE TABLE PLT (
              tiploc TEXT,
              platform_id TEXT,
              start_date TEXT,
              end_date TEXT,
              length INTEGER,
              power_supply TEXT,
              doo_passenger TEXT,
              doo_non_passenger TEXT,
              power_supply_ref_type TEXT DEFAULT 'PWR',
              PRIMARY KEY (tiploc, platform_id),
              FOREIGN KEY (tiploc) REFERENCES LOC,
              FOREIGN KEY (power_supply_ref_type, power_supply) REFERENCES REF
            );
            CREATE TABLE NWK (
              origin_location TEXT,
              destination_location TEXT,
              running_line_code TEXT,
              running_line_desc TEXT,
              start_date TEXT,
              end_date TEXT,
              initial_direction TEXT,
              final_direction TEXT,
              distance INTEGER,
              doo_passenger TEXT,
              doo_non_passenger TEXT,
              retb TEXT,
              zone TEXT,
              reversible_line TEXT,
              power_supply TEXT,
              ra TEXT,
              maximum_train_length INTEGER,
              zone_ref_type TEXT DEFAULT 'ZNE',
              power_supply_ref_type TEXT DEFAULT 'PWR',
              PRIMARY KEY (origin_location, destination_location, running_line_code),
              FOREIGN KEY (origin_location) REFERENCES LOC,
              FOREIGN KEY (destination_location) REFERENCES LOC,
              FOREIGN KEY (zone_ref_type, zone) REFERENCES REF,
              FOREIGN KEY (power_supply_ref_type, power_supply) REFERENCES REF
            );
            CREATE TABLE TLK (
              origin_location TEXT,
              destination_location TEXT,
              running_line_code TEXT,
              traction TEXT,
              trailing_load TEXT,
              speed INTEGER,
              ra_gauge TEXT,
              entry_speed INTEGER,
              exit_speed INTEGER,
              start_date TEXT,
              end_date TEXT,
              sectional_running_time TEXT,
              description TEXT,
              PRIMARY KEY (origin_location, destination_location, running_line_code, traction,
              trailing_load, speed, ra_gauge, entry_speed, exit_speed, start_date),
              FOREIGN KEY (origin_location) REFERENCES LOC,
              FOREIGN KEY (destination_location) REFERENCES LOC,
              FOREIGN KEY (origin_location, destination_location, running_line_code) REFERENCES NWK,
              FOREIGN KEY (traction, trailing_load, speed, ra_gauge) REFERENCES TLD
            );
        ''')

        # Spatial Columns
        db.execute("SELECT AddGeometryColumn('LOC', 'geom', 27700, 'POINT', 2);")
        db.execute('''
          CREATE TRIGGER insert_LOC_geom AFTER INSERT ON LOC
            WHEN NEW.easting != 0 AND NEW.easting != 999999
            AND  NEW.northing != 0 AND NEW.northing != 999999 BEGIN
          UPDATE LOC SET geom = MakePoint(easting, northing, 27700) WHERE rowid = NEW.rowid; END;
        ''')
        db.execute('''
          CREATE TRIGGER update_LOC_geom AFTER UPDATE OF easting, northing ON LOC
            WHEN NEW.easting != 0 AND NEW.easting != 999999
            AND  NEW.northing != 0 AND NEW.northing != 999999 BEGIN
          UPDATE LOC SET geom = MakePoint(easting, northing, 27700) WHERE rowid = NEW.rowid; END;
        ''')

    return db

def open_bplan(path):
    if path.suffix == '.gz':
        return gzip.open(path, 'rt', encoding='windows-1252')
    else:
        return open(path, 'r', encoding='windows-1252')

class BPlanDialect(csv.Dialect):
    delimiter = '\t'
    lineterminator = '\r\n'
    quoting = csv.QUOTE_NONE
    strict = True

def process_bplan(bplanfile, db):
    reader = csv.reader(bplanfile, dialect=BPlanDialect)
    metadata = {}
    counts = {}
    with db:
        for ((rec, action), rows) in itertools.groupby(reader, key=lambda r: r[0:2]):
            if rec == 'PIF':
                row = row_parse_function(rec, drop=1)(next(rows))
                for k, v in enumerate(record_fields[rec]):
                    metadata[v] = row[k]
            elif rec == 'PIT':
                row = next(rows)
                for i in range(1, len(row), 4):
                    actual = counts[row[i]]
                    expected = int(row[i+1])
                    if actual != expected:
                        raise Exception("Inconsistent %s counts, expected %s, got %s" %
                                (row[i], expected, actual))
                    elif int(row[i+2]) != 0 or int(row[i+3]) != 0:
                        raise Exception("Expected unsupported record update types.")

                metadata['record_count'] = counts

            else:
                if action == 'A':
                    c = db.executemany(insert_statement(rec), map(row_parse_function(rec), rows))
                    counts[rec] = c.rowcount
                else:
                    raise NotImplementedError

    return metadata

def insert_statement(rec):
    if rec not in record_fields:
        raise NotImplementedError
    fields = record_fields[rec]
    return 'INSERT INTO %s (%s) VALUES (%s)' % (rec, ','.join(fields), ','.join(['?']*len(fields)))

def row_parse_function(rec, drop=2):
    date_fields = []
    for k, v in enumerate(record_fields[rec]):
        if v.endswith('_date'):
            date_fields.append(k)

    def f(row):
        # Drop record type and action
        row = row[drop:]

        for k in date_fields:
            try:
                row[k] = datetime.strptime(row[k], '%d-%m-%Y %H:%M:%S')
            except ValueError as err:
                if row[k] != '':
                    raise err
                row[k] = None
        return row

    return f

def metadata_file_template():
    return {
        "title": "Network Rail Open Data Reference Databases",
        "description": "Reference data used by Network Rail for planning purposes",
        "license": "Network Rail Infrastructure Ltd Data Feeds Licence",
        "license_url": "https://www.networkrail.co.uk/who-we-are/transparency-and-ethics/transparency/open-data-feeds/network-rail-infrastructure-limited-data-feeds-licence/",
        "source": "Network Rail Infrastructure Ltd"
    }

def generate_metadata(item):
    return {
        "title": f"BPLAN {item['start_date']:%B %Y}",
        "description": (
            f"BPLAN database valid for the timetable period: {item['start_date']:%-d %B %Y} to "
            f"{item['end_date']:%-d %B %Y}. Database published: {item['creation_date']:%-d %B %Y}, "
            f"by: {item['toc']}, source system: {item['source_system']}."
        ),
        "source_url": "https://wiki.openraildata.com/index.php?title=BPLAN_Geography_Data",
        "tables": {
            "REF": {"description": "Reference Codes"},
            "LOC": {"description": "Locations"},
            "PLT": {"description": "Platforms and Sidings"},
            "NWK": {"description": "Network Links"},
            "TLD": {"description": "Timing Loads"},
            "TLK": {"description": "Timing Links"}
        }
    }

def load_metadata(path):
    try:
        with path.open() as f:
            metadata = json.load(f)
    except:
        metadata = metadata_file_template()

    if 'databases' not in metadata:
        metadata['databases'] = {}

    return metadata

if __name__ == "__main__":
    if len(os.sys.argv) < 2:
        print("Usage: bplan.py bplan-file[.gz] [...]")
        print("Note: bplan-file.sqlite will be overwritten.")
        os.sys.exit(0)

    metadata_file = Path('metadata.json')
    metadata = load_metadata(metadata_file)

    for f in os.sys.argv[1:]:
        try:
            bplan_path = Path(f)
            db_path = bplan_path.with_suffix('.sqlite')
            metadata_path = bplan_path.with_name('metadata.json')

            bplan = open_bplan(bplan_path)

            try:
                os.remove(db_path)
            except:
                pass

            db = create_db(db_path)
            db_metadata = process_bplan(bplan, db)
            metadata['databases'][bplan_path.stem] = generate_metadata(db_metadata)
        except Exception as e:
            print(f"Unable to process {f}:", file=os.sys.stderr)
            print(e, file=os.sys.stderr)

    with metadata_file.open('w') as f:
        json.dump(metadata, f)
