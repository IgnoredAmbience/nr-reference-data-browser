#!/usr/bin/python
import csv
from datetime import datetime
import functools
import gzip
import itertools
import os
from pathlib import PurePath
import re
import sqlite3

record_fields = {
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

class BPlanDialect(csv.Dialect):
    delimiter = '\t'
    lineterminator = '\r\n'
    quoting = csv.QUOTE_NONE
    strict = True

def open_bplan(path):
    if path.suffix == '.gz':
        return gzip.open(path, 'r', encoding='windows-1252')
    else:
        return open(path, 'r', encoding='windows-1252')

def process_bplan(bplanfile, db):
    reader = csv.reader(bplanfile, dialect=BPlanDialect)
    with db:
        for ((rec, action), rows) in itertools.groupby(reader, key=lambda r: r[0:2]):
            if rec == 'PIF':
                pass
            elif rec == 'PIT':
                pass
            else:
                if action == 'A':
                    db.executemany(insert_statement(rec), map(row_parse_function(rec), rows))
                else:
                    raise NotImplementedError

def insert_statement(rec):
    fields = record_fields[rec]
    return 'INSERT INTO %s (%s) VALUES (%s)' % (rec, ','.join(fields), ','.join(['?']*len(fields)))

def row_parse_function(rec):
    date_fields = []
    for k, v in enumerate(record_fields[rec]):
        if v.endswith('_date'):
            date_fields.append(k)

    def f(row):
        # Drop record type and action
        row = row[2:]

        for k in date_fields:
            try:
                row[k] = datetime.strptime(row[k], '%d-%m-%Y %H:%M:%S')
            except ValueError as err:
                if row[k] != '':
                    raise err
                row[k] = None
        return row

    return f

if __name__ == "__main__":
    if len(os.sys.argv) < 2:
        print("Usage: bplan.py bplan-file[.gz]")
        print("Note: bplan-file.sqlite will be overwritten.")
        os.sys.exit(0)

    bplan_path = PurePath(os.sys.argv[1])
    db_path = bplan_path.with_suffix('sqlite')

    bplan = open_bplan(bplan_path)

    try:
        os.remove(db_path)
    except:
        pass

    db = create_db(db_path)
    process_bplan(bplan, db)
