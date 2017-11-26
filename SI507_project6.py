# Import statements
import psycopg2
import psycopg2.extras
from psycopg2 import sql
from config import *
import sys
import csv 


# Write code / functions to set up database connection and cursor here.
db_connection, db_cursor = None, None

def get_connection_and_cursor():
    global db_connection, db_cursor
    if not db_connection:
        try:
            if db_password != "":
                db_connection = psycopg2.connect("dbname='{0}' user='{1}' password='{2}'".format(db_name, db_user, db_password))
                print("Success connecting to database")
            else:
                db_connection = psycopg2.connect("dbname='{0}' user='{1}'".format(db_name, db_user))
        except:
            print("Unable to connect to the database. Check server and credentials.")
            sys.exit(1) 

    if not db_cursor:
        db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    return db_connection, db_cursor

conn, cur = get_connection_and_cursor()


# Write code / functions to create tables with the columns you want and all database setup here.
def setup_database():
    cur.execute('DROP TABLE IF EXISTS "Sites"')
    cur.execute('DROP TABLE IF EXISTS "States"')

    cur.execute(""" CREATE TABLE IF NOT EXISTS "States"(
        "ID" SERIAL PRIMARY KEY,
        "Name" VARCHAR(40) NOT NULL UNIQUE
    )""")

    cur.execute(""" CREATE TABLE IF NOT EXISTS "Sites"(
        "ID" SERIAL PRIMARY KEY,
        "Name" VARCHAR(128) NOT NULL UNIQUE,
        "Type" VARCHAR(128),
        "State_ID" INTEGER REFERENCES "States"("ID"),
        "Location" VARCHAR(255),
        "Description" TEXT
    )""")

    conn.commit()
    print('Setup database complete')



# Write code / functions to deal with CSV files and insert data into the database here.

def insert(connection, cursor, table, data_dict, no_return = True):
    column_names = data_dict.keys()

    if not no_return:
        query = sql.SQL('INSERT INTO "{0}"({1}) VALUES({2}) ON CONFLICT DO NOTHING RETURNING "ID"').format(
            sql.SQL(table),
            sql.SQL(', '). join(map(sql.Identifier, column_names)),
            sql.SQL(', ').join(map(sql.Placeholder, column_names))
        )

    else:
        query = sql.SQL('INSERT INTO "{0}"({1}) VALUES({2}) ON CONFLICT DO NOTHING').format(
            sql.SQL(table),
            sql.SQL(', '). join(map(sql.Identifier, column_names)),
            sql.SQL(', ').join(map(sql.Placeholder, column_names))
        )

    sql_string = query.as_string(connection)
    #print(sql_string)
    cursor.execute(sql_string, data_dict)

    if not no_return:
        print (cursor.fetchone()["ID"])



def get_site_dict(site_list,state_id):
    return {
    "State_ID" : state_id,
    "Name" : site_list[0],
    "Type" : site_list[2],
    "Location" : site_list[1],
    "Description" : site_list[4]
    }

def get_state_id(state_name):
    cur.execute(""" SELECT "ID" FROM "States" WHERE "Name"=%s""", (state_name,))
    result = cur.fetchone()
    state_id = result["ID"]
    return state_id


# Make sure to commit your database changes with .commit() on the database connection.
# Write code to be invoked here (e.g. invoking any functions you wrote above)

if __name__ == '__main__':
    
    setup_database()

    # insert States data
    cur.execute("""INSERT INTO "States" ("Name") VALUES('arkansas')""")
    cur.execute(""" INSERT INTO "States" ("Name") VALUES('california') """)
    cur.execute(""" INSERT INTO "States" ("Name") VALUES('michigan') """)
    conn.commit()


    #Insert Sites data
    csv_list = ['arkansas.csv', 'california.csv', 'michigan.csv']
    for csv_file in csv_list:
        state_name = csv_file.split('.')[0]
        #print(state_name)
        state_id = get_state_id(state_name)
        with open(csv_file, 'r') as f:
            reader = csv.reader(f)
            first_row = next(reader)
            for row in reader:
                site_dict = get_site_dict(row, state_id)
                insert(conn, cur, "Sites", site_dict)

    conn.commit()


#query the database for all of the locations of the sites. Save the resulting data in a variable called all_locations.
print("---Query the database for all of the locations of the sites.---")
cur.execute("""SELECT "Location" FROM "Sites" """)
all_locations = cur.fetchall()
print(all_locations)

#query the database for all of the names of the sites whose descriptions include the word beautiful. Save the resulting data in a variable called beautiful_sites.
print("---Query the database for the total number of sites whose type is National Lakeshore---")
cur.execute("""SELECT "Name" FROM "Sites" WHERE "Description" ILIKE '%beautiful%' """)
beautiful_sites = cur.fetchall()
print(beautiful_sites)

#query the database for the total number of sites whose type is National Lakeshore. Save the resulting data in a variable called natl_lakeshores.
print("---Query your database for the names of all the national sites in Michigan---")
cur.execute("""SELECT COUNT(*) FROM "Sites" WHERE "Type" = 'National Lakeshore' """)
natl_lakeshores = cur.fetchall()
print(natl_lakeshores)

#query your database for the names of all the national sites in Michigan. Save the resulting data in a variable called michigan_names
print("---Query your database for the names of all the national sites in Michigan---")
cur.execute("""SELECT "Sites"."Name" FROM "Sites" INNER JOIN "States" ON ("Sites"."State_ID" = "States"."ID") WHERE "States"."Name"='michigan' """)
michigan_names = cur.fetchall()
print(michigan_names)

#query your database for the total number of sites in Arkansas. Save the resulting data in a variable called total_number_arkansas.
print("---Query your database for the total number of sites in Arkansas---")
cur.execute(""" SELECT COUNT(*) FROM "Sites" INNER JOIN "States" ON ("Sites"."State_ID" = "States"."ID") WHERE "States"."Name"='arkansas' """)
total_number_arkansas = cur.fetchall()
print(total_number_arkansas)

# We have not provided any tests, but you could write your own in this file or another file, if you want.




