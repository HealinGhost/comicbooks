import mysql.connector
import csv
from mysql.connector import errorcode

# Constants
DB_NAME = 'Marvel'


# Function that creates a database
def create_database(cursor, DB_NAME):
    try:
        cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Faild to create database {}".format(err))
        exit(1)
    return


# Function that creates all the tables
def create_tables(cursor):

    tables = []

    create_comics = "CREATE TABLE `comic_books` (" \
                    "  `id` int(11) NOT NULL," \
                    "  `name` varchar(64) NOT NULL," \
                    "  `year` int(5) NOT NULL," \
                    "  `artist` varchar(64) NOT NULL," \
                    "  `writer` varchar(64) NOT NULL," \
                    "  PRIMARY KEY (`id`)" \
                    ") ENGINE=InnoDB"

    tables.append(create_comics)

    create_movies = "CREATE TABLE `movies` (" \
                    "  `id` int(11) NOT NULL," \
                    "  `title` varchar(64) NOT NULL," \
                    "  `director` varchar(64) NOT NULL," \
                    "  `release_year` int(5) NOT NULL," \
                    "  `comic_id` int(11) NULL," \
                    "  PRIMARY KEY (`id`)" \
                    ") ENGINE=InnoDB"

    tables.append(create_movies)

    create_participants = "CREATE TABLE `participants` (" \
                          "  `comic_id` int(11) NOT NULL," \
                          "  `character_id` int(11) NOT NULL" \
                          ") ENGINE=InnoDB"

    tables.append(create_participants)

    create_characters = "CREATE TABLE `characters` (" \
                        "  `id` int(11) NOT NULL," \
                        "  `name` varchar(64) NOT NULL," \
                        "  `side` varchar(64) NOT NULL," \
                        "  `super_power` varchar(255) NOT NULL," \
                        "  `release_year` int(5) NOT NULL," \
                        "  PRIMARY KEY (`id`)" \
                        ") ENGINE=InnoDB"

    tables.append(create_characters)

    # loops through all the queries
    for table in tables:

        try:
            cursor.execute(table)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")

    return


# Reading csv and preparing them for sql
def readfile(cursor, cnx, path, table_name):
    print("Reading data for " + table_name)
    with open(path, "r") as csvfile:
        csvdata = csv.reader(csvfile, delimiter=",")
        line_count = 0
        for line in csvdata:
            if line_count == 0:
                line_count += 1
            else:
                try:
                    cursor.execute(insert_into_query(line, table_name))
                    cnx.commit()
                except mysql.connector.Error as err:
                    print(err)

    return


# Function that turns a list into a query
def insert_into_query(row, table_name):
    query = "INSERT INTO " + table_name + " VALUES ("
    for i in range(len(row) - 1):
        if row[i] == "NA":
            query += "NULL, "
        else:
            query += "'" + row[i] + "', "
    if row[len(row) - 1] == "NA":
        query += "NULL)"
    else:
        query += "'" + row[len(row) - 1] + "')"
    return query


# Main
def main():

    cnx = mysql.connector.connect(user='root',
                                  password='root',
                                  host='localhost',
                                  unix_socket='/Applications/MAMP/tmp/mysql/mysql.sock')
    cursor = cnx.cursor()

    try:
        cursor.execute("USE {}".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Database {} does not exist".format(DB_NAME))

        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cursor, DB_NAME)
            print("Database {} created succesfully.".format(DB_NAME))
            cnx.database = DB_NAME

    create_tables(cursor)
    readfile(cursor, cnx, "MarvelCharacters.csv", "characters")
    readfile(cursor, cnx, "MarvelComicBooks.csv", "comic_books")
    readfile(cursor, cnx, "MarvelMovies.csv", "movies")
    readfile(cursor, cnx, "MarvelParticipants.csv", "participants")

    cnx.close()
    cursor.close()
    return


main()
