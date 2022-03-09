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


# Function that creaes a table
def create_table(cursor, data, table_name):

    creat_comics = "CREATE TABLE `cars` (" \
                 "  `id` int(11) NOT NULL," \
                 "  `name` varchar(64) NOT NULL," \
                 "  `year` int(4) NOT NULL," \
                 "  `artist` varchar(64) NOT NULL," \
                 "  `writer` SMALLINT NOT NULL," \
                 "  PRIMARY KEY (`id`)" \
                 ") ENGINE=InnoDB"


        cursor.execute(new_table)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")
    return


# Reading csv and preparing them for sql
def readfile(list, path):
    with open(path, "r", encoding='utf-8') as csvreader:
        csvfile = csv.reader(csvreader, delimiter=",")
        temp_list = []
        for row in csvfile:
            temp_list.append(row)
            new_string = "VALUES ("
            for i in range(len(row) - 1):
                new_string += "'" + row[i].replace("'", "''") + "', "

            new_string += "'" + row[len(row) - 1].strip("\n") + "');"
            list.append(new_string)
        # I set the first row to be the data of each columns
        list[0] = temp_list[0]
        csvreader.close()
    return


# Function that inserts the values in the given table
def insert_into(cursor, cnx, data, table_name):

    # Creates the insert line
    temp_list = data
    insert = "INSERT INTO " + table_name + " ("
    for i in range(len(data[0]) - 1):
        insert += data[0][i] + ", "
    insert += data[0][-1] + ")"

    # remove the first value, cause it is not used
    del temp_list[0]
    insert_sql = []
    for row in temp_list:
        new_string = insert + " " + row
        insert_sql.append(new_string)

    # executing in sql
    for query in insert_sql:
        try:
            print("SQL query {}: ".format(query), end='')
            cursor.execute(query)
        except mysql.connector.Error as err:
            print(err.msg)
        else:
            # Make sure data is committed to the database
            cnx.commit()
            print("OK")
    return


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

        # Creating a table of planets in db
        # data = []
        # readfile(data, "planets.csv")
        create_table(cursor, data, "planets")
        insert_into(cursor, cnx, data, "planets")

    cnx.close()
    cursor.close()
    return


main()
