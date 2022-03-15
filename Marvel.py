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


def gui(cursor):
    response = "Y"
    while(response.upper() == "Y"):
        # The list of operations for the main menu.
        print("Main Menu")
        print("1.Print the characters of the comic book depending on the comic book id given")
        print("2.Print the characters (having at least one comic book appearance) and their number of appearances")
        print("3.Print the super power of the character given from the view created called allPowers")
        print("4.Print the comic books where the wanted character appears")
        print("5.Show what comic books some movies are inspired from")
        print("6.Print the anticipated marvel characters for the movies in the database based on the comic books")
        print("7.Show the average release year and the number of characters of each side")

        result = input("Enter: ")

        # Prints a part of the characters of the comic book depending on the comic book id given.
        if result == "1":
            print("1. Symbiote Spider-Man Vol 1 #5\
                   \n2. Amazing Spider-Man Vol 1 #194 \
                   \n3. Doctor Strange (2015) #389 \
                   \n4. The Infinity Gauntlet #1\
                   \n5. Tales of Suspense Vol 1 #39\
                   \n6. Incredible Hulk Vol 1 #181\
                   \n7. Civil War: Front Line Vol 1 #1\
                   \n8. Amazing Fantasies Vol 1 #15\
                   \n9. Iron Man Vol 4 #4\
                   \n10. Avengers Vol 1 #1")
            answer = str(input("Enter the id of the comic book wanted: "))
            cursor.execute("SELECT ch.name FROM participants p \
            INNER JOIN comic_books co ON co.id = p.comic_id \
            INNER JOIN characters ch ON ch.id = p.character_id \
            WHERE p.comic_id = " + answer)
            for i in cursor:
                print(i)
            response = input("To go back to main menu, press Y: ")
            print("")

        # Prints the characters with at least once appearance in the comic books inserted and the number of appearances.
        elif result == "2":
            cursor.execute("SELECT ch.name, count(ch.id) FROM characters ch \
            INNER JOIN participants p ON ch.id = p.character_id \
            GROUP BY ch.name")
            for i in cursor:
                print(i)
            response = input("To go back to main menu, press Y: ")
            print("")


        #We get all the species with the average height wanted.
        elif result == "3":
            print("Spider-Man\nWolverine\nColossus\nDoctor Doom\nSilver Surfer\nCaptain America\nDormammu\nMole Man \
            \nMorbius\nRed Skull\nAnti-Venom\nHulk\nMagneto\nSabretooth\nDoctor Strange\nBlack Panther\nJuggernaut \
            \nDeadpool\nThor\nGreen Goblin\nMysterio\nBlack Cat\nIron-Man")
            answer = str(input("Enter the character wanted: "))
            cursor.execute("SELECT super_power FROM allPowers WHERE name = " + "'" + answer + "'")
            for i in cursor:
                print(i)
            response = input("To go back to main menu, press Y: ")
            print("")


        # We get the climate which is most preferable for the wanted specie
        elif result == "4":
            print("1.Spider-Man\n2.Wolverine\n3.Colossus\n4.Doctor Doom\n5.Silver Surfer\n6.Captain America\n7.Dormammu\n8.Mole Man \
            \n9.Morbius\n10.Red Skull\n11.Anti-Venom\n12.Hulk\n13.Magneto\n14.Sabretooth\n15.Doctor Strange\n16.Black Panther\n17.Juggernaut \
            \n18.Deadpool\n19.Thor\n20.Green Goblin\n21.Mysterio\n22.Black Cat\n23.Iron-Man")
            answer = str(input("Enter the id of the character wanted: "))
            cursor.execute("SELECT co.name FROM participants p \
            INNER JOIN comic_books co ON co.id = p.comic_id \
            INNER JOIN characters ch ON ch.id = p.character_id \
            WHERE " + answer + " = p.character_id")
            for i in cursor:
                print(i)
            response = input("To go back to main menu, press Y: ")
            print("")


        # We get the comic book which partially inspired a movie
        elif result == "5":
            cursor.execute("SELECT m.title, co.name FROM movies m, comic_books co WHERE m.comic_id = co.id")
            for i in cursor:
                print(i)
            response = input("To go back to main menu, press Y: ")
            print("")

        # We get the anticipated charcaters for the movies
        elif result == "6":
            cursor.execute("SELECT m.title, ch.name FROM participants p \
            INNER JOIN characters ch ON p.character_id = ch.id \
            INNER JOIN comic_books cb ON cb.id = p.comic_id \
            INNER JOIN movies m ON m.comic_id = cb.id")
            for i in cursor:
                print(i)
            response = input("To go back to main menu, press Y: ")
            print("")

        # We get the sides, the average release year and how many characters of each side there are
        elif result == "7":
            cursor.execute("SELECT side, AVG(release_year), COUNT(side) FROM characters GROUP BY side")
            for i in cursor:
                print(i)
            response = input("To go back to main menu, press Y: ")
            print("")

        else:
            print("Wrong input!")
            print("")

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
        print("Database found successfuly!")
    except mysql.connector.Error as err:
        print("Database {} does not exist".format(DB_NAME))
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Trying to create the database {}".format(DB_NAME))
            create_database(cursor, DB_NAME)
            cnx.database = DB_NAME
            create_tables(cursor)
            readfile(cursor, cnx, "MarvelCharacters.csv", "characters")
            readfile(cursor, cnx, "MarvelComicBooks.csv", "comic_books")
            readfile(cursor, cnx, "MarvelMovies.csv", "movies")
            readfile(cursor, cnx, "MarvelParticipants.csv", "participants")
            cursor.execute("CREATE VIEW allPowers AS SELECT name, super_power FROM characters ch")
            print("Database {} created succesfully.".format(DB_NAME))

    gui(cursor)
    cnx.close()
    cursor.close()

    return


main()
