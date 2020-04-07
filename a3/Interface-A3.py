#!/usr/bin/python2.7
#
# Abhinaw Sarang
# CSE511 Spring 2020 Assignement-3
#

import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    """
    Connect to DB, create table, open input file,
    load date into DB with seperator
    then drop unwanted columns.
    """
    cursor = openconnection.cursor()

    cursor.execute(
        "CREATE TABLE " + ratingstablename +
        " (rowID serial primary key, UserID INT, temp1 VARCHAR(10)," +
        " MovieID INT , temp2 VARCHAR(10), Rating REAL, temp3 VARCHAR(10), Timestamp BIGINT)")
    inputFile = open(ratingsfilepath, 'r')
    cursor.copy_from(inputFile, ratingstablename, sep=':',
                  columns=('UserID', 'temp1', 'MovieID', 'temp2', 'Rating', 'temp3', 'Timestamp'))
    cursor.execute(
        "ALTER TABLE " + ratingstablename + " DROP COLUMN Timestamp, " +
        " DROP COLUMN temp1, DROP COLUMN temp2, DROP COLUMN temp3")
    
    cursor.close()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    """
    Connect to DB, compute range for each partition
    create tables by selecting all rows into each rating range.
    """
    cursor = openconnection.cursor()
    ratingRange = 5.0 / numberofpartitions
    partitonName = 0
    ratingStart = 0

    while ratingStart < 5.0:
        if ratingStart == 0:
            cursor.execute(
                "CREATE TABLE range_part" + str(partitonName) +
                " AS SELECT UserID, MovieID, Rating FROM " + ratingstablename +
                " WHERE Rating<=" + str(ratingStart + ratingRange) + ";")
        else:
            cursor.execute(
                "CREATE TABLE range_part" + str(partitonName) +
                " AS SELECT UserID, MovieID, Rating FROM " + ratingstablename +
                " WHERE Rating>" + str(ratingStart) +
                " AND Rating<=" + str(ratingStart + ratingRange) + ";")
        partitonName = partitonName + 1
        ratingStart = ratingStart + ratingRange
    
    cursor.close()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    """
    Connect to DB, then based on original table row number
    compute partition number for each row and
    create partiton table by selecting those rows.
    """
    cursor = openconnection.cursor()

    for pNum in range(numberofpartitions):
        cursor.execute(
            "CREATE TABLE rrobin_part" + str(pNum) +
            " AS SELECT UserID, MovieID, Rating FROM " + ratingstablename +
            " WHERE rowID % " + str(numberofpartitions) + " = " + str((pNum+1) % numberofpartitions))

    cursor.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Connect to DB, fetch all tables with name like rrobin_part,
    count of these partition tables will give number of partitions.
    Find partiton table with max number of entry, then inser row into next partition table.
    """
    cursor = openconnection.cursor()
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name like 'rrobin_part%'")
    numberOfPart = 0
    for row in cursor:
        numberOfPart = numberOfPart + 1

    endingPartNumber = 0
    maxRowPart = 0
    for tableNumber in range(numberOfPart):
        cursor.execute("SELECT COUNT (*) FROM rrobin_part" + str(tableNumber) + ";")
        currPartCount = cursor.fetchone()[0]

        if currPartCount >= maxRowPart:
            endingPartNumber = tableNumber
            maxRowPart = currPartCount

    currPartition = (endingPartNumber + 1) % numberOfPart

    cursor.execute(
        "INSERT INTO rrobin_part" + str(currPartition) +
        " (UserID ,MovieID, Rating) VALUES (" +
        str(userid) + "," + str(itemid) + "," + str(rating) + ")")

    cursor.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Connect to DB, fetch all tables with name like range_part,
    count of these partition tables will give number of partitions.
    Find range for each tables then insert given row into matching partition table.
    """
    cursor = openconnection.cursor()
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name like 'range_part%'")
    numberOfPart = 0
    for row in cursor:
        numberOfPart = numberOfPart + 1
    ratingRange = 5.0 / numberOfPart
    partitionnumber = 0
    ratingEnd = ratingRange

    while ratingEnd <= 5.0:
        if rating <= ratingEnd:
            break
        ratingEnd = ratingEnd + ratingRange
        partitionnumber = partitionnumber + 1

    cursor.execute(
        "INSERT INTO range_part" + str(partitionnumber) +
        " (UserID, MovieID, Rating) VALUES (" +
        str(userid) + "," + str(itemid) + "," + str(rating) + ")")

    cursor.close()

def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print ('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()
