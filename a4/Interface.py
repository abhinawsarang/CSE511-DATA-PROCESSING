#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    allResult = []

    cursor = openconnection.cursor()
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_name like 'roundrobinratingspart%'")
    numberOfRRPart = 0
    for row in cursor:
        numberOfRRPart = numberOfRRPart + 1
    for tableNumber in range(numberOfRRPart):
        currTableName = "roundrobinratingspart" + str(tableNumber)
        cursor.execute("SELECT '" + currTableName + "' as PartitionName, UserID, MovieID, Rating FROM " + currTableName +
        " WHERE Rating >= %f and Rating <= %f" % (ratingMinValue, ratingMaxValue))
        for each in cursor:
            allResult.append(each)

    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_name like 'rangeratingspart%'")
    numberOfRangePart = 0
    for row in cursor:
        numberOfRangePart = numberOfRangePart + 1
    for tableNumber in range(numberOfRangePart):
        currTableName = "rangeratingspart" + str(tableNumber)
        cursor.execute("SELECT '" + currTableName + "' as PartitionName, UserID, MovieID, Rating FROM " + currTableName +
        " WHERE Rating >= %f and Rating <= %f" % (ratingMinValue, ratingMaxValue))
        for each in cursor:
            allResult.append(each)

    cursor.close()
    writeToFile('RangeQueryOut.txt', allResult)



def PointQuery(ratingsTableName, ratingValue, openconnection):
    allResult = []

    cursor = openconnection.cursor()
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_name like 'roundrobinratingspart%'")
    numberOfRRPart = 0
    for row in cursor:
        numberOfRRPart = numberOfRRPart + 1
    for tableNumber in range(numberOfRRPart):
        currTableName = "roundrobinratingspart" + str(tableNumber)
        cursor.execute("SELECT '" + currTableName + "' as PartitionName, UserID, MovieID, Rating FROM " + currTableName + " WHERE Rating = %f" % (ratingValue))
        for each in cursor:
            allResult.append(each)

    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_name like 'rangeratingspart%'")
    numberOfRangePart = 0
    for row in cursor:
        numberOfRangePart = numberOfRangePart + 1
    for tableNumber in range(numberOfRangePart):
        currTableName = "rangeratingspart" + str(tableNumber)
        cursor.execute("SELECT '" + currTableName + "' as PartitionName, UserID, MovieID, Rating FROM " + currTableName + " WHERE Rating = %f" % (ratingValue))
        for each in cursor:
            allResult.append(each)

    cursor.close()
    writeToFile('PointQueryOut.txt', allResult)


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
