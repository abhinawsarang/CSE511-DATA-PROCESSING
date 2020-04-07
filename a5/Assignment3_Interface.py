#!/usr/bin/python2.7
#
# Assignment3 Interface
# Author Abhinaw Sarang

import os
import psycopg2
import sys
import threading

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    number_of_threads = 5
    cursor = openconnection.cursor()
    cursor.execute("SELECT COUNT(*) FROM {0}".format(InputTable))
    total_records = cursor.fetchall()[0]

    if total_records[0] == 0:
        cursor.execute("CREATE TABLE {0} AS TABLE {1} WITH NO DATA".format(OutputTable, InputTable))

    else:
        min_val = find_min(cursor, SortingColumnName, InputTable)
        max_val = find_max(cursor, SortingColumnName, InputTable)
        if min_val == max_val:
            min_val -= 1
        interval = (max_val - min_val) / number_of_threads
        start = min_val
        create_temp_tables(cursor, number_of_threads, start, interval, "temp1_table", InputTable, SortingColumnName,
                           max_val)

        thread = [None] * number_of_threads
        for index in range(number_of_threads):
            table = "temp1_table_" + str(index)
            arguments = (cursor, table, SortingColumnName, index)
            thread[index] = threading.Thread(target=sort_tuples, args=arguments)
            thread[index].start()

        for i in range(len(thread)):
            thread[i].join()
        cursor.execute("CREATE TABLE {0} AS TABLE {1} WITH NO DATA".format(OutputTable, InputTable))
        for index in range(number_of_threads):
            table = "temp1_table_" + str(index)
            cursor.execute("INSERT INTO {0} SELECT * FROM {1}_{2}".format(OutputTable, table, index))
    openconnection.commit()


def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    number_of_threads = 5
    cursor = openconnection.cursor()
    cursor.execute("SELECT COUNT(*) FROM {0}".format(InputTable1))
    total_records_1 = cursor.fetchall()[0]
    cursor.execute("SELECT COUNT(*) FROM {0}".format(InputTable2))
    total_records_2 = cursor.fetchall()[0]
    if total_records_1[0] == 0 or total_records_2[0] == 0:
        create_output_table(cursor, InputTable1, InputTable2, OutputTable)

    else:
        cursor.execute(
            "SELECT COUNT(*) FROM {0}, {1} WHERE {0}.{2} = {1}.{3}".format(InputTable1, InputTable2, Table1JoinColumn,
                                                                           Table2JoinColumn))
        MIN_VALUE_TABLE_1 = find_min(cursor, Table1JoinColumn, InputTable1)
        MAX_VALUE_TABLE_1 = find_max(cursor, Table1JoinColumn, InputTable1)

        MIN_VALUE_TABLE_2 = find_min(cursor, Table2JoinColumn, InputTable2)
        MAX_VALUE_TABLE_2 = find_max(cursor, Table2JoinColumn, InputTable2)

        min_val = min(MIN_VALUE_TABLE_1, MAX_VALUE_TABLE_2)
        max_val = max(MAX_VALUE_TABLE_1, MAX_VALUE_TABLE_2)
        if min_val == max_val:
            min_val -= 1
        interval = (max_val - min_val) / number_of_threads
        start = min_val
        create_temp_tables(cursor, number_of_threads, start, interval, "temp1_table", InputTable1, Table1JoinColumn,
                           max_val)
        create_temp_tables(cursor, number_of_threads, start, interval, "temp2_table", InputTable2, Table2JoinColumn,
                           max_val)

        create_output_table(cursor, InputTable1, InputTable2, OutputTable)

        thread = [None] * number_of_threads
        for i in range(number_of_threads):
            table1 = "temp1_table_" + str(i)
            table2 = "temp2_table_" + str(i)
            arguments = (cursor, table1, table2, OutputTable, Table1JoinColumn, Table2JoinColumn)
            thread[i] = threading.Thread(target=join_tables, args=arguments)
            thread[i].start()

        for i in range(len(thread)):
            thread[i].join()
    openconnection.commit()


def find_min(cursor, SortingColumnName, InputTable):
    cursor.execute("SELECT MIN({0}) FROM {1}".format(SortingColumnName, InputTable))
    MIN_VALUE = cursor.fetchall()[0][0]
    return MIN_VALUE


def find_max(cursor, SortingColumnName, InputTable):
    cursor.execute("SELECT MAX({0}) FROM {1}".format(SortingColumnName, InputTable))
    MAX_VALUE = cursor.fetchall()[0][0]
    return MAX_VALUE


def sort_tuples(cursor, table, SortingColumnName, index):
    cursor.execute("SELECT * INTO {1}_{0} FROM {1} ORDER BY {2}".format(index, table, SortingColumnName))


def join_tables(cursor, Table1, Table2, OutputTable, column1, column2):
    cursor.execute("INSERT INTO {0} SELECT * FROM {1}, {2} WHERE {1}.{3} = {2}.{4}".format(OutputTable, Table1, Table2, column1, column2))


def create_temp_tables(cursor, number_of_threads, start, interval, temp_table, orignal_table, join_column, max_val):
    for i in range(number_of_threads):
        if i == 0:
            query = "SELECT * INTO {0}_{1} FROM {2} WHERE {3} > {4} AND {3} <= {5}".format(temp_table, i, orignal_table,
                                                                                           join_column, start - 1,
                                                                                           start + interval)
        elif i == number_of_threads - 1:
            query = "SELECT * INTO {0}_{1} FROM {2} WHERE {3} > {4} AND {3} <= {5}".format(temp_table, i, orignal_table,
                                                                                           join_column, start, max_val)
        else:
            query = "SELECT * INTO {0}_{1} FROM {2} WHERE {3} > {4} AND {3} <= {5}".format(temp_table, i, orignal_table,
                                                                                           join_column, start,
                                                                                           start + interval)

        cursor.execute(query)
        start += interval


def create_output_table(cursor, InputTable1, InputTable2, OutputTable):
    column_with_types = []

    cursor.execute("select column_name, data_type from INFORMATION_SCHEMA.COLUMNS where table_name = '{0}'".format(
        InputTable1))
    column_with_types.extend(cursor.fetchall())
    first_table_columns = len(column_with_types)
    cursor.execute("select column_name, data_type from INFORMATION_SCHEMA.COLUMNS where table_name = '{0}'".format(
        InputTable2))
    column_with_types.extend(cursor.fetchall())

    create_table_query = "CREATE TABLE IF NOT EXISTS {0} (".format(OutputTable)

    for index, (col, type) in enumerate(column_with_types):
        if index > first_table_columns - 1:
            create_table_query += 'new_'
        create_table_query += col + ' ' + type + ','
    create_table_query = create_table_query[:len(create_table_query) - 1] + ');'
    cursor.execute(create_table_query)