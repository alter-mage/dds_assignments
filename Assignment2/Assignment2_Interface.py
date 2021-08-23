#
# Assignment2 Interface
#

import psycopg2
import os
import sys
import threading

def range_insert_into_partition(InputTable, SortingColumnName, schema_info, lower_bound, upper_bound, partition_index, openconnection):
    # print('doing random stuff')
    cursor = openconnection.cursor()
    partition_name = ''.join([InputTable, str(partition_index)])
    range_partition_create_query = "CREATE TABLE IF NOT EXISTS %s (" % (partition_name)
    for schema_info_tuple in schema_info:
        range_partition_create_query += "%s %s, " % (schema_info_tuple[0], schema_info_tuple[1].upper())
    range_partition_create_query = range_partition_create_query[:-2]+")"
    cursor.execute(range_partition_create_query)
    if partition_index == 0:
        range_insert_query = "INSERT INTO %s SELECT * FROM %s WHERE %s>=%s and %s<=%s ORDER BY %s ASC" % (partition_name, InputTable, SortingColumnName, lower_bound, SortingColumnName, upper_bound, SortingColumnName)
    else:
        range_insert_query = "INSERT INTO %s SELECT * FROM %s WHERE %s>%s and %s<=%s ORDER BY %s ASC" % (partition_name, InputTable, SortingColumnName, lower_bound, SortingColumnName, upper_bound, SortingColumnName)
    cursor.execute(range_insert_query)
    cursor.close()
    return

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.

    cursor = openconnection.cursor()
    schema_query = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='%s'" % (InputTable)
    cursor.execute(schema_query)
    schema_info = cursor.fetchall()
    
    sorting_column_min_query = "SELECT MIN(%s) FROM %s" % (SortingColumnName, InputTable)
    cursor.execute(sorting_column_min_query)
    sorting_column_min = float(cursor.fetchall()[0][0])

    sorting_column_max_query = "SELECT MAX(%s) FROM %s" % (SortingColumnName, InputTable)
    cursor.execute(sorting_column_max_query)
    sorting_column_max = float(cursor.fetchall()[0][0])
    sorting_column_interval = (sorting_column_max-sorting_column_min)/5

    threads = [None]*5
    for thread_index in range(5):
        if thread_index == 0:
            lower_bound = sorting_column_min
            upper_bound = sorting_column_min + sorting_column_interval
        else:
            lower_bound = upper_bound
            upper_bound += sorting_column_interval
        threads[thread_index] = threading.Thread(target=range_insert_into_partition, args=(InputTable, SortingColumnName, schema_info, lower_bound, upper_bound, thread_index, openconnection))
        threads[thread_index].start()
    
    for thread_index in range(5):
        threads[thread_index].join()

    output_table_create_query = "CREATE TABLE IF NOT EXISTS %s (" % (OutputTable)
    for schema_info_tuple in schema_info:
        output_table_create_query += "%s %s, " % (schema_info_tuple[0], schema_info_tuple[1].upper())
    output_table_create_query = output_table_create_query[:-2] + ")"
    cursor.execute(output_table_create_query)

    for partition_index in range(5):
        partition_table = ''.join([InputTable, str(partition_index)])
        partition_select_query = "INSERT INTO %s SELECT * FROM %s" % (OutputTable, partition_table)
        cursor.execute(partition_select_query)
        drop_partition_table_query = "DROP TABLE IF EXISTS %s" % (partition_table)
        cursor.execute(drop_partition_table_query)

    cursor.close()
    openconnection.commit()
    # pass #Remove this once you are done with implementation

def range_join_into_partition(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, lower_bound, upper_bound, partition_name, schema_info_1, schema_info_2, openconnection):
    cursor = openconnection.cursor()

    partition_create_query = "CREATE TABLE IF NOT EXISTS %s (" % (partition_name)
    for schema_info_1_tuple in schema_info_1:
        partition_create_query += "%s %s, " % (schema_info_1_tuple[0], schema_info_1_tuple[1].upper())
    for schema_info_2_tuple in schema_info_2:
        partition_create_query += "%s %s, " % (schema_info_2_tuple[0], schema_info_2_tuple[1].upper())
    partition_create_query = partition_create_query[:-2] + ")"
    cursor.execute(partition_create_query)

    if partition_name[-1] == '0':
        partition_join_insert_query = "INSERT INTO %s SELECT * FROM %s, %s WHERE %s.%s=%s.%s AND %s.%s>=%s and %s.%s<=%s" % (partition_name, InputTable1, InputTable2, InputTable1, Table1JoinColumn, InputTable2, Table2JoinColumn, InputTable1, Table1JoinColumn, str(lower_bound), InputTable1, Table1JoinColumn, str(upper_bound))
    else:
        partition_join_insert_query = "INSERT INTO %s SELECT * FROM %s, %s WHERE %s.%s=%s.%s AND %s.%s>%s and %s.%s<=%s" % (partition_name, InputTable1, InputTable2, InputTable1, Table1JoinColumn, InputTable2, Table2JoinColumn, InputTable1, Table1JoinColumn, str(lower_bound), InputTable1, Table1JoinColumn, str(upper_bound))
    cursor.execute(partition_join_insert_query)
    cursor.close()

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    cursor = openconnection.cursor()

    schema_query_1 = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='%s'" % (InputTable1)
    cursor.execute(schema_query_1)
    schema_info_1 = cursor.fetchall()
    schema_query_2 = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='%s'" % (InputTable2)
    cursor.execute(schema_query_2)
    schema_info_2 = cursor.fetchall()

    join_column1_min_query = "SELECT MIN(%s) FROM %s" % (Table1JoinColumn, InputTable1)
    cursor.execute(join_column1_min_query)
    join_column1_min = cursor.fetchall()[0][0]
    join_column2_min_query = "SELECT MIN(%s) FROM %s" % (Table2JoinColumn, InputTable2)
    cursor.execute(join_column2_min_query)
    join_column2_min = cursor.fetchall()[0][0]
    join_column1_max_query = "SELECT MAX(%s) FROM %s" % (Table1JoinColumn, InputTable1)
    cursor.execute(join_column1_max_query)
    join_column1_max = cursor.fetchall()[0][0]
    join_column2_max_query = "SELECT MAX(%s) FROM %s" % (Table2JoinColumn, InputTable2)
    cursor.execute(join_column2_max_query)
    join_column2_max = cursor.fetchall()[0][0]
    if join_column1_min < join_column2_min:
        join_min = join_column1_min
    else:
        join_min = join_column2_min
    
    if join_column1_max > join_column2_max:
        join_max = join_column1_max
    else:
        join_max = join_column2_max
    range_interval = (join_max-join_min)/5

    threads = [None]*5
    for partition_index in range(5):
        partition_name = ''.join(['_'.join([InputTable1, InputTable2]), str(partition_index)])
        if partition_index == 0:
            lower_bound = join_min
            upper_bound = join_min + range_interval
        else:
            lower_bound = upper_bound
            upper_bound = upper_bound + range_interval
        threads[partition_index] = threading.Thread(target=range_join_into_partition, args=(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, lower_bound, upper_bound, partition_name, schema_info_1, schema_info_2, openconnection))
        threads[partition_index].start()
        
    for partition_index in range(5):
        threads[partition_index].join()

    output_table_create_query = "CREATE TABLE IF NOT EXISTS %s (" % (OutputTable)
    for schema_info_1_tuple in schema_info_1:
        output_table_create_query += "%s %s, " % (schema_info_1_tuple[0], schema_info_1_tuple[1].upper())
    for schema_info_2_tuple in schema_info_2:
        output_table_create_query += "%s %s, " % (schema_info_2_tuple[0], schema_info_2_tuple[1].upper())
    output_table_create_query = output_table_create_query[:-2] + ")"
    cursor.execute(output_table_create_query)

    for partition_index in range(5):
        partition_name = ''.join(['_'.join([InputTable1, InputTable2]), str(partition_index)])
        output_insert_query = "INSERT INTO %s SELECT * FROM %s" % (OutputTable, partition_name)
        cursor.execute(output_insert_query)
        partition_table_drop_query = "DROP TABLE %s" % (partition_name)
        cursor.execute(partition_table_drop_query)

    cursor.close()
    openconnection.commit()
    # pass # Remove this once you are done with implementation


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='dds_assignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='dds_assignment2'):
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
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
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
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


