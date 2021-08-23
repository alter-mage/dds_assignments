import psycopg2
import os
import sys
import math

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    # pass # Remove this once you are done with implementation
    cursor = openconnection.cursor()
    createquery = "CREATE TABLE IF NOT EXISTS %s (userid INTEGER, movieid INTEGER, rating DOUBLE PRECISION)" % (ratingstablename)
    cursor.execute(createquery)
    ratingsfile = open(ratingsfilepath, 'r')
    for rating in ratingsfile:
        splitrating = rating.split('::')
        insertquery = "INSERT INTO %s (userid, movieid, rating) VALUES (%s, %s, %s)" % (
            ratingstablename, splitrating[0], splitrating[1], splitrating[2]
        )
        cursor.execute(insertquery)
    cursor.close()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    # pass # Remove this once you are done with implementation
    cursor = openconnection.cursor()
    metatablequery = "CREATE TABLE IF NOT EXISTS %s (metadata TEXT, value DOUBLE PRECISION)" % ('meta')
    cursor.execute(metatablequery)
    rangepartitioninsertquery = "INSERT INTO %s (metadata, value) VALUES ('%s', %s)" % ('meta', 'rangepartition', numberofpartitions)
    cursor.execute(rangepartitioninsertquery)
    divisor = round(5/numberofpartitions, 2)
    rangedivisorinsertquery = "INSERT INTO %s (metadata, value) VALUES ('%s', %s)" % ('meta', 'rangedivisor', divisor)
    cursor.execute(rangedivisorinsertquery)
    rangefragmenttablelist = []
    for i in range(numberofpartitions):
        rangefragmenttablelist.append('range_ratings_part%s' % (i))
        createquery = "CREATE TABLE IF NOT EXISTS %s (userid INTEGER, movieid INTEGER, rating DOUBLE PRECISION)" % (rangefragmenttablelist[i])
        cursor.execute(createquery)
    selectquery = "SELECT * from %s" % (ratingstablename)
    cursor.execute(selectquery)
    rows = cursor.fetchall()
    for row in rows:
        fragmentindex = math.ceil(row[2]/divisor)-1
        if fragmentindex == -1:
            fragmentindex = 0
        insertquery = "INSERT INTO %s (userid, movieid, rating) VALUES (%s, %s, %s)" % (
            rangefragmenttablelist[fragmentindex], row[0], row[1], row[2]
        )
        cursor.execute(insertquery)
    cursor.close()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    # pass # Remove this once you are done with implementation
    cursor = openconnection.cursor()
    metatablequery = "CREATE TABLE IF NOT EXISTS %s (metadata TEXT, value DOUBLE PRECISION)" % ('meta')
    cursor.execute(metatablequery)
    roundrobinpartitioninsertquery = "INSERT INTO %s (metadata, value) VALUES ('%s', %s)" % ('meta', 'roundrobinpartition', numberofpartitions)
    cursor.execute(roundrobinpartitioninsertquery)
    counter = 0
    roundrobincounterinsertquery = "INSERT INTO %s (metadata, value) VALUES ('%s', %s)" % ('meta', 'roundrobincounter', counter)
    cursor.execute(roundrobincounterinsertquery)
    roundrobinfragmenttablelist = []
    for i in range(numberofpartitions):
        roundrobinfragmenttablelist.append('round_robin_ratings_part%s' % (i))
        createquery = "CREATE TABLE IF NOT EXISTS %s (userid INTEGER, movieid INTEGER, rating DOUBLE PRECISION)" % (roundrobinfragmenttablelist[i])
        cursor.execute(createquery)
    selectquery = "SELECT * FROM %s" % (ratingstablename)
    cursor.execute(selectquery)
    ratingsrows = cursor.fetchall()
    for ratingsrow in ratingsrows:
        insertquery = "INSERT INTO %s (userid, movieid, rating) VALUES (%s, %s, %s)" % (
            roundrobinfragmenttablelist[counter%numberofpartitions], ratingsrow[0], ratingsrow[1], ratingsrow[2]
        )
        cursor.execute(insertquery)
        if counter == 4:
            counter = 0
        else:
            counter += 1
    roundrobincounterupdatequery = "UPDATE %s SET value=%s WHERE %s ='%s'" % ('meta', counter, 'metadata', 'roundrobincounter')
    cursor.close()


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    # pass # Remove this once you are done with implementation
    cursor = openconnection.cursor()
    roundrobinpartitionquery = "SELECT %s FROM %s WHERE %s ='%s'" % ('value', 'meta', 'metadata', 'roundrobinpartition')
    cursor.execute(roundrobinpartitionquery)
    partition = int(cursor.fetchall()[0][0])
    roundrobincounterquery = "SELECT %s FROM %s WHERE %s ='%s'" % ('value', 'meta', 'metadata', 'roundrobincounter')
    cursor.execute(roundrobincounterquery)
    counter = int(cursor.fetchall()[0][0])
    insertquery = "INSERT INTO %s (userid, movieid, rating) VALUES (%s, %s, %s)" % (
        'round_robin_ratings_part%s' % (counter%partition), userid, itemid, rating
    )
    cursor.execute(insertquery)
    if counter == 4:
        counter = 0
    else:
        counter += 1
    roundrobincounterupdatequery = "UPDATE %s SET value = %s WHERE %s ='%s'" % ('meta', counter, 'metadata', 'roundrobincounter')
    cursor.close()


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    # pass # Remove this once you are done with implementation
    cursor = openconnection.cursor()
    insertquery = "INSERT INTO %s (userid, movieid, rating) VALUES (%s, %s, %s)" % (
        ratingstablename, userid, itemid, rating
    )
    cursor.execute(insertquery)
    rangedivisorquery = "SELECT %s FROM %s WHERE %s ='%s'" % ('value', 'meta', 'metadata', 'rangedivisor')
    cursor.execute(rangedivisorquery)
    divisor = cursor.fetchall()[0][0]
    fragmentindex = math.ceil(rating/divisor)-1
    if fragmentindex == -1:
        fragmentindex = 0
    fragmenttable = "range_ratings_part%s" % (fragmentindex)
    fragmentinsertquery = "INSERT INTO %s (userid, movieid, rating) VALUES (%s, %s, %s)" % (
        fragmenttable, userid, itemid, rating
    )
    cursor.execute(fragmentinsertquery)
    cursor.close()


def rangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    # pass #Remove this once you are done with implementation
    cursor = openconnection.cursor()
    outputfile = open(outputPath, 'a')
    rangepartitionquery = "SELECT %s FROM %s WHERE %s ='%s'" % ('value', 'meta', 'metadata', 'rangepartition')
    cursor.execute(rangepartitionquery)
    rangepartition = int(cursor.fetchall()[0][0])
    rangedivisorquery = "SELECT %s FROM %s WHERE %s ='%s'" % ('value', 'meta', 'metadata', 'rangedivisor')
    cursor.execute(rangedivisorquery)
    rangedivisor = cursor.fetchall()[0][0]
    minratingindex = math.ceil(ratingMinValue/rangedivisor)-1
    if minratingindex == -1:
        minratingindex = 0
    maxratingindex = math.ceil(ratingMaxValue/rangedivisor)-1
    if maxratingindex == -1:
        maxratingindex = 0
    for rangefragmenttableindex in range(minratingindex, maxratingindex+1):
        rangeselectquery = "SELECT * FROM %s" % ('range_ratings_part%s' % (rangefragmenttableindex))
        cursor.execute(rangeselectquery)
        rangefragmenttablerows = cursor.fetchall()
        for rangefragmentrow in rangefragmenttablerows:
            if rangefragmentrow[2]>=ratingMinValue and rangefragmentrow[2]<=ratingMaxValue:
                outputfile.write('%s,' % ('range_ratings_part%s' % (rangefragmenttableindex)) + ','.join(str(data) for data in rangefragmentrow) + '\n')
    roundrobinpartitionquery = "SELECT %s FROM %s WHERE %s ='%s'" % ('value', 'meta', 'metadata', 'roundrobinpartition')
    cursor.execute(roundrobinpartitionquery)
    roundrobinpartition = int(cursor.fetchall()[0][0])
    for roundrobintableindex in range(roundrobinpartition):
        roundrobinselectquery = "SELECT * FROM %s" % ('round_robin_ratings_part%s' % (roundrobintableindex))
        cursor.execute(roundrobinselectquery)
        roundrobinfragmenttablerows = cursor.fetchall()
        for roundrobinfragmenttablerow in roundrobinfragmenttablerows:
            if roundrobinfragmenttablerow[2]>=ratingMinValue and roundrobinfragmenttablerow[2]<=ratingMaxValue:
                outputfile.write('%s,' % ('round_robin_ratings_part%s' % (roundrobintableindex)) + ','.join(str(data) for data in roundrobinfragmenttablerow) + '\n')
    outputfile.close()
    cursor.close()


def pointQuery(ratingValue, openconnection, outputPath):
    # pass # Remove this once you are done with implementation
    cursor = openconnection.cursor()
    outputfile = open(outputPath, 'a')
    rangedivisorquery = "SELECT %s FROM %s WHERE %s ='%s'" % ('value', 'meta', 'metadata', 'rangedivisor')
    cursor.execute(rangedivisorquery)
    rangedivisor = cursor.fetchall()[0][0]
    rangefragmenttableindex = math.ceil(ratingValue/rangedivisor)-1
    if rangefragmenttableindex == -1:
        rangefragmenttableindex = 0
    rangefragmentselectquery = "SELECT * FROM %s" % ('range_ratings_part%s' % (rangefragmenttableindex))
    cursor.execute(rangefragmentselectquery)
    rangefragmentrows = cursor.fetchall()
    for rangefragmentrow in rangefragmentrows:
        if rangefragmentrow[2] == ratingValue:
            outputfile.write('%s,' % ('range_ratings_part%s' % (rangefragmenttableindex)) + ','.join(str(data) for data in rangefragmentrow) + '\n')
    roundrobinpartitionquery = "SELECT %s FROM %s WHERE %s ='%s'" % ('value', 'meta', 'metadata', 'roundrobinpartition')
    cursor.execute(roundrobinpartitionquery)
    roundrobinpartition = int(cursor.fetchall()[0][0])
    for roundrobinfragmentindex in range(roundrobinpartition):
        roundrobinselectquery = "SELECT * FROM %s" % ('round_robin_ratings_part%s' % (roundrobinfragmentindex))
        cursor.execute(roundrobinselectquery)
        roundrobinfragmenttablerows = cursor.fetchall()
        for roundrobinfragmentrow in roundrobinfragmenttablerows:
            if roundrobinfragmentrow[2] == ratingValue:
                outputfile.write('%s,' % ('round_robin_ratings_part%s' % (roundrobinfragmentindex)) + ','.join(str(data) for data in roundrobinfragmentrow) + '\n')
    outputfile.close()
    cursor.close()


def createDB(dbname='dds_assignment1'):
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
    con.close()

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
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
