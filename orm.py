# -*- coding: utf-8 -*-
import MySQLdb
import time
from functions import *

class ORM:

    host = 'localhost'
    user = 'root'
    pw = 'root'
    database = ''

    tb_name = ''

    def __init__(self, database = 'test'):
        self.database = database
        self.conn = MySQLdb.connect(self.host, self.user, self.pw, self.database)

    def set_table(self, tb_name):
        self.tb_name = tb_name

    def read(self, instructions = {}, cols = '', returnKey = ''):

        with self.conn as c:

            perimeter = ""

            if bool(instructions):
                 perimeter += " WHERE "

            i = 1
            for key in instructions:

                if is_number(instructions[key]):
                    perimeter += "`" + key + "` = " + str(instructions[key])
                elif instructions[key] == None:
                    perimeter += "IS NULL"
                else:
                    perimeter += "`" + key + "`  LIKE '" + str(instructions[key]) + "'"

                if i < len(record):
                    perimeter += ' AND '

                i += 1

            column_criteria = "*"

            if cols: #not empty
                if isinstance(cols, basestring):
                    #single key
                    column_criteria = cols
                else:
                    #expect a list here
                    column_criteria = ''
                    i = 1
                    for col_key in cols:
                        column_criteria += col_key

                        if i != len(cols):
                            column_criteria += ", "

            statement = "SELECT " + column_criteria + " FROM `" + self.tb_name + "`" + perimeter

            try:
                c.execute(statement)
                rows = c.fetchall()

                if cols and isinstance(cols, basestring): #return a list
                    return_list = []
                    for row in rows:
                        return_list.append(row[0])

                    return return_list

                if returnKey: #return a dict
                    new_rows = {}

                    key_map = {}
                    for i in range(len(c.description)):
                        key_map[c.description[i][0]] = i

                    if returnKey in key_map:
                        key_index = key_map[returnKey] # 1 in this case

                    for x in range(len(rows)):

                        row = {}

                        for y in range(len(rows[x])):
                            if y != key_index:
                                row[get_key_by_value(y, key_map)] = rows[x][y]

                        new_rows[rows[x][key_index]] = row

                    return new_rows

                return rows

            except Exception, e:
                print(e)
                print('Read Error')

    def insert(self, record, returnID = False):

        with self.conn as c:
            table_keys = ''
            row_values = ''

            i = 1
            for key in record:
                table_keys += "`" + key + "`,"

                value = '"' + str(record[key]) + '"'

                if record[key] == None:
                    value = 'NULL'

                row_values += value + ","

                if i == len(record):
                    #remove the last ','
                    table_keys = table_keys[:-1]
                    row_values = row_values[:-1]

                i += 1

            # statement = 'INSERT INTO `' + self.database + '`.`' + self.tb_name + '` (' + table_keys + ') VALUES (' + row_values + ')'
            statement = "INSERT INTO `" + self.database + "`.`" + self.tb_name + "` (" + table_keys + ") VALUES (" + row_values + ")"

            try:
                c.execute(statement)
                self.conn.commit()  # without it, the insert c.execute() command above will not run

                if returnID != False:
                    lastID = c.lastrowid
                    return lastID

            except Exception, e:
                print(e)
                print('Insert Error:: statement>>' + statement)
                raise Exception


    def close(self):
        self.conn.close()