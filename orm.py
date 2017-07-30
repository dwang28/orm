# -*- coding: utf-8 -*-
import MySQLdb
import time
from functions import *



class DuplicateEntryError(ValueError):
    pass

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

    def create_table(self, recipe):
        with self.conn as c:

            tbl_name = recipe['table_name']

            tbl_column_instructions = ''

            i = 1
            for field in recipe['fields']:
                instruction = "`" + field['name'] + "`" #name
                instruction += " " + field['type']

                if 'length' in field:
                    instruction += "("+ str(field['length']) +")"

                if 'primary_key' in field and field['primary_key'] == True:
                    instruction += " PRIMARY KEY"

                if 'auto_increment' in field and field['auto_increment'] == True:
                    instruction += " AUTO_INCREMENT"

                if 'unique' in field and field['unique'] == True:
                    instruction += " UNIQUE"

                if 'primary_key' in field or ('null' in field and field['null'] == False):
                    instruction += " NOT NULL"


                if 'default' in field:
                    instruction += " DEFAULT "+ field['default']

                if i < len(recipe['fields']):
                    instruction += " ,"

                i += 1

                tbl_column_instructions += instruction

            collate = " CHARACTER SET utf8 COLLATE utf8_unicode_ci"
            statement = "CREATE TABLE IF NOT EXISTS `" + tbl_name + "` (" + tbl_column_instructions + ")" + collate

            try:
                c.execute(statement)

            except Exception, e:
                print(e)
                print('Error Creating Table')

            if 'unique' in recipe:
                statement = "ALTER TABLE " + tbl_name + " "


                i = 1
                column_instruction = ''
                for column_name in recipe['unique']:
                    column_instruction += "`" + column_name + "`"

                    if i < len(recipe['unique']):
                        column_instruction += ", "

                    i += 1

                statement += "ADD UNIQUE KEY `"+ tbl_name+ "_unique_key` (" + column_instruction + ")"
                try:
                    c.execute(statement)
                except Exception, e:
                    print(e)
                    print('Error setting up unique key')


    def test(self):

        with self.conn as c:
            statement = "SHOW COLUMNS FROM "+ self.tb_name

            try:
                result = c.execute(statement)

            except Exception, e:
                print(e)

            print(type(result))



    def read(self, instructions = {}, cols = '', returnKey = ''):

        with self.conn as c:

            perimeter = ""

            if bool(instructions):
                 perimeter += " WHERE "

            i = 1
            for key in instructions:

                #prepare perimeter key
                if is_number(instructions[key]):
                    perimeter += "`" + key + "` = %(" + key+ ")s"
                elif instructions[key] == None:
                    perimeter += "IS NULL"
                else:
                    perimeter += "`" + key + "`  LIKE %(" + key+ ")s"

                if i < len(instructions):
                    perimeter += ' AND '

                #escape special charecters
                if isinstance(instructions[key], basestring):
                    instructions[key] =  instructions[key].replace('\\', '\\\\')

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
                #ref: https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlcursor-execute.html
                c.execute(statement, instructions)
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
                print('Read Error, see error message, statement instructions to select below')
                print(e)
                print(statement)
                print(instructions)

    # row = {
    #     'id': theme_id,
    #     'name': title,
    #     'author': author_id,
    #     'category': category_id
    # }
    def insert(self, record, returnID = False):

        with self.conn as c:
            table_keys = ''
            # row_values = ''
            value_placeholder = ''
            values = []

            i = 1
            for key in record:
                table_keys += "`" + key + "`,"

                value_placeholder += "%s,"
                # value = '"' + str(record[key]) + '"'


                value = str(record[key])

                if record[key] == None:
                    value = 'NULL'

                values.append(value)
                # row_values += value + ","

                if i == len(record):
                    #remove the last ','
                    table_keys = table_keys[:-1]
                    value_placeholder = value_placeholder[:-1]

                i += 1

            statement = "INSERT INTO `" + self.database + "`.`" + self.tb_name + "` (" + table_keys + ") VALUES (" + value_placeholder + ")"
            # statement = "INSERT INTO `" + self.database + "`.`" + self.tb_name + "` (" + table_keys + ") VALUES (" + row_values + ")"

            try:
                value_tuple = tuple(values)

                c.execute(statement, value_tuple)
                # c.execute(statement)
                self.conn.commit()  # without it, the insert c.execute() command above will not run

                if returnID != False:
                    lastID = c.lastrowid
                    return lastID

            except Exception, e:
                if str(e)[:23] == '(1062, "Duplicate entry': #duplicate entry

                    result = self.read(record)

                    if len(result) == 1: #found one record
                        # #assuming the first column is the id, otherwise it won't work
                        raise DuplicateEntryError(result[0][0])

                    elif len(result) == 0: #differnt set of info for the same unique key

                        msg = e.args[1]

                        duplicate_entry = msg.find("Duplicate entry '")

                        begin_of_value = duplicate_entry+17
                        end_of_value = 17+ msg[begin_of_value:].find("'")

                        value = msg[begin_of_value:end_of_value]


                        begin_of_key = end_of_value + 9 + msg[end_of_value:].find("for key '")
                        end_of_key = begin_of_key + msg[begin_of_key:].find("'")

                        key = msg[begin_of_key:end_of_key]


                        if key in record and record[key] == value:
                            result = self.read({key:value})
                            raise DuplicateEntryError(result[0][0])

                        else:
                            print(record)
                            raise ValueError('Failed to update record for duplicate entry ' + str(key) + ":" + str(value))

                    else:
                        print(e)
                        print(statement)
                        raise ValueError('Found ' + str(len(result)) + ' entry when try to locate id of existing record')


                    return 'Dupliacte Entry'


                else:
                    print('Insert Error:: statement>>' + statement)
                    # message = template.format(type(e).__name__, e.args)
                    print(type(e).__name__)

                    raise Exception


    def close(self):
        self.conn.close()