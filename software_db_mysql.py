import mysql.connector as mysql
import os,sys

class Database:
    '''
    Database class to connect to database and perform regular database
    functionality.
    '''

    def __init__(self, database_name):
        '''
        Init function, to hold database details, and initiate connection to
        database. Program expects host, port, username and password values are
        correct.
        Arguments for class: database_name as a string.
        '''
        self.database_name = database_name
        self.host = "192.168.163.128"
        self.port = 3306
        self.username = "testuser"
        self.password = "password"

        self.cursor = self.start_connection()

    # -------------------------------------------------------------------- #

    def start_connection(self):
        '''
        Starts connection to MySQL Server.
        Return value: MySQL cursor object.
        '''
        try:
            self.mysql_obj = mysql.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                passwd=self.password,
                database=self.database_name,
                connect_timeout=2000
            )
            cursor = self.mysql_obj.cursor()
            return cursor
        except mysql.Error as err:
            print("Connection Error")

    def close_connection(self):
        '''
        Close database connection.
        Arguments: void.
        Return value: void.
        '''
        print("Closing Connection")
        self.mysql_obj.close()
        print("Connection Closed Successfully")

    # ------------------------------------------------------------------ #

    def create_database(self):
        '''
        Create database on MySQL server.
        Arguments: void.
        Return value: void.
        '''
        try:
            query = "CREATE DATABASE IF NOT EXISTS {}".format(self.database_name)
            self.cursor.execute(query)
        except mysql.Error as err:
            print("Error: {}".format(err), file=sys.stderr)

    def get_databases(self):
        '''
        Get list of databases from server.
        Arguments: void.
        Return value: Array containing each database name. If query fails,
        returns an empty array.
        '''
        db_array = []
        query = "SHOW DATABASES"
        try:
            self.cursor.execute(query)
            records = self.cursor.fetchall()
            for record in records:
                db_array.append(record[0])
        except mysql.Error as err:
            print("Error: {}".format(err), file=sys.stderr)
        finally:
            return db_array

    def display_databases(self):
        '''
        Get database from server using get_databases function, and display the
        results to stdout.
        Arguments: void.
        Return value: void.
        '''
        db_list = self.get_databases()
        print('List of Databases present:')
        print('-'*20)
        for records in db_list:
            print(records)
        print('-'*20)

    # ------------------------------------------------------------------- #

    def create_table(self, table_name, table_schema):
        '''
        Create a table on the database.
        Arguments:
            table_name: String
            table_schema: String table schema
        Return value: void.
        '''
        query = "CREATE TABLE IF NOT EXISTS {}.{}({})".format(self.database_name, table_name, table_schema)
        try:
            self.cursor.execute(query)
        except mysql.Error as err:
            print("Error: {}".format(err), file=sys.stderr)

    def get_tables(self):
        '''
        Get list of tables from server.
        Arguments: void.
        Return value: Array containing each table name. If query is unsuccessful,
            returns an empty array.
        '''
        table_list = []
        query = "SHOW TABLES FROM {}".format(self.database_name)
        try:
            self.cursor.execute(query)
            records = self.cursor.fetchall()
            for record in records:
                table_list.append(record[0])
        except mysql.Error as err:
            print("Error: {}".format(err), file=sys.stderr)
        finally:
            return table_list

    def display_tables(self):
        '''
        Get tables from server using get_tables function, and display the
        results to stdout.
        Arguments: void.
        Return value: void.
        '''
        table_list = self.get_tables()
        print("List of Tables present in {}".format(self.database_name))
        print('-'*20)
        for record in table_list:
            print(record)
        print('-'*20)

    def get_table_schema(self, table_name):
        '''
        Get column names for selected table.
        Arguments: table_name.
        Return value: Returns an array. If query is unsuccessful, returns an
            empty array. 
        '''
        records = []
        query = "DESCRIBE {}.{}".format(self.database_name, table_name)
        try:
            self.cursor.execute(query)
            records = self.cursor.fetchall()
        except mysql.Error as err:
            print("Error: {}".format(err), file=sys.stderr)
        finally:
            return records

    # ------------------------------------------------------------------- #

    def insert_value(self, table, input_array=[]):
        '''
        Insert values into the table. Runs in two modes: If input array is empty,
            runs interactively. Otherwise takes data from input_array and inserts
            them into the database.
        Argumets:
            table: Table name
            input_array: An array of given values. Expects all values to be strings.
        Return value: void.
        '''
        table_schema = self.get_table_schema(table)
        # Interactive Mode
        if input_array == []:
            print("Values that can be inserted into the table: Name Type(max size)")
            for num,record in enumerate(table_schema):
                if num != 0:
                    print(num, record[0], record[1])
            for num,record in enumerate(table_schema):
                if num !=0:
                    value = input("Insert value for {}: ".format(record[0]))
                    input_array.append(value)
        # Generating schema  and value strings, using .join string function and list comprehension.
        schema_string = ",".join([x[0] for n,x in enumerate(table_schema) if n !=0])
        value_string = ",".join(["'{}'".format(x) for x in input_array])
        query_string = "INSERT INTO {} ({}) VALUES ({})".format(table, schema_string, value_string)
        try:
            self.cursor.execute(query_string)
            self.mysql_obj.commit()
        except mysql.Error as err:
            print("Error: {}".format(err), file=sys.stderr)

    def search_value(self, table, search_string):
        '''
        Searches for a given string in the database.
        Arguments:
            table: Table name
            search_string: The string to search in the database
        Return value:
            If search is successful, returns a dict with all corresponding search
            values. If search string is empty or search fails, returns an empty dict.
        '''
        line_dict = {}
        if search_string == "":
            return line_dict
        else:
            table_schema = self.get_table_schema(table)
            column_names = ",".join([x[0] for n,x in enumerate(table_schema)])
            column_array = [x[0] for n,x in enumerate(table_schema)]
            # Search for position of old value
            id_query = "SELECT * FROM {}.{} WHERE '{}' IN ({})".format(self.database_name, table, 
                search_string, column_names)
            try:
                self.cursor.execute(id_query)
                value = self.cursor.fetchall()
                # Mapping of values in dict, col_name variable for exact column name
                for i in range(len(column_array)):
                    line_dict[column_array[i]] = value[0][i]
            except mysql.Error as err:
                print("Error: {}".format(err), file=sys.stderr)
            finally:
                return line_dict

    def update_value(self, table, old_value, new_value):
        '''
        Replaces an existing value with a new value.
        Arguments:
            table: table name
            old_value: The value to look for in the database
            new_value: the value to replace in the database
        Return value: void.
        '''
        table_schema = self.get_table_schema(table)
        column_array = [x[0] for n,x in enumerate(table_schema)]
        id = column_array[0]
        line_dict = self.search_value(table, old_value)
        # Get Key of Search String
        for key,value in line_dict.items():
            if value == old_value:
                col_name = key
        update_query = "UPDATE {}.{} SET {} = '{}' WHERE {}={}".format(self.database_name, table, col_name,
            new_value, id, line_dict[id])
        try:
            self.cursor.execute(update_query)
            self.mysql_obj.commit()
        except mysql.Error as err:
            print("Error: {}".format(err), file=sys.stderr)

    def delete_values(self, table, value):
        '''
        Deletes an entire row when given a search term.
        Arguments:
            table: Table name
            value: The value to look for.
        Return value: void
        '''
        table_schema = self.get_table_schema(table)
        column_array = [x[0] for n,x in enumerate(table_schema)]
        id = column_array[0]
        line_dict = self.search_value(table, value)
        deletion_query = "DELETE FROM {}.{} WHERE {} = {}".format(self.database_name, table, id, line_dict[id])
        # Confirmation before deletion
        confirm = input("Are you sure you want to delete the entire row? (Yes / No): ")
        confirm = confirm.lower()
        if confirm[0] == "y":
            try:
                self.cursor.execute(deletion_query)
                self.mysql_obj.commit()
                print("The selected row has been deleted.")
            except mysql.Error as err:
                print("Error: {}".format(err), file=sys.stderr)

    def show_all_values(self, table):
        '''
        Shows all values from a given table in a database
        Arguments:
            table: Table name
        Return value:
            If query is succesful, returns an array with multiple dicts
            corresponding to each row. If query fails, returns a blank array.
        '''
        result = []
        query = "SELECT * FROM {}.{}".format(self.database_name, table)
        table_schema = self.get_table_schema(table)
        column_array = [x[0] for n,x in enumerate(table_schema)]
        try:
            self.cursor.execute(query)
            records = self.cursor.fetchall()
        except mysql.Error as err:
            print("Error: {}".format(error), file=sys.stderr)
        for line in records:
            line_dict = {}
            for num,value in enumerate(line):
                line_dict[column_array[num]] = value
            result.append(line_dict)
            
        return result

class SoftwareIndustry(Database):
    '''
    Software Industry Class to create employee database. Interactive Program
    '''
    def __init__(self, database_name):
        self.database_name = database_name
        super().__init__(self.database_name)
        self.status = True

    def clear_screen(self):
        '''
        Clears the command line screen. Compatible with both Windows and UNIX
        systems.
        '''
        os.system('clear')
        os.system('cls')

    def exit_program(self):
        self.close_connection()
        sys.exit()

    def continue_program(self):
        choice = input("Continue (Yes / No): ")
        if choice.lower()[0] == "y":
            self.status = True
            self.interactive()
        else:
            status = False
            print("Exiting Program.")
            self.exit_program()

    def interactive(self):
        '''
        Runs a command-line interactive program to operate the database
        Arguments: None.
        Return value: Void.
        '''
        self.clear_screen()
        while self.status:
            print("1. Create Database")
            print("2. Show All Databases")
            print("3. Create Table in Database")
            print("4. Show All Tables in Database")
            print("5. Add Data to Table")
            print("6. Update Data in Table")
            print("7. Delete a Row from Table")
            print("8. Show all Data Rows in Table")
            print("9. Exit Program")
            choice = int(input("Enter Your Choice: "))
            if choice == 9:
                print("Exiting Program.")
                status = False
                sys.exit()
            elif choice == 1:
                self.create_database()
                self.continue_program()
            elif choice == 2:
                self.display_databases()
                self.continue_program()
            elif choice == 3:
                table_name = input("Enter Table Name: ")
                table_schema = input("Enter the table schema: ")
                self.create_table(table_name, table_schema)
                self.continue_program()
            elif choice == 4:
                self.display_tables()
                self.continue_program()
            elif choice == 5:
                table_name = input("Enter Table Name: ")
                self.insert_value(table_name)
                self.continue_program()
            elif choice == 6:
                table_name = input("Enter Table Name: ")
                existing_table = self.show_all_values(table_name)
                for row in existing_table:
                    print(row)
                old_value = input("Enter Old Value you want to change: ")
                new_value = input("Enter New Value you want to insert: ")
                self.update_value(table_name, old_value, new_value)
                print("Showing Updated Changes")
                existing_table = self.show_all_values(table_name)
                for row in existing_table:
                    print(row)
                self.continue_program()
            elif choice == 7:
                table_name = input("Enter Table Name: ")
                existing_table = self.show_all_values(table_name)
                for row in existing_table:
                    print(row)
                value = input("Enter the value whose row you want to delete: ")
                self.delete_values(table_name, value)
                self.continue_program()
            elif choice == 8:
                table_name = input("Enter Table Name: ")
                existing_table = self.show_all_values(table_name)
                for row in existing_table:
                    print(row)
                self.continue_program()
            else:
                print("Incorrect Option selected.")
                self.continue_program()

if __name__ == "__main__":
    si = SoftwareIndustry("SoftwareIndustry")
    si.interactive()
