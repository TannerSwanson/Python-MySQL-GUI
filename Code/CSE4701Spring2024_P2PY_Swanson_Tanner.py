import mysql.connector

class database:
    def __init__(self, host, username, password, database):
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        self.connection = None
        self.cursor = None
    
    def connect(self):
        '''Connect to MySQL server. Returns 1 for success'''
        try:
            self.connection = mysql.connector.connect (host=self.host, user=self.username, password=self.password,database=self.database)
            self.cursor = self.connection.cursor()
            return 1
        except mysql.connector.Error as e:
            print("Failed to connect to MySQL database, error:", e)
        
    def disconnect(self):
        '''If connected, disconnects.'''
        if self.connection != None:
            if self.connection.is_connected():
                self.cursor.close()
                self.connection.close()

    def execute_query(self, sql_query, values):
        '''Sql_query as string, values as tuple. Used for READ queries'''
        try:
            if (self.connection is None):
                print("Connection is not properly established!")
                return
            self.cursor = self.connection.cursor()
            self.cursor.execute(sql_query, values)
            return None
        except mysql.connector.Error as e:
            return e
    
    def execute_query_commit(self, sql_query, values):
        '''Sql_query as string, values as tuple. Used for WRITE queries'''
        try:
            if (self.connection is None):
                print("Connection is not properly established!")
                return
            self.cursor = self.connection.cursor()
            self.cursor.execute(sql_query, values)
            self.connection.commit()
            return None
        except mysql.connector.Error as e:
            return e
    
    def update_values(self, table, update_dict, key):
        '''takes table as str, update_dict as str:str pair, key as str.
        Only adds user inputted values to query, leaves N/A responses out.'''
        query_str = f'UPDATE {table} SET '
        first_attr = True  # Flag to track if it's the first attribute
        if update_dict:
            for attrLabel, attrValue in update_dict.items():
                # Properly quote string values and handle NULL values
                if isinstance(attrValue, str):
                    attrValue = f'"{attrValue}"'
                elif attrValue is None:
                    attrValue = 'NULL'

                # Append comma only if it's not the first attribute
                if not first_attr:
                    query_str += ', '
                query_str += f'{attrLabel} = {attrValue}'
                first_attr = False  # Update flag after the first attribute

            query_str += f' WHERE Ssn = {key}'
            status = self.execute_query_commit(query_str, ())
            if (status is not None):
                print(status)


    def promptForInput(self, keys):
        '''Takes 1 argument, list of attribute names. Returns dict where that argument is the key, and user
        response is value. Only has key/value pair for attributes users chose to respond to.'''
        responseDict = {}
        for k in keys:
            inputVal = input(f"Enter value for {k}, just enter NA to ignore attribute (for modify)")
            if (inputVal != 'NA'):
                responseDict[k] = inputVal
        return responseDict
    
    def functionSwitchStatement(self, operation, table):
        '''Takes 2 strings as input'''
        match (operation, table):
            case ('add', 'Employee'):
                self.add_Employee()
            case ('modify', 'Employee'):
                self.modify_Employee()
            case ('view', 'Employee'):
                self.view_Employee()
            case ('remove', 'Employee'):
                self.remove_Employee()
            case ('add', 'Dependent'):
                self.add_Dependent()
            case ('remove', 'Dependent'):
                self.remove_Dependent()
            case ('add', 'Department'):
                self.add_Department()
            case ('view', 'Department'):
                self.view_Department()
            case ('remove', 'Department'):
                self.remove_Department()
            case ('add', 'Dept_Location'):
                self.add_Dept_Location()
            case ('remove', 'Dept_Location'):
                self.remove_Dept_Location()
            case _:
                print("Invalid combination! Please retry!")
                self.start()
    
    def start(self):
        '''Prompts user for input on operation and table'''
        operation = input("Which operation do you want to do (add, modify, view, or remove)?")
        table = input("Which table is this for (Employee, Dependent, Department, Dept_Location)?")
        operation_options = ['add', 'modify', 'view', 'remove']
        table_options = ['Employee', 'Dependent', 'Department', 'Dept_Location']
        if operation in operation_options:
            if table in table_options:
                self.functionSwitchStatement(operation, table) # big switch case to find proper function
            else:
                print("That table does not exist. Retry.")
                self.start()
        else:
            print("That operation does not exist. Retry")
            self.start()
        
    def get_ordered_values(self, entries, fields):
        '''returns list in same order that the fields order is in'''
        return [entries[key] for key in fields if key in entries]


    def add_noLock(self, fields, keyLabel, query_str):
        '''fields as list, keyLabel as string, query_str as string. 
        Asks user for input, fills out query, and executes'''
        conn = self.connect()
        if (conn == 1):
            entries = self.promptForInput(fields)
            if (entries[keyLabel] is None):
                print(keyLabel, " is required.")
            else:
                values = self.get_ordered_values(entries, fields)
                status = self.execute_query_commit(query_str, values)
                if (status is not None):
                    print("Error: ", status)
                self.disconnect()
                self.start()
        else:
            print("There is an issue with your connection!")
        self.disconnect()
        self.start() # allows you to do another operation wihtout restarting program

    def modify_op(self, table, keyLabel, fields, fields2, lockQuery):
        '''table as string, keylabel as string, fields and fields2 as lists of strings,
        and lockQuery as string with %s in it. Runs modify query'''
        conn = self.connect()
        if (conn == 1):
            # get inputs
            entries = self.promptForInput(fields)
            values = self.get_ordered_values(entries, fields)
            if (entries[keyLabel] is None):
                print(keyLabel, " is required.")
                return -1
            key = entries[keyLabel]

            # make initial read
            sql_query1 = f'SELECT * FROM {table} WHERE ({keyLabel} = {key})'
            status1 = self.execute_query(sql_query1, ())
            if (status1 != None):
                print("Error:", status1)
                return status1
            details1 = self.cursor.fetchone()
            if (details1 is None):
                print("Incorrect Key. No record found using this key.")
                self.disconnect
                self.start()
            print("Select Details:", details1)

            # lock tables
            status2 = self.execute_query(lockQuery, ())
            if (status2 != None):
                print("Error:", status2)
                return status2

            # make edits
            entries = self.promptForInput(fields2)
            status3 = self.update_values(table, entries, key)
            
            if (status3 != None):
                print("Error:", status3)
                return status3

            # unlock tables
            sql_query4 = 'UNLOCK TABLES'
            status4 = self.execute_query(sql_query4, ())
            if (status4 != None):
                print("Error:", status4)
                return status4
        self.disconnect()
        self.start()
    
    def view_Employee(self):
        '''Takes user input and executes view employee queries. 
        Prints employee information, manager name, department name, and dependents names'''
        conn = self.connect()
        if (conn == 1):
            fields = ['Ssn']
            table = 'Employee'
            keyLabel = 'Ssn'

            # get inputs
            entries = self.promptForInput(fields)
            values = self.get_ordered_values(entries, fields)
            if (entries[keyLabel] is None):
                print(keyLabel, " is required.")
                self.start()
            key = entries[keyLabel]

            # make initial read
            sql_query1 = f'SELECT * FROM {table} WHERE ({keyLabel} = {key})'
            status1 = self.execute_query(sql_query1, ())
            if (status1 != None):
                print("Error:", status1)
                self.start()
            details1 = self.cursor.fetchone()
            if (details1 is None):
                print("Incorrect Key. No record found using this key.")
                self.disconnect()
                self.start()
            else:
                print("Select Details:", details1)

            # read Department
            dno = details1[9]
            sql_query2 = f'SELECT Dname FROM Department WHERE (Dnumber = {dno})'
            status2 = self.execute_query(sql_query2, ())
            if (status2 != None):
                print("Error:", status2)
                self.start()
            details2 = self.cursor.fetchone()
            print("Department Name:", details2)

            # read Supervisor Name
            superSsn = details1[8]
            sql_query3 = f'SELECT Fname, Minit, Lname FROM Employee WHERE (Ssn = {superSsn})'
            status3 = self.execute_query(sql_query3, ())
            if (status3 != None):
                print("Error:", status3)
                self.start()
            details3 = self.cursor.fetchone()
            print("Manager Name:", details3)

            # read dependents
            essn = details1[3]
            sql_query4 = f'SELECT Dependent_name FROM Dependent WHERE (Essn = {essn})'
            status4 = self.execute_query(sql_query4, ())
            if (status4 != None):
                print("Error:", status4)
                self.start()
            details4 = self.cursor.fetchall()
            print("Dependents:", details4)

            self.disconnect()
            self.start()

    def add_yesLock(self, table, lock_query, keyLabelForMain, keyLabelForSecondary, fields1, fields2, create_query):
        '''Takes table as string, lock_query as string, keylabels for main table and the dependency table, two lists 
        with str inputs and a create_query. Asks for user input, builds query, executes, and handles errors.'''
        conn = self.connect()
        if (conn == 1):
            entries = self.promptForInput(fields1)
            values = self.get_ordered_values(entries, fields1)
            if (entries[keyLabelForMain] is None):
                print(keyLabelForMain, " is required.")
                self.disconnect()
                return -1            
            key = entries[keyLabelForMain]

            # make initial read
            sql_query1 = f'SELECT * FROM {table} WHERE ({keyLabelForMain} = {key})'
            status1 = self.execute_query(sql_query1, ())
            if (status1 != None):
                print("Error:", status1)
                self.disconnect()
                return status1
            details1 = self.cursor.fetchall()
            if (details1 is None):
                print("Incorrect Key. No record found using this key.")
                self.disconnect
                self.start()
            print("Read Details:", details1)

            # lock tables
            status2 = self.execute_query(lock_query, ())
            if (status2 != None):
                print("Error:", status2)
                self.disconnect()
                return status2

            # write to dependents
            entries = self.promptForInput(fields2)
            if (entries[keyLabelForSecondary] is None):
                print(keyLabelForSecondary, " is required.")
                self.disconnect()
                return -1
            else:
                sql_query3 = create_query
                values = self.get_ordered_values(entries, fields2)
                status3 = self.execute_query_commit(sql_query3, values)
                if (status3 != None):
                    print("Error:", status3)
                    self.disconnect()
                    return status3
            
            # unlock tables
            sql_query4 = 'UNLOCK TABLES'
            status4 = self.execute_query(sql_query4, ())
            if (status4 != None):
                print("Error:", status4)
                self.disconnect()
                return status4
        self.disconnect()
        self.start()

    def remove_yesLock1(self, fields1, fields2, lockQuery, delete_query, table):
        '''Takes two lists of strings, string lock and delete queries plus table as str.
        builds queries, executes, error handles. Asks for confirmation before submitting.'''
        # remove_yesLock1 handles cases with no dependencies and supports multiple keys
        conn = self.connect()
        if (conn == 1):
            keyLabelForMain = fields1[0]
            entries = self.promptForInput(fields1)
            values = self.get_ordered_values(entries, fields1)
            if (entries[keyLabelForMain] is None):
                print(keyLabelForMain, " is required.")
                self.disconnect()
                return -1            
            key = entries[keyLabelForMain]

            # make initial read
            keyVal = entries[keyLabelForMain]
            keyL1 = fields2[0]
            sql_query1 = f'SELECT * FROM {table} WHERE ({keyL1} = {keyVal})'
            status1 = self.execute_query(sql_query1, ())
            if (status1 != None):
                print("Error:", status1)
                self.disconnect()
                return status1
            details1 = self.cursor.fetchall()
            if (details1 is None):
                print("Incorrect Key. No record found using this key.")
                self.disconnect
                self.start()
            print("Select Details:", details1)

            # lock tables
            status2 = self.execute_query(lockQuery, ())
            if (status2 != None):
                print("Error:", status2)
                self.disconnect()
                return status2

            # delete from table, handle multiple keys (dependents)
            entries = self.promptForInput(fields2)
            for keyItem in fields2:
                if entries[keyItem] is None:
                    print("Essn AND Dependent_name are required.")
                    self.disconnect()
                    return -1
            else:
                values = self.get_ordered_values(entries, fields2)

                confirmation = input("Do you want to delete? Y\\N")
                if (confirmation == 'N' or confirmation == 'n'): # if they decide against it
                    self.disconnect()
                    self.start()

                status3 = self.execute_query_commit(delete_query, values)
                if (status3 != None):
                    print("Error:", status3)
                    self.disconnect()
                    return status3
            
            # unlock tables
            sql_query4 = 'UNLOCK TABLES'
            status4 = self.execute_query(sql_query4, ())
            if (status4 != None):
                print("Error:", status4)
                self.disconnect()
                return status4
            
        self.disconnect()
        self.start()

    def view_Department(self):
        '''Prompts user for input, queries for department information, plus manager name
        and dept location. Handles errors.'''
        conn = self.connect()
        if (conn == 1):
            fields = ['Dnumber']
            entries = self.promptForInput(fields)

            # make initial read of dept data
            dnumber = entries['Dnumber']
            sql_query1 = f'SELECT * FROM DEPARTMENT WHERE (Dnumber = {dnumber})'
            status1 = self.execute_query(sql_query1, ())
            if (status1 != None):
                print("Error:", status1)
                self.start()
            details1 = self.cursor.fetchone()
            if (details1 is None):
                print("Incorrect Key. No record found using this key.")
                self.disconnect
                self.start()
            print("Department Details:", details1)
            
            # read mgr name
            superssn = details1[2]
            sql_query2 = f'SELECT Fname, Minit, Lname FROM EMPLOYEE WHERE (Ssn = {superssn})'
            status2 = self.execute_query(sql_query2, ())
            if (status2 != None):
                print("Error:", status2)
                self.start()
            details2 = self.cursor.fetchone()
            if (details1 is None):
                print("Incorrect Key. No record found using this key.")
                self.disconnect
                self.start()
            print("Manager Name:", details2)

            # read dept locations
            sql_query3 = f'SELECT Dlocation FROM Dept_Locations WHERE (Dnumber = {dnumber})'
            status3 = self.execute_query(sql_query3, ())
            if (status3 != None):
                print("Error:", status3)
                self.start()
            details3 = self.cursor.fetchall()
            print("Department Locations:", details3)
        else:
            print("Connection not established properly. Retry!")
        self.disconnect()
        self.start()

    def remove_yesLock2(self, fields, table, keyLabel, lock_query):
        '''Similar to remove_yesLock1, but for tables with only one key.
        Prompts for user input, generates query, asks to confirm, and deletes.
        Warns about existing dependencies.'''
        conn = self.connect()
        if (conn == 1):
            entries = self.promptForInput(fields)

            # make initial read of dept data
            keyVal = entries[keyLabel]
            sql_query1 = f'SELECT * FROM {table} WHERE ({keyLabel} = {keyVal})'
            status1 = self.execute_query(sql_query1, ())
            if (status1 != None):
                print("Error:", status1)
                self.start()
            details1 = self.cursor.fetchone()
            if (details1 is None):
                print("Incorrect Key. No record found using this key.")
                self.disconnect
                self.start()
            print("Read Details:", details1)

            # lock the relevant tables
            status2 = self.execute_query(lock_query, ())
            if (status2 != None):
                print("Error:", status2)
                self.disconnect()
                return status2

            confirmation = input("Do you want to delete? Y\\N")
            if (confirmation == 'N' or confirmation == 'n'): # if they decide against it
                self.disconnect()
                self.start()
            
            # attempt to delete the department
            sql_query3 = f'DELETE FROM {table} WHERE ({keyLabel} = {keyVal})'
            status3 = self.execute_query_commit(sql_query3, ())
            if (status3 != None):
                print("Error:", status3)
                print("MUST REMOVE THESE EXISTING DEPENDENCIES FIRST!")
                self.disconnect()
                return status3
        else:
            print("Connection not established properly. Retry!")
        self.disconnect()
        self.start()
        return None
    
    def add_Employee(self):
        '''Asks user for inputs regarding fields below, uses query_str + values as query for MySQL server.'''
        fields = ['Fname', 'Minit', 'Lname', 'Ssn', 'Bdate', 'Address', 'Sex', 'Salary', 'Super_Ssn', 'Dno']
        keyLabel = 'Ssn'
        query_str = 'INSERT INTO EMPLOYEE (Fname, Minit, Lname, Ssn, Bdate, Address, Sex, Salary, Super_ssn, Dno) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
        self.add_noLock(fields, keyLabel, query_str)

    def modify_Employee(self):
        '''Asks user for input for ssn (key) and modifiable values. lockQry + responses are query for MySQL Server'''
        fields = ['Ssn']
        fields2 = ['Address', 'Sex', 'Salary', 'Super_ssn', 'Dno']
        table = 'Employee'
        keyLabel = 'Ssn'
        lockQry ='LOCK TABLES Employee WRITE, Department WRITE, Dependent READ, Works_On READ'
        response = self.modify_op(table, keyLabel, fields, fields2, lockQry)
        # 1 if success, -1 if missing key, str for mysql error message

    def remove_Employee(self):
        '''Asks user for input for ssn (key), lock_query + user input are used as MySQL server query.'''
        fields = ['Ssn']
        table = 'EMPLOYEE'
        keyLabel = 'Ssn'
        lock_query = 'LOCK TABLES EMPLOYEE WRITE, DEPARTMENT READ, DEPT_LOCATIONS READ, PROJECT READ, WORKS_ON READ'
        self.remove_yesLock2(fields, table, keyLabel, lock_query)

    def add_Dependent(self):
        '''Asks user for ssn (key), as well as dependent attribute values. create_query + input are queried to MySQL server'''
        table = 'Employee'
        lock_query = 'LOCK TABLES DEPENDENT WRITE, EMPLOYEE READ'
        keyLabelForMain = 'Ssn'
        keyLabelForSecondary = 'Essn'
        fields1 = ['Ssn']
        fields2 = ['Essn', 'Dependent_Name', 'Sex', 'Bdate', 'Relationship']
        create_query = 'INSERT INTO DEPENDENT (Essn, Dependent_Name, Sex, Bdate, Relationship) VALUES (%s, %s, %s, %s, %s)'
        self.add_yesLock(table, lock_query, keyLabelForMain, keyLabelForSecondary, fields1, fields2, create_query)

    def remove_Dependent(self):
        '''Takes ssn (key) and user input for dependent table keys, uses lock query and delete_query + user input for MySQL server'''
        fields = ['Ssn']
        fields2 = ['Essn', 'Dependent_name']
        lockQuery = 'LOCK TABLES Employee READ, Dependent WRITE'
        delete_query = 'DELETE FROM DEPENDENT WHERE (Essn = %s AND Dependent_name = %s)'
        table = 'DEPENDENT'
        self.remove_yesLock1(fields, fields2, lockQuery, delete_query, table)

    def add_Department(self):
        '''Takes user input for department table attributes, query_str + user input queried to MySQL server'''
        fields = ['Dname', 'Dnumber', 'Mgr_ssn', 'Mgr_start_date']
        keyLabel = 'Dnumber'
        query_str = 'INSERT INTO DEPARTMENT (Dname, Dnumber, Mgr_ssn, Mgr_start_date) VALUES (%s, %s, %s, %s)'        
        self.add_noLock(fields, keyLabel, query_str)

    def remove_Department(self):
        '''User input for Dnumber, lock_query + user input queried to MySQL server'''
        fields = ['Dnumber']
        table = 'DEPARTMENT'
        keyLabel = 'Dnumber'
        lock_query = 'LOCK TABLES DEPARTMENT WRITE, EMPLOYEE READ, DEPT_LOCATIONS READ, PROJECT READ, WORKS_ON READ'
        self.remove_yesLock2(fields, table, keyLabel, lock_query)

    def add_Dept_Location(self):
        '''User input for Dnumber and Dlocation, paired with create_query and sent to MySQL server'''
        table = 'Department'
        lock_query = 'LOCK TABLES DEPT_LOCATIONS WRITE, DEPARTMENT READ'
        keyLabelForMain = 'Dnumber'
        keyLabelForSecondary = 'Dnumber'
        fields1 = ['Dnumber']
        fields2 = ['Dnumber', 'Dlocation']
        create_query = 'INSERT INTO DEPT_LOCATIONS (Dnumber, DLocation) VALUES (%s, %s)'
        self.add_yesLock(table, lock_query, keyLabelForMain, keyLabelForSecondary, fields1, fields2, create_query)
    
    def remove_Dept_Location(self):
        '''User input for Dnumber and Dlocation, lockQuery and delete_query + user input sent to MySQL server'''
        fields = ['Dnumber']
        fields2 = ['Dnumber', 'Dlocation']
        lockQuery = 'LOCK TABLES DEPT_LOCATIONS WRITE, EMPLOYEE READ, DEPARTMENT WRITE, PROJECT READ, WORKS_ON READ'
        delete_query = 'DELETE FROM DEPT_LOCATIONS WHERE (Dnumber = %s and Dlocation = %s)'
        table = 'DEPARTMENT'
        self.remove_yesLock1(fields, fields2, lockQuery, delete_query, table)
    
if __name__ == '__main__':
    hostValue = "localhost"
    usernameValue = "root"
    passwordValue = "password"
    databaseValue = "cse4701spring2024_project1_p1_swanson_tanner"

    db = database(hostValue, usernameValue, passwordValue, databaseValue)
    db.start()