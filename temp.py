# a=5
# print(a)

# def func():
#     a = 10
#     print(a)

# func()
    
# print(a)

import mysql.connector

connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Password",
    # database="Student_info"
    # autocommit=False
)

connection2 = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Password",
    # database="Student_info"
)

cursor = connection.cursor()
cursor2 = connection2.cursor()
# cursor.execute("Commit")
# cursor.execute("CREATE DATABASE IF Not Exists  Student_info")
# cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
cursor.execute("USE Student_info")
cursor2.execute("USE Student_info")
# cursor.execute("Start Transaction;")
# # cursor.execute("CREATE TABLE Student (name VARCHAR(255), age INTEGER(10))")
# cursor.execute("INSERT INTO Student (name, age) VALUES ('SITIT', 21);")
# connection.commit()
# cursor.execute("Commit")
# cursor.execute("Use dt")
cursor2.execute("Select * from Student;")
a = cursor2.fetchall()
print(a)
# connection.commit()
# cursor.execute("Insert INTO stud (name, age) VALUES ('POPPY', 22)")
# cursor.execute("Insert INTO stud (name, age) VALUES ('DPS', 23)")
# cursor.execute("Commit")
# connection.rollback()
# cursor.execute("Commit")
# cursor.execute("rollback")
# connection.commit()
# connection.rollback()
connection.commit()

connection.close()

