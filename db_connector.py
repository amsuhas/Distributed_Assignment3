import subprocess

# Command to execute
command = "systemctl start mysql"

# Execute the command
process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

# Read the output and error
output, error = process.communicate()

# Check if there was an error
if process.returncode != 0:
    print(f"Error: {error.decode('utf-8')}")
else:
    print(f"Output: {output.decode('utf-8')}")



# import mysql.connector

# # Establishing a connection to the MySQL server
# connection = mysql.connector.connect(
#     host="localhost",
#     user="",
#     password="",
#     database="Student_info"
# )

# # Creating a cursor object using the cursor() method
# cursor = connection.cursor()

# create_table_query = """
# CREATE TABLE students (
#     id INT AUTO_INCREMENT PRIMARY KEY,
#     name VARCHAR(255),
#     marks INT
# )
# """
# cursor.execute(create_table_query)

# # add enties to the table
# add_student_query = """
# INSERT INTO students (name, marks)
# VALUES
# ('John', 90),
# ('Alice', 95),
# ('Bob', 85)
# """
# cursor.execute(add_student_query)

# # Commit your changes in the database
# connection.commit()

# # Example: executing a SQL query
# cursor.execute("SELECT * FROM students")

# # Fetching rows from the executed query
# rows = cursor.fetchall()

# # Displaying the fetched rows
# for row in rows:
#     print(row)

# # Closing the cursor
# cursor.close()

# # Closing the connection
# connection.close()
