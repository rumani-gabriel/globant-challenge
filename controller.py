from sql.db import get_db
import json

def insert_employee(name, datetime, department_id, job_id):
    db = get_db()
    cursor = db.cursor()
    statement = "INSERT INTO hired_employees(name, datetime, department_id, job_id)VALUES(?,?,?,?)"
    cursor.execute(statement, [name, datetime, department_id, job_id])

    if not all(val for val in [name, datetime, department_id, job_id]):
            return {"message": "All fields are mandatory."}, 400
    try:
            
            query = "INSERT INTO hired_employees (name, datetime, department_id, job_id) VALUES (%s, %s, %s, %s, %s)"
            cursor.execute(query, (name, datetime, department_id, job_id))
            db.commit()
            return {"message": "Employee added successfully."}, 201
    except Error as e:
        return {"error": str(e)}, 500
    finally:
        cursor.close()
        db.close()
    
    db.close()
    return True

def get_employees():
    db = get_db()
    cursor = db.cursor()
    query = "SELECT field1, field2, field3, field4, field5 FROM employees"
    cursor.execute(query)
    return cursor.fetchall()

def backup():
    db = get_db()
    cursor = db.cursor()
    query = "SELECT * FROM employees"
    cursor.execute(query)
    return cursor.fetchall()

def restore():
   with open('backup.json') as f:
    data = json.load(f)

    db = get_db()
    cursor = db.cursor()
    for row in data:
     query = 'INSERT INTO tablename (field1, field2, field3, field4, field5) VALUES ({row}:field1, {row}:field2, {row}:field3, {row}:field4, {row}:field5)'
     cursor.execute(query)
    
"""Number of employees hired for each job and department in 2021 divided by quarter. The table must be ordered alphabetically by department and job. 
"""
def query1():
    db = get_db()
    cursor = db.cursor()
    query = 'SELECT d.department as department, j.job as job, COUNT(CASE WHEN MONTH(datetime) IN (1,2,3) THEN 1 END) as "Q1",COUNT(CASE WHEN MONTH(datetime) IN (4,5,6) THEN 1 END) as "Q2", COUNT(CASE WHEN MONTH(datetime) IN (7,8,9) THEN 1 END) as "Q3", COUNT(CASE WHEN MONTH(datetime) IN (10,11,12) THEN 1 END) as "Q4" FROM employees e JOIN job j ON e.job = j.id JOIN department d ON e.department = d.id WHERE YEAR(datetime) = 2021 GROUP BY d.department, j.job'
    cursor.execute(query)
    return cursor.fetchall()

"""List of ids, name and number of employees hired of each department that hired more employees than the mean of employees hired in 2021 for all the departments, ordered by the number of employees hired (descending).
"""
def query2():
    db = get_db()
    cursor = db.cursor()
    query = 'WITH employee_department AS (SELECT department, COUNT(id) as contratcs FROM employees   WHERE YEAR(datetime) = 2021 GROUP BY department), average AS (SELECT AVG(empleados_contratados) as avg FROM employee_department)SELECT d.id, d.department, e.contratcs FROM  employee_department e JOIN department d ON e.department = d.id WHERE e.contracts > (SELECT avg FROM average) ORDER BY e.empleados_contratados DESC'
    cursor.execute(query)
    return cursor.fetchall()

