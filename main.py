from flask import Flask, jsonify, request
import controller
from sql.db import create_employees
import pandas as pd
from flask import Flask, send_file
from fastavro import schema, reader
import fastavro
import json

app = Flask(__name__)

"""in this small block I used a little engineering to load the records into the database"""
df = pd.read_csv("employees.csv")
dfn = df[df.columns[1]]
dfd = df[df.columns[2]]
dfdep = df[df.columns[3]]
dfj = df[df.columns[4]]

names = []
dates = []
dep = []
jobs = []

"""at the beginning I load the lists that in the next url I will iterate to load the records"""
@app.route('/')
def home():
    for i in range(len(dfn)):
        names.append(dfn[i])
        dates.append(dfd[i])
        dep.append(dfdep[i])
        jobs.append(dfj[i])

    print(names, dates, dep, jobs)
    return"<h1>Welcome</h1>"

"""at the start, the method that loads the records through an iteration is executed"""
@app.route('/employee', methods=['POST'])
def insert_employee():
    
    for i in range(len(names)):

        employee_details = request.get_json()
        name = employee_details[names[i]]
        datetime = employee_details[dates[i]]
        department = employee_details [dep[i]]
        job = employee_details[jobs[i]]
        result = controller.insert_employee(name, datetime, department, job)
        return jsonify(result)

"""get all the records in the table"""
@app.route('/get_employees', methods=['GET'])
def get_employees():
    employees = controller.get_employees()
    return jsonify(employees)

"""an avro file is made to have a backup of the table in question"""
@app.route('/backup', methods=['GET'])
def do_backup():
     back = controller.backup()
     backup = jsonify(back)
     with open('backup.avro', 'wb') as out:
        fastavro.writer(out, schema, backup)
     return send_file('backup.avro', as_attachment=True)

"""the avro file is read and converted to json to be used to restore the table"""
@app.route('/read')
def read():
    with open('backup.avro', 'rb') as fo:
        avro_reader = reader(fo)
        schema = schema
        records = [r for r in avro_reader]
    
    json_data = json.dumps(records)

    with open('backup.json', 'w') as f:
     f.write(json_data)

"""table restore is applied"""
@app.route('/restore')
def restore():
    controller.restore()


@app.route('/query1', methods=['GET'])
def query1():
    query = controller.query1()
    return jsonify(query)

@app.route('/query2', methods=['GET'])
def query2():
    query = controller.query1()
    return jsonify(query)

if __name__ == "__main__":
  
    
    app.run(host='127.0.0.1',port=4000, debug=True)