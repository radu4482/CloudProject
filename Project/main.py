import random
import mysql.connector
from mysql.connector.constants import ClientFlag
from google.cloud import storage
from flask import Flask, request, make_response, Response
from flask_cors import CORS, cross_origin
from google.cloud.storage import Blob

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

storage_client = storage.Client()

config = {
    'user': 'root',
    'password': '',
    'host': '34.116.234.12',
    'client_flags': [ClientFlag.SSL],
    'ssl_ca': 'server-ca.pem',
    'ssl_cert': 'client-cert.pem',
    'ssl_key': 'client-key.pem',
    'database': 'Markers'
}


@app.errorhandler(404)
def not_found(item, url):
    return {"Item": item, "Status": "Resource not found",
            "Actions": {"CreateResourceById": {"URL": url, "METHOD": "PUT"},
                        "CreateResourceBlindly": {"URL": url.split("/")[0], "METHOD": "POST"}}}


@app.errorhandler(405)
def method_not_allowed(method, url, allowed_methods):
    return {"URL": url, "UnallowedMethod": method, "AllowedMethods": allowed_methods}


@app.errorhandler(400)
def bad_request(method, url):
    return {"URL": url, "Method": method, "Status": "Bad request"}


@app.route('/utilizatori', methods=["GET", "POST"])
def utilizatorList():
    if request.method not in ["GET", "POST"]:
        return method_not_allowed(request.method, request.path, ["GET", "POST"])

    cnxn = mysql.connector.connect(**config)
    cursor = cnxn.cursor()
    resp = Response("")
    if request.method == "GET":
        cursor.execute("Select * from Utilizator")
        out = cursor.fetchall()
        cnxn.close()
        myResponse = {
            bucket[0]: {"Username": bucket[1], "mail": bucket[2], "password": bucket[3]}
            for bucket in out}
    if request.method == "POST":
        request_json = request.json
        if "usermane" not in request_json.keys() or "mail" not in request_json.keys() or "password" not in request_json.keys():
            return bad_request(request.method, request.path)

        cursor.execute("INSERT INTO Utilizator (username,mail,password) values ('%s','%s','%s')" % (
        request_json["usermane"], request_json["mail"], request_json["password"]))
        cnxn.commit()
        out = cursor.fetchall()
        print(out)
        myResponse = {
            "Response": "Everything ok"
        }

    resp = make_response(myResponse)
    resp.status_code = 200
    resp.headers["Content-Type"] = "application/json"
    return resp



@app.route('/utilizatori/<id>', methods=["GET"])
def utilizatorById(id):
    if request.method not in ["GET"]:
        return method_not_allowed(request.method, request.path, ["GET", "POST"])

    cnxn = mysql.connector.connect(**config)
    cursor = cnxn.cursor()
    resp = Response("")
    if request.method == "GET":
        cursor.execute("Select * from Utilizator where user_id=%s" % id)
        out = cursor.fetchall()
        cnxn.close()
        myResponse = {
            bucket[0]: {"Username": bucket[1], "mail": bucket[2], "password": bucket[3]}
            for bucket in out}

    resp = make_response(myResponse)
    resp.status_code = 200
    resp.headers["Content-Type"] = "application/json"
    return resp


@app.route('/utilizator/<username>', methods=["GET"])
def utilizatorByName(username):
    if request.method not in ["GET"]:
        return method_not_allowed(request.method, request.path, ["GET", "POST"])

    cnxn = mysql.connector.connect(**config)
    cursor = cnxn.cursor()
    resp = Response("")
    if request.method == "GET":
        cursor.execute("Select * from Utilizator where username='%s'" % username)
        out = cursor.fetchall()
        cnxn.close()
        myResponse = {
            bucket[1]: {"id": bucket[0], "Username": bucket[1], "mail": bucket[2], "password": bucket[3]}
            for bucket in out}

    resp = make_response(myResponse)
    resp.status_code = 200
    resp.headers["Content-Type"] = "application/json"
    return resp


@app.route('/utilizatori/<username>/visits/<oras>/<marker_name>', methods=["POST"])
def visits(username,oras, marker_name):
    if request.method not in ["POST"]:
        return method_not_allowed(request.method, request.path, ["POST"])

    cnxn = mysql.connector.connect(**config)
    cursor = cnxn.cursor()
    resp = Response("")
    if request.method == "POST":
        #if verifyUserExists(user_name) and  verifyMonumentExists(marker_id):
        cursor.execute("Insert INTO Vizited Select Utilizator.user_id,Markers.marker_id FROM Markers "
                       "INNER JOIN Utilizator WHERE Markers.Oras='%s' and Markers.Titlu='%s' and Utilizator.username='%s'"% (oras,marker_name,username))
        out = cursor.fetchall()
        cnxn.commit()
        cnxn.close()
        myResponse = {
            bucket[0]: {"user_id": bucket[0], "monument_id": bucket[1]}
            for bucket in out}
        resp = make_response(myResponse)
        resp.status_code = 200
        resp.headers["Content-Type"] = "application/json"
        return resp
    else:
        myResponse = {
            "Response": "Some id is not ok"
        }
        resp = make_response(myResponse)
        resp.status_code = 404
        resp.headers["Content-Type"] = "application/json"


@app.route('/utilizatori/<user_name>/visits/<oras>', methods=["GET"])
def visited(user_name,oras):
    if request.method not in ["GET"]:
        return method_not_allowed(request.method, request.path, ["GET"])

    cnxn = mysql.connector.connect(**config)
    cursor = cnxn.cursor()
    resp = Response("")
    if request.method == "GET":
        cursor.execute("Select Markers.marker_id, Markers.Titlu, Markers.Oras, Markers.Latitudine, Markers.Longitudine, Markers.URL  from Markers "
                       "INNER JOIN Vizited ON Markers.marker_id=Vizited.marker_id "
                       "JOIN Utilizator ON Utilizator.user_id=Vizited.user_id and Utilizator.username='%s' "
                       "WHERE Markers.Oras='%s'" % (user_name,oras))
        out = cursor.fetchall()
        cnxn.close()
        myResponse = {
            bucket[0]:{"Titlu": bucket[1], "Oras": bucket[2],"Latitudine": bucket[3],"Longitudine": bucket[4],"URL": bucket[5]}
            for bucket in out}

    resp = make_response(myResponse)
    resp.status_code = 200
    resp.headers["Content-Type"] = "application/json"
    return resp



@app.route('/monuments', methods=["GET", "POST"])
def allMonuments():
    if request.method not in ["GET", "POST"]:
        return method_not_allowed(request.method, request.path, ["GET", "POST"])

    resp = Response("")
    if request.method == "GET":
        monuments = selectFromSQL()
        buckets_dicts = {
            bucket[0]: {"Oras": bucket[5], "Descriere": bucket[1], "Latitudine": bucket[2], "Longitudine": bucket[3],
                        "URL": bucket[4],"ID":bucket[6]}
            for bucket in monuments}

        resp = make_response(buckets_dicts)
        resp.status_code = 200
        resp.headers["Content-Type"] = "application/json"

    elif request.method == "POST":
        request_json = request.json

        if verifyDuplicat(request_json["Oras"], request_json["Titlu"]) == True:
            myResponse = {
                "Response": "This monument is allready registered in this city"
            }
            resp = make_response(myResponse)
            resp.status_code = 440  # este un cod pus la intamplare
            resp.headers["Content-Type"] = "application/json"
        else:
            insertIntoSQL(request_json["Titlu"], request_json["Oras"], request_json["Descriere"],
                          request_json["Latitudine"],
                          request_json["Longitudine"], request_json["URL"])

            myResponse = {
                "Response": "Everything ok"
            }
            resp = make_response(myResponse)
            resp.status_code = 200
            resp.headers["Content-Type"] = "application/json"

    return resp


@app.route('/monuments/<nume_oras>', methods=["GET", "POST"])
def cityMonuments(nume_oras):
    if request.method not in ["GET", "POST"]:
        return method_not_allowed(request.method, request.path, ["GET", "POST"])

    resp = Response("")
    if request.method == "GET":
        monuments = monumentsFromCity(nume_oras)
        print(monuments)

        buckets_dicts = {
            bucket[0]: {"Oras": bucket[5], "Descriere": bucket[1], "Latitudine": bucket[2], "Longitudine": bucket[3],
                        "URL": bucket[4],"ID":bucket[6]}
            for bucket in monuments}

        resp = make_response(buckets_dicts)
        resp.status_code = 200
        resp.headers["Content-Type"] = "application/json"

    elif request.method == "POST":
        request_json = request.json

        if "Titlu" not in request_json.keys():
            return bad_request(request.method, request.path)

        if verifyDuplicat(nume_oras, request_json["Titlu"]) == True:
            myResponse = {
                "Response": "This monument is allready registered in this city"
            }
            resp = make_response(myResponse)
            resp.status_code = 440  # este un cod pus la intamplare
            resp.headers["Content-Type"] = "application/json"
        else:
            insertIntoSQL(request_json["Titlu"], nume_oras, request_json["Descriere"], request_json["Latitudine"],
                          request_json["Longitudine"], request_json["URL"])

            myResponse = {
                "Response": "Everything ok"
            }
            resp = make_response(myResponse)
            resp.status_code = 200
            resp.headers["Content-Type"] = "application/json"

    return resp


@app.route('/monuments/<nume_oras>/<monument_title>', methods=["GET", "POST", "PUT", "DELETE"])
def cityMonument(nume_oras, monument_title):
    if request.method not in ["GET", "POST", "PUT", "DELETE"]:
        return method_not_allowed(request.method, request.path, ["GET", "POST", "PUT", "DELETE"])

    resp = Response("")

    if request.method == "GET":
        monument = getMonument(nume_oras, monument_title)
        if monument == []:
            myResponse = {
                "Response": "No monument found"
            }
            resp = make_response(myResponse)
            resp.status_code = 404  # not found
            resp.headers["Content-Type"] = "application/json"
        else:
            monument_details = {
                bucket[0]: {"Oras": bucket[5], "Descriere": bucket[1], "Latitudine": bucket[2],
                            "Longitudine": bucket[3],
                            "URL": bucket[4], "ID": bucket[6]}
                for bucket in monument}

            resp = make_response(monument_details)
            resp.status_code = 200
            resp.headers["Content-Type"] = "application/json"

    if request.method == "POST":
        request_json = request.json
        if verifyDuplicat(nume_oras, monument_title)== True :
            myResponse = {
                "Response": "This monument is allready registered in this city"
            }
            resp = make_response(myResponse)
            resp.status_code = 440  # este un cod pus la intamplare
            resp.headers["Content-Type"] = "application/json"
        else:
            insertIntoSQL(monument_title, nume_oras, request_json["Descriere"], request_json["Latitudine"],
                          request_json["Longitudine"], request_json["URL"])

            myResponse = {
                "Response": "Everything ok"
            }
            resp = make_response(myResponse)
            resp.status_code = 200
            resp.headers["Content-Type"] = "application/json"
    if request.method == "PUT": a = 1
    if request.method == "DELETE":
        if verifyDuplicat(nume_oras, monument_title):
            print("yes")
        else:
            print("no")
        deleteMonument(nume_oras, monument_title)
        if verifyDuplicat(nume_oras, monument_title):
            print("yes")
        else:
            print("no")
        myResponse = {
            "Response": "Deleted"
        }
        resp = make_response(myResponse)
        resp.status_code = 200  # not found
        resp.headers["Content-Type"] = "application/json"
    return resp

def insertIntoSQL(Titlu, Oras, Descriere, Latitudine, Longitudine, URL):
    cnxn = mysql.connector.connect(**config)
    cursor = cnxn.cursor()
    print(Titlu + " " + Oras + " " + Descriere + " " + Latitudine + " " + Longitudine + " " + URL)
    cursor.execute(
        """INSERT INTO Markers (Titlu, Oras ,Descriere,Latitudine,Longitudine,URL) VALUES ('%s', '%s', '%s', '%s', '%s','%s')""" % (
            Titlu, Oras, Descriere, Latitudine, Longitudine, URL))
    cnxn.commit()
    cnxn.close()


def selectFromSQL():
    cnxn = mysql.connector.connect(**config)
    cursor = cnxn.cursor()
    cursor.execute("SELECT * FROM Markers")
    out = cursor.fetchall()
    cnxn.close()
    return out


# De adaugat conditia, probabil CITY
def monumentsFromCity(oras):
    cnxn = mysql.connector.connect(**config)
    cursor = cnxn.cursor()
    cursor.execute("SELECT * FROM Markers Where Oras='%s'" % oras)
    out = cursor.fetchall()
    cnxn.close()
    return out


def deleteMonument(Oras, Titlu):
    cnxn = mysql.connector.connect(**config)
    cursor = cnxn.cursor()
    cursor.execute("delete from Markers where Oras='%s' and Titlu='%s'" % (Oras, Titlu))
    cnxn.commit()
    out = cursor.fetchall()
    for i in out:
        print(i)
    cnxn.close()
    return out


# Same as inserIntoSQL
def addMonument(Oras, Titlu, Descriere, Latitudine, Longitudine, URL):
    cnxn = mysql.connector.connect(**config)
    cursor = cnxn.cursor()
    cursor.execute(
        """INSERT INTO Markers (Titlu, Oras ,Descriere,Latitudine,Longitudine,URL) VALUES ('%s', '%s', '%s', '%s', '%s','%s')""" % (
            Titlu, Oras, Descriere, Latitudine, Longitudine, URL))
    cnxn.commit()
    cnxn.close()


def verifyDuplicat(Oras, Titlu):
    out = getMonument(Oras, Titlu)
    if out == []:
        return False
    else:
        return True


def getMonument(Oras, Titlu):
    cnxn = mysql.connector.connect(**config)
    cursor = cnxn.cursor()
    cursor.execute("SELECT * FROM Markers where Oras='%s' and Titlu='%s'" % (Oras, Titlu))
    out = cursor.fetchall()
    return out


def verifyUserExists(id):
    cnxn = mysql.connector.connect(**config)
    cursor = cnxn.cursor()
    cursor.execute("SELECT * FROM Utilizator where user_id='%s'" % id)
    out = cursor.fetchall()
    if out == []:
        return False
    else:
        return True


def verifyMonumentExists(id):
    cnxn = mysql.connector.connect(**config)
    cursor = cnxn.cursor()
    cursor.execute("SELECT * FROM Markers where marker_id='%s'" % id)
    out = cursor.fetchall()
    if out == []:
        return False
    else:
        return True


if __name__ == "__main__":
    selectFromSQL()
    app.run('127.0.0.1', 8080)
