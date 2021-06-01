import random
from google.cloud import storage
from flask import Flask, request, make_response, Response
from google.cloud.storage import Blob
import os
import mysql.connector
from mysql.connector.constants import ClientFlag

app = Flask(__name__)

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


@app.route('/utilizatori/<user_id>/visits/<marker_id>', methods=["POST"])
def visits(user_id, marker_id):
    if request.method not in ["POST"]:
        return method_not_allowed(request.method, request.path, ["POST"])

    cnxn = mysql.connector.connect(**config)
    cursor = cnxn.cursor()
    resp = Response("")
    if request.method == "POST":
        if verifyUserExists(user_id) and  verifyMonumentExists(marker_id):
            cursor.execute("Insert INTO Vizited (user_id,marker_id) Values(%s,%s)" % (user_id, marker_id))
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


@app.route('/utilizatori/<user_id>/visits', methods=["GET"])
def visited(user_id):
    if request.method not in ["GET"]:
        return method_not_allowed(request.method, request.path, ["GET"])

    cnxn = mysql.connector.connect(**config)
    cursor = cnxn.cursor()
    resp = Response("")
    if request.method == "GET":
        cursor.execute("Select * from Vizited where user_id=%s" % user_id)
        out = cursor.fetchall()
        cnxn.close()
        myResponse = {
            bucket[0]: {"user_id": bucket[0], "monument_id": bucket[1]}
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
        if verifyDuplicat(nume_oras, monument_title) == True:
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


@app.route('/buckets', methods=["GET", "POST"])
def buckets():
    if request.method not in ["GET", "POST"]:
        return method_not_allowed(request.method, request.path, ["GET", "POST"])

    resp = Response("")

    if request.method == "GET":
        buckets = storage_client.list_buckets()
        buckets = list(filter(lambda x: "id" in x.labels.keys(), buckets))

        if buckets is []:
            buckets_dicts = {}
        else:
            buckets_dicts = {
                bucket.labels['id']: {"data": {"name": bucket.name, "storageClass": bucket.storage_class}, "Actions": {
                    "DeteleResource": {"URL": f"/buckets/{bucket.labels['id']}", "Method": "DELETE"}}} for bucket in
                buckets}

        resp = make_response(buckets_dicts)
        resp.status_code = 200
        resp.headers["Content-Type"] = "application/json"

    elif request.method == "POST":

        request_json = request.json

        if "name" not in request_json.keys() or "storageClass" not in request_json.keys():
            return bad_request(request.method, request.path)

        bucket_id = random.randint(1, int(2 ** 64))
        buckets = storage_client.list_buckets()

        if "id" in [label for label in [bkt.labels for bkt in buckets]]:
            bucket_ids = [i.labels["id"] for i in buckets]
        else:
            bucket_ids = []

        while bucket_id in bucket_ids:
            bucket_id = random.randint(1, int(2 ** 64))

        bucket = storage_client.bucket(request_json["name"])
        bucket.storage_class = request_json["storageClass"]

        bucket.labels = {"id": str(bucket_id)}

        print(bucket.labels)
        new_bucket = storage_client.create_bucket(bucket, location="europe-west3")

        response_dict = {"id": bucket.labels["id"], "Actions": {
            "DeleteResource": {"URL": f"/buckets/{bucket.labels['id']}", "Method": "DELETE"}
            , "ReplaceResource": {"URL": f"/buckets/{bucket.labels['id']}", "Method": "PUT"}
            , "GetResource": {"URL": f"/buckets/{bucket.labels['id']}", "Method": "GET"}}}

        myResponse = {

        }
        print(response_dict)
        resp = make_response(response_dict)

        resp.status_code = 200
        resp.headers["Content-Type"] = "application/json"
    return resp


@app.route('/buckets/<id>', methods=["GET", "PUT", "POST", "DELETE"])
def bucket(id):
    if request.method not in ["GET", "PUT", "POST", "DELETE"]:
        return method_not_allowed(request.method, request.path, ["GET", "PUT", "POST", "DELETE"])

    if request.method == "GET":

        bucket = getBucket(id)
        if bucket == []:
            bucket_dicts = {}
        else:
            buckets_dicts = {
                bucket.labels['id']: {"data": {"name": bucket.name, "storageClass": bucket.storage_class}, "Actions": {
                    "DeteleResource": {"URL": f"/buckets/{bucket.labels['id']}", "Method": "DELETE"}}}
            }
        resp = make_response(buckets_dicts)
        resp.status_code = 200
        resp.headers["Content-Type"] = "application/json"

    # pun pe id ul specificat
    if request.method == "PUT":
        request_json = request.json
        if "name" not in request_json.keys() or "storageClass" not in request_json.keys():
            return bad_request(request.method, request.path)

        deleteBucket(id)
        bucket = postBucket(request_json["name"], request_json["storageClass"], id)
        bucket_dicts = getBucketInfo(bucket)
        resp = make_response(bucket_dicts)

    # ignore
    if request.method == "POST":
        pass

    if request.method == "DELETE":
        resp = deleteBucket(id)
        pass

    return resp


@app.route("/buckets/<int:bct_id>/objects", methods=["GET", "POST"])
def objects(bct_id):
    if request.method not in ["GET", "POST"]:
        return method_not_allowed(request.method, request.path, ["GET", "POST"])

    response = {}

    if request.method == "GET":

        buckets = storage_client.list_buckets()
        bucket = next(filter(lambda x: "id" in x.labels.keys() and int(x.labels["id"]) == bct_id, buckets), -1)

        if bucket == -1:
            return not_found(f"{bct_id}", request.path)

        blobs = storage_client.list_blobs(bucket.name)
        blobs = list(filter(lambda x: x.metadata is not None and "id" in x.metadata.keys(), blobs))

        if blobs:
            blobs_dict = {blob.metadata["id"]: {"data": {"name": blob.name, "mime-type": blob.content_type,
                                                         "content": blob.download_as_bytes().decode()}, "Actions": {}}
                          for blob in blobs}
        else:
            blobs_dict = {}

        response = make_response(blobs_dict)
        response.status_code = 200
        response.headers["Content-Type"] = "application/json"

    elif request.method == "POST":

        if "data" not in request.json.keys() or "name" not in request.json["data"] or "content" not in request.json[
            "data"] or "mime-type" not in request.json["data"]:
            return bad_request(request.method, request.path)

        buckets = storage_client.list_buckets()
        bucket = next(filter(lambda x: "id" in x.labels.keys() and int(x.labels["id"]) == bct_id, buckets), -1)

        if bucket == -1:
            return not_found(f"{bct_id}", request.path)

        blob_id = random.randint(1, int(2 ** 64))
        blobs = storage_client.list_blobs(bucket.name)
        blob_ids = []
        if "id" in [meta for meta in [blob.metadata for blob in blobs]]:
            blob_ids = [i.metadata["id"] for i in blobs]

        while blob_id in blob_ids:
            blob_id = random.randint(1, int(2 ** 64))

        blob_object = bucket.blob(request.json["data"]["name"])
        blob_object.metadata = {"id": blob_id}

        # print(f"{request.json['data']['content']}  {type(request.json['data']['content'])}")

        blob_object.upload_from_string(request.json["data"]["content"], content_type=request.json["data"]["mime-type"])

        response_dict = {"id": blob_object.metadata["id"], "Actions": {
            "DeleteResource": {"URL": f"/buckets/{blob_object.metadata['id']}", "Method": "DELETE"}
            , "ReplaceResource": {"URL": f"/buckets/{blob_object.metadata['id']}", "Method": "PUT"}
            , "GetResource": {"URL": f"/buckets/{blob_object.metadata['id']}", "Method": "GET"}}}

        print(response_dict)
        response = make_response(response_dict)
        response.status_code = 201
        response.headers["Content-Type"] = "application/json"

    return response


@app.route('/buckets/<int:bkt_id>/objects/<int:obj_id>', methods=["GET", "PUT", "DELETE"])
def object(bkt_id, obj_id):
    if request.method not in ["GET", "PUT", "DELETE"]:
        return method_not_allowed(request.method, request.path, ["GET", "PUT", "DELETE"])

    response = {}

    if request.method == "GET":

        buckets = storage_client.list_buckets()
        bucket = next(filter(lambda x: "id" in x.labels.keys() and int(x.labels["id"]) == bkt_id, buckets), -1)
        # bucket = next(filter(lambda x: "id" in x.labels.keys() and int(x.labels["id"]) == bct_id, buckets), -1)
        if bucket == -1:
            return not_found(f"{bkt_id}", request.path)

        blobs = storage_client.list_blobs(bucket.name)
        blob = next(
            filter(lambda x: x.metadata is not None and "id" in x.metadata.keys() and int(x.metadata["id"]) == obj_id,
                   blobs), -1)

        if blob != -1:
            blobs_dict = {blob.metadata["id"]: {"data": {"name": blob.name, "mime-type": blob.content_type,
                                                         "content": blob.download_as_bytes().decode()}, "Actions": {}}}
        else:
            blobs_dict = {}

        response = make_response(blobs_dict)
        response.status_code = 200
        response.headers["Content-Type"] = "application/json"

    elif request.method == "DELETE":

        buckets = storage_client.list_buckets()
        bucket = next(filter(lambda x: "id" in x.labels.keys() and int(x.labels["id"]) == bkt_id, buckets), -1)
        # bucket = next(filter(lambda x: "id" in x.labels.keys() and int(x.labels["id"]) == bct_id, buckets), -1)
        if bucket == -1:
            return not_found(f"{bkt_id}", request.path)

        blobs = storage_client.list_blobs(bucket.name)
        blob = next(
            filter(lambda x: x.metadata is not None and "id" in x.metadata.keys() and int(x.metadata["id"]) == obj_id,
                   blobs), -1)

        if blob != -1:
            blob.delete()
        else:
            return not_found(f"Object {obj_id}", request.path)

    return response


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
def addMonument(Oras, Titlu, Descriere, Latitudine, Longitudine, ):
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
