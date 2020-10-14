from flask import Flask, Response, request
from bson.objectid import ObjectId
from bson.son import SON
import pymongo
import warnings
import json
import datetime

#######################
app = Flask(__name__)
DATABASE_NAME = "company"
#######################

try:
    client = pymongo.MongoClient(
        host='localhost',
        port=27017,
        serverSelectionTimeoutMS=100
    )
    db = client[DATABASE_NAME]  # Database Name
    client.server_info()  # trigger the exception if not connected to db
except:
    print("ERROR - Cannot connect to DATABASE: '{}'".format(DATABASE_NAME))

###########################
'''
	Function to create data
'''


@app.route("/create", methods=['POST'])
def create_data():
    try:
        print("Creating user...")
        user = {
            'status': request.form['status'],
            'txnid': request.form['txnid'],
            'send_otp_error_desc': SON([
                ('status', request.form['send_otp_error_desc_status']),
                ('username', request.form['send_otp_error_desc_username']),
                ('fptoken', request.form['send_otp_error_desc_fptoken']),
                ('txn', request.form['send_otp_error_desc_txn']),
            ]),
            'oauth_txn': request.form['oauth_txn'],
            'user': request.form['user'],
            'client_id': request.form['client_id'],
            'created_date': datetime.datetime.now().isoformat(),
            # 'created_date': datetime.datetime(2020, 10, 9, 7, 21, 38, 527653).isoformat(),
            "login_resp": SON(
                [
                    ('status', request.form['login_resp_status']),
                    ('username', request.form['login_resp_username']),
                ]
            )
        }
        dbResponse = db["data"].insert_one(user)
        response = {
            'message': "data created successfully",
            "id": f"{dbResponse.inserted_id}",
        }
        return Response(
            json.dumps(response),
            status=200,
            mimetype="application/json"
        )

    except Exception as ex:
        print("********************************")
        print(ex)
        print("********************************")
        response = {
            'message': "data not created",
        }
        return Response(
            json.dumps(response),
            status=500,
            mimetype="application/json"
        )


###########################
'''
	Function to find current date (today's) users
'''


@app.route("/find_by_current_date", methods=['GET'])
def find_by_current_date():
    try:
        data = list(
            db['data'].find(
                {
                    'created_date': {
                        "$gte": datetime.datetime.now().strftime('%Y-%m-%dT00:00:00'),
                        '$lte': datetime.datetime.now().strftime('%Y-%m-%dT23:59:999999'),
                    },
                }
            )
        )
        for user in data:
            user['_id'] = str(user['_id'])
        records = {
            "data": data,
            'no_of_records': len(data)
        }
        return Response(
            response=json.dumps(records),
            status=200,
            mimetype="application/json"
        )

    except Exception as ex:
        print("********************************")
        print(ex)
        print("********************************")
        response = {
            'message': "cannot read users created",
        }
        return Response(
            json.dumps(response),
            status=500,
            mimetype="application/json"
        )


###########################
'''
	Function to find today's regitered users data with the length as 10
'''


@app.route('/count_current_date_users_by_10', methods=["GET"])
def find_by_userLength_by_10():
    try:
        data = list(
            db['data'].find({
                '$and': [
                    {
                        'created_date': {
                            "$gte": datetime.datetime.now().strftime('%Y-%m-%dT00:00:00'),
                            '$lte': datetime.datetime.now().strftime('%Y-%m-%dT23:59:999999'),
                        },
                    },
                    {
                        'user': {
                            "$regex": "^[\s\S]{10}$"
                        }
                    }
                ]
            })
        )
        for user in data:
            user['_id'] = str(user['_id'])
        records = {
            "users": data,
            'no_of_records': len(data)
        }
        return Response(
            response=json.dumps(records),
            status=200,
            mimetype="application/json"
        )

    except Exception as ex:
        print("********************************")
        print(ex)
        print("********************************")
        response = {
            'message': "cannot read users created",
        }
        return Response(
            json.dumps(response),
            status=500,
            mimetype="application/json"
        )


###########################
'''
	Function to find today's regitered users data with the length as 12
'''


@app.route('/count_current_date_users_by_12', methods=["GET"])
def find_by_userLength_by_12():
    try:
        data = list(
            db['data'].find({
                '$and': [
                    {
                        'created_date': {  # calculating today's date
                            "$gte": datetime.datetime.now().strftime('%Y-%m-%dT00:00:00'),
                            '$lte': datetime.datetime.now().strftime('%Y-%m-%dT23:59:999999'),
                        },
                    },
                    {
                        'user': {
                            "$regex": "^[\s\S]{12}$"
                        }
                    }
                ]
            })
        )
        for user in data:
            user['_id'] = str(user['_id'])
        records = {
            "users": data,
            'no_of_records': len(data)
        }
        return Response(
            response=json.dumps(records),
            status=200,
            mimetype="application/json"
        )

    except Exception as ex:
        print("********************************")
        print(ex)
        print("********************************")
        response = {
            'message': "cannot read users created",
        }
        return Response(
            json.dumps(response),
            status=500,
            mimetype="application/json"
        )


###########################
'''
	start the server
'''
if __name__ == '__main__':
    app.run(
        host="localhost",
        port=80,
        debug=True
    )
