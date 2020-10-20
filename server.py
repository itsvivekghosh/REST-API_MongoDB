from apscheduler.scheduler import Scheduler
from flask import Flask, Response, request
from bson.objectid import ObjectId
from bson.son import SON
import pymongo
import warnings
import json
import datetime
import atexit


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
@app.route("/create", methods=['POST'])
def create_data():
    '''
        Function to create data
    '''
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
            'created_date': datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
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
@app.route("/find_by_current_date", methods=['GET'])
def find_by_current_date():
    '''
        Function to find current date (today's) users
    '''
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
@app.route('/count_current_date_users_by_10', methods=["GET"])
def find_by_userLength_by_10():
    '''
        Function to find today's regitered users data with the length as 10
    '''
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
@app.route('/count_current_date_users_by_12', methods=["GET"])
def find_by_userLength_by_12():
    '''
        Function to find today's regitered users data with the length as 12
    '''
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


#############################
def save_total_records_in_time():
    '''
    Scheduling a Job to save total records of current date 
    '''
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
        # Get current Date
        date_field = datetime.datetime.now().strftime('%Y-%m-%d')

        # find total users of current date
        result = db['total_users_by_date'].find_one(
            {
                'date': date_field,
            }
        )
        # new record(updated record)
        record = {
            "date": date_field,
            "total_users": len(data)
        }
        if result is not None:
            db['total_users_by_date'].replace_one(
                {
                    'date': date_field
                },
                record  # update with the new record (total users)
            )
        else:
            col = db['total_users_by_date'].insert_one(record)

        print(
            "Total {} No. of Record(s) saved into DATABASE: {} and Collection {}".format(
                len(data),
                DATABASE_NAME,
                date_field
            )
        )
    except Exception as exception:
        print("Error while retrieving or saving data:")
        print("*******************************")
        print(exception)
        print("*******************************")


##############################
def schedule_task(hr=11, min=30, sec=0):
    '''
    Scheduler to perform task (update total users into database)
    '''
    sched = Scheduler()
    sched.add_cron_job(
        save_total_records_in_time,
        day_of_week='mon-sun',
        # Timing to update the Total users by the current date
        hour=hr, minute=min, second=sec
    )
    # sched.add_cron_job(
    #     save_total_records_in_time,
    #     day_of_week='mon-sun',
    #     # Timing to update the Total users by the current date
    #     hour=hr, minute=min, second=sec
    # )
    sched.start()


#########################
def start_server(host='localhost', port=80):
    '''
    Running Server on the requirements
    '''
    try:
        app.run(
            host=host,
            port=port,
            debug=True,
            use_reloader=False,
        )
    except Exception as exception:
        print("Error while Running server on {}:{}".format(host, port))
        print("*******************************")
        print(exception)
        print("*******************************")


###########################
if __name__ == '__main__':
    '''
    Initialize the schedule to count the users as Thread
    and Starting the Flask server
    '''
    schedule_task(
        # set hr as Hour (24 hr 0-23) and min as minute (0-59)
        hr=1, min=22, sec=1
    )
    # count records at last of the day
    schedule_task(
        # set hr as Hour (24 hr 0-23) and min as minute (0-59)
        hr=23, min=58
    )
    # starting flask server
    start_server(
        host='localhost',
        port=80,
    )
