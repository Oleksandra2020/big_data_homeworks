from flask_restful import reqparse, Resource, Api
from cassandra_client import CassandraClient
from flask import Flask, request
from datetime import date

app = Flask(__name__)
api = Api(app)
app.config['BUNDLE_ERRORS'] = True


parser = reqparse.RequestParser()
parser.add_argument('request_num', type=int, required=True,
                    help='According to the task definition (from 1 to 3)')
parser.add_argument('uuid', type=str, required=True)
parser.add_argument('start_date', type=str, required=False)
parser.add_argument('end_date', type=str, required=False)

# curl --header "Content-Type: application/json" \
#   --request GET \
#   --data '{"request_num":1,"uuid":"C7162498"}' \
#   http://0.0.0.0:8080/

# curl --header "Content-Type: application/json" \
#   --request GET \
#   --data '{"request_num":2,"uuid":"C1572516402"}' \
#   http://0.0.0.0:8080/

# curl --header "Content-Type: application/json" \
#   --request GET \
#   --data '{"request_num":3,"uuid":"C7162498","start_date":"2021-05-23","end_date":"2022-10-10"}' \
#   http://0.0.0.0:8080/


class RetrieveData(Resource):

    def get(self):

        dct = {}
        args = request.get_json()
        client.connect()
        if args['request_num'] == 1:
            result = client.query_one(args['uuid'])
        elif args['request_num'] == 2:
            result = client.query_two(args['uuid'])
        elif args['request_num'] == 3:
            result = client.query_three(
                args['start_date'], args['end_date'], args['uuid'])
        else:
            client.close()
            return "Invalid request"

        res = {"transactions": []}
        for i in result:
            dct = {}

            dct['name_org'] = i.name_org
            dct['date'] = str(i.trans_date)
            dct['step'] = i.step
            dct['tp'] = i.tp
            dct['amount'] = i.amount
            dct['old_balance_org'] = i.old_balance_org
            dct['new_balance_org'] = i.new_balance_org
            dct['name_dest'] = i.name_dest
            dct['old_balance_dest'] = i.old_balance_dest
            dct['new_balance_dest'] = i.new_balance_dest
            dct['is_fraud'] = i.is_fraud
            dct['is_flagged_fraud'] = i.is_flagged_fraud

            res["transactions"].append(dct)

        return res


api.add_resource(RetrieveData, "/")

if __name__ == '__main__':
    host = 'cassandra-node1'
    port = 9042
    keyspace = 'hw8'
    client = CassandraClient(host, port, keyspace)
    app.run(debug=True, host="0.0.0.0", port="8080")
