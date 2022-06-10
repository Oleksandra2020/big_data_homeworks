from flask_restful import reqparse, Resource, Api
from cassandra_client import CassandraClient
from flask import Flask, request
import datetime

app = Flask(__name__)
api = Api(app)
app.config['BUNDLE_ERRORS'] = True


parser = reqparse.RequestParser()
parser.add_argument('request_num', type=int, required=True,
                    help='According to the task definition (from 1 to 3)')
parser.add_argument('product_id', type=str, required=False)
parser.add_argument('customer_id', type=str, required=False)
parser.add_argument('review_id', type=str, required=False)
parser.add_argument('star_rating', type=str, required=False)
parser.add_argument('start_date', type=str, required=False)
parser.add_argument('end_date', type=str, required=False)
parser.add_argument('N', type=str, required=False)

class RetrieveData(Resource):

    def get(self):

        dct = {}
        args = request.get_json()
        client.connect()
        if args['request_num'] == 1:
            result = client.query_one(args['product_id'])
        elif args['request_num'] == 2:
            result = client.query_two(args['product_id'], args['star_rating'])
        elif args['request_num'] == 3:
            result = client.query_three(args['customer_id'])
        elif args['request_num'] == 4:
            result = client.query_four()
        elif args['request_num'] == 5:
            result = client.query_five()
        elif args['request_num'] == 6:
            result = client.query_six_seven()
        elif args['request_num'] == 7:
            result = client.query_six_seven()
        else:
            client.close()
            return "Invalid request"

        if args['request_num'] < 4:
            res = {"reviews": []}
            for i in result:
                dct = {}
                dct["review_id"] = i.review_id

            res["reviews"].append(dct)
        elif args['request_num'] == 4:
            dct = {}
            start_date = datetime.datetime.strptime(
                args['start_date'], '%Y-%m-%d')
            end_date = datetime.datetime.strptime(args['end_date'], '%Y-%m-%d')

            for i in result:
                review_date = datetime.datetime.strptime(
                    str(i.review_date), '%Y-%m-%d')
                if review_date > start_date and review_date < end_date:
                    if i.product_id not in dct:
                        dct[i.product_id] = 0
                    dct[i.product_id] += i.count
            res = {"products": []}

        elif args['request_num'] == 5:
            dct = {}
            start_date = datetime.datetime.strptime(
                args['start_date'], '%Y-%m-%d')
            end_date = datetime.datetime.strptime(args['end_date'], '%Y-%m-%d')

            for i in result:
                review_date = datetime.datetime.strptime(
                    str(i.review_date), '%Y-%m-%d')
                if review_date > start_date and review_date < end_date and i.verified_purchase == 'Y':
                    if i.customer_id not in dct:
                        dct[i.customer_id] = 0
                    dct[i.customer_id] += i.count
            res = {"customers": []}

        elif args['request_num'] == 6 or args['request_num'] == 7:
            dct = {}
            start_date = datetime.datetime.strptime(
                args['start_date'], '%Y-%m-%d')
            end_date = datetime.datetime.strptime(args['end_date'], '%Y-%m-%d')

            for i in result:
                review_date = datetime.datetime.strptime(
                    str(i.review_date), '%Y-%m-%d')
                stars = i.star_rating
                if args['request_num'] == 6:
                    if stars == 1 or stars == 2:
                        continue
                else:
                    if stars == 4 or stars == 5:
                        continue
                if review_date > start_date and review_date < end_date:
                    if i.customer_id not in dct:
                        dct[i.customer_id] = 0
                    dct[i.customer_id] += i.count
            res = {"customers": []}

        if args['request_num'] >= 4:
            max_values = sorted(dct.values(), reverse=True)[:args['N']]
            reverse_dct = {}
            for i in dct:
                if dct[i] not in reverse_dct:
                    reverse_dct[dct[i]] = []
                reverse_dct[dct[i]].append(i)

            count = 0
            for i in max_values:
                if count == args["N"]:
                    break
                val = reverse_dct[i][:args['N']-count]

                for j in val:
                    res[list(res.keys())[0]].append([j, i])
                count += len(val)

        return res


api.add_resource(RetrieveData, "/")

if __name__ == '__main__':
    host = 'cassandra-node1'
    port = 9042
    keyspace = 'hw4'
    client = CassandraClient(host, port, keyspace)
    app.run(debug=True, host="0.0.0.0", port="8080")
