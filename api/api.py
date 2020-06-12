from flask import Flask, jsonify
from db_adapters.cassandra_adapter import Cassandra
from db_adapters.mysql_adapter import MySQL_Adapter

app = Flask(__name__)


@app.route('/reviews_by_product_id/<string:prod_id>', methods=["GET"])
def reviews_by_product_id(prod_id):
    return jsonify(database.reviews_by_product_id(prod_id)), 200


@app.route('/reviews_by_product_id_and_star_rating/<string:prod_id>/<int:rating>', methods=["GET"])
def reviews_by_product_id_and_star_rating(prod_id, rating):
    return jsonify(database.reviews_by_product_id_and_star_rating(prod_id, rating)), 200


@app.route('/reviews_by_customer_id/<int:cust_id>', methods=["GET"])
def reviews_by_customer_id(cust_id):
    return jsonify(database.reviews_by_customer_id(cust_id)), 200


@app.route('/n_most_reviewed_items/<string:start_date>/<string:end_date>/<int:n>', methods=["GET"])
def n_most_reviewed_items(start_date, end_date, n):
    return jsonify(database.n_most_reviewed_items(start_date, end_date, n)), 200


@app.route('/n_most_productive_customers/<string:start_date>/<string:end_date>/<int:n>', methods=["GET"])
def n_most_productive_customers(start_date, end_date, n):
    return jsonify(database.n_most_productive_customers(start_date, end_date, n)), 200


@app.route('/n_best_products_by_fraction/<float:fraction>/<int:n>', methods=["GET"])
def n_best_products_by_fraction(fraction, n):
    return jsonify(database.n_best_products_by_fraction(fraction, n)), 200


@app.route('/most_productive_haters/<string:start_date>/<string:end_date>/<int:n>', methods=["GET"])
def most_productive_haters(start_date, end_date, n):
    return jsonify(database.n_productive_users(start_date, end_date, n)), 200


@app.route('/most_productive_backers/<string:start_date>/<string:end_date>/<int:n>', methods=["GET"])
def most_productive_backers(start_date, end_date, n):
    return jsonify(database.n_productive_users(start_date, end_date, n, False)), 200


if __name__ == "__main__":
    cassandra = Cassandra("<user>", "<password>", ["ip"], "<keyspace>")
    mysql = MySQL_Adapter("<host>", "<user>", "<password>", "<database>")

    database = cassandra
    app.run(debug=False)

