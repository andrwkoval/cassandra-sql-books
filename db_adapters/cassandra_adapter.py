from db_adapters.database_adapter import Database

from datetime import datetime, timedelta
from collections import Counter

from dse.cluster import Cluster
from dse.auth import PlainTextAuthProvider


class Cassandra(Database):
    def __init__(self, user, password, ips, keyspace="books"):
        auth_provider = PlainTextAuthProvider(user, password)
        cluster = Cluster(ips, auth_provider=auth_provider)
        self.session = cluster.connect(keyspace=keyspace)

    def reviews_by_product_id(self, id):
        result = self.session.execute("SELECT * FROM reviews_by_product WHERE product_id='{}';".format(id))
        return Cassandra.to_json(result)

    def reviews_by_product_id_and_star_rating(self, id, rating):
        result = self.session.execute(
            "SELECT * FROM reviews_by_product WHERE product_id='{}' AND star_rating={};".format(id, rating))
        return Cassandra.to_json(result)

    def reviews_by_customer_id(self, id):
        result = self.session.execute("SELECT * FROM reviews_by_customer WHERE customer_id={};".format(id))
        return Cassandra.to_json(result)

    def n_most_reviewed_items(self, start_date, end_date, n):
        result = self.session.execute(
            "SELECT product_id FROM reviews_by_date_star WHERE review_date IN ({});".format(",".join(list_from_date_period(start_date, end_date)))
        )
        result = self.session.execute(
            "SELECT product_id, product_category, product_title FROM bookstore.reviews_by_product WHERE product_id in ({}) GROUP BY product_id;"
                .format(", ".join([f"'{i[0]}'" for i in Cassandra.n_most(result, n)]))
        )
        return Cassandra.to_json(result)

    def n_most_productive_customers(self, start_date, end_date, n):
        dt_lst = ",".join(Cassandra.list_from_date_period(start_date, end_date))
        result = self.session.execute(
            "SELECT customer_id FROM reviews_by_date_star WHERE review_date IN ({}) AND verified_purchase=true;"
                .format(dt_lst)
        )
        return [{"customer_id": i[0]} for i in Cassandra.n_most(result, n, True)]

    def n_best_products_by_fraction(self, fraction, n):
        result = self.session.execute(
            "SELECT * FROM reviews_by_fraction_of_five WHERE fake_partition IN (0, 1) AND fraction_of_five={} LIMIT {};"
                .format(fraction, n)
        )
        return Cassandra.to_json(result)

    def n_productive_users(self, start_date, end_date, n, haters=True):
        result = self.session.execute(
            "SELECT customer_id FROM reviews_by_date_star WHERE review_date IN ({}) AND verified_purchase in (true, false) AND star_rating >= 4;"
                .format(",".join(Cassandra.list_from_date_period(start_date, end_date)))
        )
        if haters:
            result = self.session.execute(
                "SELECT customer_id FROM reviews_by_date_star WHERE review_date IN ({}) AND verified_purchase in (true, false) AND star_rating <=2;"
                    .format(",".join(Cassandra.list_from_date_period(start_date, end_date)))
        )
        return [{"customer_id": i[0]} for i in Cassandra.n_most(result, n, True)]

    @staticmethod
    def to_json(row_list):
        result = []
        for i in row_list:
            result.append(dict(i._asdict()))
        for i in result:
            if 'review_date' in i:
                i['review_date'] = str(i['review_date'])
        return result

    @staticmethod
    def n_most(result, n, customer=False):
        counter = Counter(map(lambda x: x.product_id, result)).most_common(n)
        if customer:
            counter = Counter(map(lambda x: x.customer_id, result)).most_common(n)
        return counter

    @staticmethod
    def list_from_date_period(start_date, end_date):
        result = []
        dt_start = datetime.strptime(start_date, '%Y-%m-%d')
        while dt_start <= datetime.strptime(end_date, '%Y-%m-%d'):
            result.append("'" + str(dt_start.date()) + "'")
            dt_start += timedelta(days=1)
        return result