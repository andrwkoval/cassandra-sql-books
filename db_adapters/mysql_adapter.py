import mysql.connector as sql
from db_adapters.database_adapter import Database

class MySQL_Adapter(Database):
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database

        self.connection = sql.connect(host=self.host,
                                      user=self.user,
                                      password=self.password,
                                      database=self.database)
        self.cursor = self.connection.cursor()

    def reviews_by_product_id(self, id):
        self.cursor.execute(f"select * from review where product_id={id};")
        return self._get_data()

    def reviews_by_product_id_and_star_rating(self, id, rating):
        self.cursor.execute(f"select * from review where product_id={id} and star_rating={rating};")
        return self._get_data()

    def reviews_by_customer_id(self, id):
        self.cursor.execute(f"select * from review where customer_id={id};")
        return self._get_data()

    def n_most_reviewed_items(self, start_date, end_date, n):
        self.cursor.execute("select product_id, product.title as title, category.name as category from review"
                            "left join product on review.product_id=product.id"
                            "left join product on product.category_id=category.id"
                            "where review_date >= {} and review_date <= {}"
                            "group by product_id order by count(*) desc limit {};".format(start_date, end_date, n))
        return self._get_data()

    def n_most_productive_customers(self, start_date, end_date, n):
        self.cursor.execute("select customer_id from review"
                            "where review_date >= {} and review_date <= {}"
                            "group by customer_id order by count(*) desc limit {};".format(start_date, end_date, n))
        return self._get_data()

    def n_best_products_by_fraction(self, fraction, n):
        self.cursor.execute(
            "select product.product_id, product.product_title, product.product_category, product.fraction_of_five from"
            "(select product_id, category.name as category, product.title as title,"
            "round(count(case when star_rating=5 then product_id end) / count(*),3) as fraction_of_five,"
            "count(*) as total_reviews"
            "from review"
            "left join product on review.product_id=product.id"
            "left join category on product.category_id=category.id"
            "where verified_purchase=true"
            "group by product_id) as product"
            "where product.fraction_of_five = {} and product.total_reviews >= 100"
            "order by fraction_of_five desc limit {};".format(fraction, n))
        return self._get_data()

    def n_productive_users(self, start_date, end_date, n, haters=True):
        query = "select customer_id from review"\
                "where review_date >= {} and review_date <= {} and star_rating <= 2"\
                "group by customer_id order by count(*) desc limit {};".format(start_date, end_date, n)
        if not haters:
            "select customer_id from review"
            "where review_date >= {} and review_date <= {} and star_rating >= 4"
            "group by customer_id order by count(*) desc limit {};".format(start_date, end_date, n)
        self.cursor.execute(query)
        return self._get_data()

    def _get_data(self):
        return list(dict(zip(self.cursor.column_names, i)) for i in self.cursor.fetchall())
