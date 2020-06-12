class Database:
    def reviews_by_product_id(self, id):
        raise NotImplementedError()

    def reviews_by_product_id_and_star_rating(self, id, rating):
        raise NotImplementedError()

    def reviews_by_customer_id(self, id):
        raise NotImplementedError()

    def n_most_reviewed_items(self, start_date, end_date, n):
        raise NotImplementedError()

    def n_most_productive_customers(self, start_date, end_date, n):
        raise NotImplementedError()

    def n_best_products_by_fraction(self, fraction, n):
        raise NotImplementedError()

    def n_productive_users(self, start_date, end_date, n, haters=True):
        raise NotImplementedError()