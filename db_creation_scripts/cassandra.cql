create keyspace books with replication = {'class':'SimpleStrategy','replication_factor':1};
use books;

create table by_product
(
    marketplace       text,
    customer_id       int,
    review_id         text,
    product_id        text,
    product_parent    text,
    product_title     text,
    product_category  text,
    star_rating       int,
    helpful_votes     int,
    total_votes       int,
    vine              boolean,
    verified_purchase boolean,
    review_headline   text,
    review_body       text,
    review_date       date,
    primary key ((product_id), star_rating, customer_id)
);

create table by_customer
(
    marketplace       text,
    customer_id       int,
    review_id         text,
    product_id        text,
    product_parent    text,
    product_title     text,
    product_category  text,
    star_rating       int,
    helpful_votes     int,
    total_votes       int,
    vine              boolean,
    verified_purchase boolean,
    review_headline   text,
    review_body       text,
    review_date       date,
    primary key ((customer_id), review_id)
);

create table by_review
(
    marketplace       text,
    customer_id       int,
    review_id         text,
    product_id        text,
    product_parent    text,
    product_title     text,
    product_category  text,
    star_rating       int,
    helpful_votes     int,
    total_votes       int,
    vine              boolean,
    verified_purchase boolean,
    review_headline   text,
    review_body       text,
    review_date       date,
    primary key ((review_date), star_rating, review_id, verified_purchase)
);

create table by_relation
(
    verified_revies int,
    fraction_of_five float,
    marketplace       text,
    customer_id       int,
    review_id         text,
    product_id        text,
    product_parent    text,
    product_title     text,
    product_category  text,
    star_rating       int,
    helpful_votes     int,
    total_votes       int,
    vine              boolean,
    verified_purchase boolean,
    review_headline   text,
    review_body       text,
    review_date       date,
    primary key ((verified_revies), product_id, fraction_of_five)
);