create database books;
create user '<user>'@'%' identified by 'password';
grant all privileges on books.* To '<user>'@'%';
flush privileges;
use books;

create table review
(
    id                char(13) primary key,
    marketplace       int,
    customer_id       int,
    product_id        char(10),
    star_rating       tinyint,
    helpful_votes     int,
    total_votes       int,
    vine              boolean,
    verified_purchase boolean,
    headline          text,
    body              longtext,
    review_date       date,
    foreign key (marketplace) references marketplace (id),
    foreign key (product_id) references product (id)
);

create table product
(
    id          char(10) primary key,
    parent      text,
    title       text,
    category_id int,
    foreign key (category_id) references category (id)
);

create table category
(
    id   int primary key auto_increment,
    name text
);

create table marketplace
(
    id   int primary key auto_increment,
    name char(2)
)