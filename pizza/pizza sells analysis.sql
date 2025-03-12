/* 	First, create the database. Then, import the pizza-related CSV file. 
	Large two files (order_time and order date) are not directly imported into MYSQL. 
    For this reason, first create a table in tables, then import the CSV files' data.
    
    I attempted to solve the questions in my own way. */

-- ----------------------------- BORUN KUNDU -------------------------------------
create table order_time
(
order_id int primary key,
order_date date not null,
order_time time not null
);
create table order_details
(
order_details_id int primary key,
order_id int not null,
pizza_name varchar(50),
quantity int not null
);


use pizza;

select * from pizzas;
select * from pizza_types;
select * from order_time;
select * from order_details;

-- 1. Retrieve the total number of orders placed.

select count(order_id)
from order_time;

-- 2. Calculate the total revenue generated from pizza sales.

select round(sum(pizzas.price * order_details.quantity),2) as revinue
from pizzas join order_details
on pizzas.pizza_name = order_details.pizza_name;

-- 3 Identify the highest-priced pizza.

select * from pizzas
order by price desc
limit 1
;

-- 4 Identify the most common pizza size ordered.

with temp1 as
(
select pizza_name, sum(quantity) as p_qu
from order_details
group by pizza_name
order by 2 desc
)
select pizzas.size as ps, sum(p_qu) as pq
from temp1
join pizzas
on temp1.pizza_name = pizzas.pizza_name
group by ps
order by pq desc
limit 1
;


-- 5. List the top 5 most ordered pizza types along with their quantities.
select pizza_name, sum(quantity) as p_qu
from order_details
group by pizza_name
order by 2 desc;




with temp1 as
(
select pizza_name, sum(quantity) as p_qu
from order_details
group by pizza_name
order by 2 desc
), temp2 as
( 
select pizzas.pizza_type, sum(p_qu) as p
from temp1 
join pizzas
on pizzas.pizza_name = temp1.pizza_name
group by pizzas.pizza_type
order by p desc
)
select pizza_types.name
from pizza_types
join temp2
on temp2.pizza_type = pizza_types.pizza_type
limit 5
;



-- 6. Join the necessary tables to find the total quantity of each pizza category ordered.
with temp1 as 
(
select pizza_name, sum(quantity) as p_qu
from order_details
group by pizza_name
order by 1 asc
)
select pizzas.pizza_type, pizzas.size, temp1.p_qu
from pizzas
join temp1
on temp1.pizza_name = pizzas.pizza_name
order by 1 asc
;


-- 7 Determine the distribution of orders by hour of the day.

-- check duplicates
with o_rank as
(
select *,
dense_rank() over(partition by order_id, order_date, order_time order by order_id) as rank1
from order_time
)
select * from o_rank 
where rank1 > 1
;

select 
hour(order_time) as or_hour,
count(distinct order_id)
from order_time
group by or_hour
order by 2 desc;

-- count quatity of order in hour
select 
hour(ot.order_time) as or_hour,
count(distinct ot.order_id) as plased_order,
count(od.quantity) as quantity_of_pizza
from order_time as ot
join order_details as od
on ot.order_id = od.order_id
group by or_hour
order by 3 asc;


-- 8 Group the orders by date and calculate the average number of pizzas ordered per day.

with temp1 as
(
select order_time.order_date as dates, sum(order_details.quantity) as per_day_quantity
from order_time
join order_details
on order_details.order_id = order_time.order_id
group by order_time.order_date
)
select avg(per_day_quantity) 
from temp1
;


-- 9. Determine the top 3 most ordered pizza types based on revenue.


select order_details.pizza_name as pizza_name,
round(sum(pizzas.price * order_details.quantity),2) as revinue
from pizzas join order_details
on pizzas.pizza_name = order_details.pizza_name
group by order_details.pizza_name
order by 2 desc
limit 3;


-- 10. Calculate the percentage contribution of each pizza type to total revenue.


select round(sum(p.price * od1.quantity),2) as revinue,
round(sum(p.price * od1.quantity) / (select sum(p1.price * od2.quantity)
										from pizzas as p1 join order_details as od2
										on p1.pizza_name = od2.pizza_name)*100,2) as percentage

from pizzas as p join order_details as od1
on p.pizza_name = od1.pizza_name
group by p.pizza_type;

-- 11 Analyze the cumulative revenue generated over time (hour).

select hour(order_time.order_time) as hours,
round(sum(pizzas.price * order_details.quantity),2) as revinue
from pizzas join order_details
on pizzas.pizza_name = order_details.pizza_name

join order_time
on order_details.order_details_id = order_time.order_id

group by hours;


-- 12. Determine the top 3 most ordered pizza types based on revenue for each pizza category.

select category 
from pizza_types
group by category;

with temp1 as -- find mamually top seal by each catagory
(
select pizza_name, sum(quantity) as p_qu
from order_details
group by pizza_name
order by 2 desc
), temp2 as
( 
select pizzas.pizza_type, sum(p_qu) as p
from temp1 
join pizzas
on pizzas.pizza_name = temp1.pizza_name
group by pizzas.pizza_type
order by p desc
)
select pizza_types.category, pizza_types.name,p
from pizza_types
join temp2
on temp2.pizza_type = pizza_types.pizza_type
where category = 'Chicken'
limit 3
;


-- Determine the top 3 most ordered pizza types based on revenue for each pizza category.


with temp1 as
(
select category, pizza_types.name as name,
round(sum(pizzas.price * order_details.quantity),2) as revinue
from pizzas 
join order_details
on pizzas.pizza_name = order_details.pizza_name

join pizza_types
on pizzas.pizza_type = pizza_types.pizza_type
group by category, pizza_types.name
), 
temp2 as 
(
select category, name, revinue,
dense_rank() over(partition by category order by name) as rank1
from temp1
)
select category, name, revinue
from temp2
where rank1 <= 3
;