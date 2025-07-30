
-- 1. Find top 3 outlets by cuisine type without using limit and top function
with cte as(select restaurant_id,cuisine,count(order_id) as total_orders,ROW_NUMBER() over (partition by cuisine order by count(order_id) desc) as rn
from noon_orders group by restaurant_id,cuisine)
select restaurant_id,cuisine,total_orders from cte where rn<=3

-- 2. Find the daily new customer count from the launch date (everyday how many new customers we are acquiring)
with cte as(select *,row_number() over (partition by customer_code order by placed_at asc) as rn from noon_orders)
select cast(placed_at as date) as placed_at,sum(case when rn=1 then 1 else 0 end) as new_customer
from cte 
group by cast(placed_at as date)

-- 3. Count of all the users who were acquired in Jan 2025 and only placed one order in Jan and did not place any other order 
with cte as(select customer_code,count(*) as cnt from noon_orders 
where CAST(placed_at as date) between '2025-01-01' and '2025-01-31'
group by customer_code
having count(*)=1)
select count(*) as users_in_jan 
from cte where customer_code not in 
(select customer_code from noon_orders where cast(placed_at as date)>='2025-02-01')

-- 4. List all the customers with no order in the last 7 days but were acquired 1 month ago  with their first order on promo
with cte as(select Customer_code,min(placed_at) as first_order_date,max(placed_at) as latest_order_date
from noon_orders
group by Customer_code)
select c.*,n.Promo_code_Name
from cte c inner join noon_orders n on c.Customer_code=n.Customer_code and c.first_order_date=n.Placed_at
where latest_order_date < DATEADD(day,-7,getdate()) and first_order_date < DATEADD(MONTH,-1,getdate()) and
n.Promo_code_Name is not null

-- 5. Growth team is planning to create a trigger that will target customers after their every third order with a personalized 
-- communication and they have asked you to create query for this
with cte as (select *,ROW_NUMBER() over (partition by customer_code order by placed_at) as rn from noon_orders)
select * from cte where rn%3=0 and cast(placed_at as date) = cast(getdate() as date)

-- 6. List customers who placed more than 1 order and all their orders on a promo only.
with cte as(select Customer_code,count(*) as total_orders from noon_orders
group by Customer_code having count(*)>1)
select distinct c.Customer_code from cte c inner join noon_orders n on c.Customer_code=n.Customer_code
where c.Customer_code not in (select customer_code from noon_orders where Promo_code_Name is null)

-- 7. What percent of customers were organically acquired in Jan 2025 (Placed their first order without promo code)
with cte as(select *,ROW_NUMBER() over (partition by customer_code order by placed_at) as rn from noon_orders
where MONTH(placed_at)=1)
select count( case when rn=1 and Promo_code_Name is null then customer_code end)*100.0/count(distinct customer_code) as organic_user_percent
from cte 







