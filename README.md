# Test_Technique_Foodles
-- Tranformation et partitionnement des tables "staging" (bronze)
CREATE OR REPLACE VIEW converted_date_sales AS
    SELECT
        fridge_id,
        product_id,
        TO_CHAR(TO_TIMESTAMP(sale_date), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS iso_date,
        price
FROM sales
;

-- Tables intermédiaires (silver)


CREATE OR REPLACE VIEW category_total_amount_sales_by_canteen AS 
    SELECT
        fc.canteen_id,
        p.category_id,
        SUM(s.price) AS total_amount_sales,
    FROM sales s
    LEFT JOIN products p ON p.product_id = s.product_id
    LEFT JOIN fridge_canteens fc ON fc.fridge_id = s.fridge_id
    GROUP BY fc.canteen_id, p.category_id
;
    
CREATE OR REPLACE VIEW sales_amount_by_fridge AS 
    SELECT
        fc.canteen_id,
        p.category_id,
        cs.iso_date,
        cs.fridge_id,
        SUM(cs.price) AS total_sales_amount,
    FROM converted_date_sales cs
    LEFT JOIN products p ON p.product_id = s.product_id
    LEFT JOIN fridge_canteens fc ON fc.fridge_id = cs.fridge_id
    GROUP BY fc.canteen_id, p.category_id, cs.iso_date, cs.fridge_id
;



CREATE OR REPLACE VIEW ranked_sales AS 
    SELECT 
        *,
        RANK() OVER(PARTITION BY canteen_id, category_id, iso_date, ORDER BY total_sales DESC) AS sales_rank
    FROM sales_amount_by_fridge
    ;


-- Tables mart (gold)
CREATE OR REPLACE final_table AS
    SELECT
        canteen_id,
        category_id,
        iso_date,
        fridge_id,
        total_sales_amount
    FROM ranked_sales
    WHERE sales_rank = 1
    ;


