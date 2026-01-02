SELECT 
    r.rental_id,
    r.rental_date::DATE AS date_rented,
    f.title AS movie_title,
    c.name AS category,
    f.rating,
    CONCAT(cus.first_name, ' ', cus.last_name) AS customer_name,
    city.city,
    cty.country,
    p.amount AS revenue,
    r.return_date::DATE AS date_returned,
    -- Calculate if the rental was returned late
    CASE 
        WHEN (r.return_date - r.rental_date) > (f.rental_duration * INTERVAL '1 day') THEN 'Late'
        ELSE 'On-Time'
    END AS return_status
FROM rental r
JOIN inventory i ON r.inventory_id = i.inventory_id
JOIN film f ON i.film_id = f.film_id
JOIN film_category fc ON f.film_id = fc.film_id
JOIN category c ON fc.category_id = c.category_id
JOIN customer cus ON r.customer_id = cus.customer_id
JOIN address a ON cus.address_id = a.address_id
JOIN city city ON a.city_id = city.city_id
JOIN country cty ON city.country_id = cty.country_id
JOIN payment p ON r.rental_id = p.rental_id
WHERE r.rental_date >= CAST(%(today)s AS DATE) - INTERVAL '14 days'
  AND r.rental_date < CAST(%(today)s AS DATE)
ORDER BY r.rental_date DESC;