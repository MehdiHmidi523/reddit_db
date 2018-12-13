SELECT authors.author, MIN(sum)
FROM (
	SELECT author, SUM(score) AS sum
	FROM posts
	GROUP BY author
) AS authors
