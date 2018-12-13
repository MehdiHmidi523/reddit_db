SELECT (amount / days) FROM
	(SELECT (MAX(created_utc) - MIN(created_utc)) / 86400 AS days, COUNT(*) AS amount
	FROM posts
	WHERE subreddit = 'WTF')
;
