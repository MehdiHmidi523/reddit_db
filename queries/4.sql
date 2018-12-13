SELECT COUNT(DISTINCT(posts.subreddit))
FROM posts
WHERE posts.author IN (
	SELECT DISTINCT(author)
	FROM posts
	WHERE link_id = 't3_1422u0' AND parent_id LIKE 't1%'
);
