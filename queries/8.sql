SELECT COUNT(author)
FROM (
        SELECT author
        FROM posts
        WHERE parent_id LIKE 't3%'
        GROUP BY author
        HAVING COUNT(subreddit) = 1
);
