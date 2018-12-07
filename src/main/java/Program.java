import org.json.JSONObject;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Objects;

public class Program {
    public static void createNewDatabase(String dbLocation) {
        try (Connection conn = DriverManager.getConnection(dbLocation)) {
            if (conn != null) {
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("The driver name is " + meta.getDriverName());
                System.out.println("A new database has been created.");
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void createTable(String dbLocation) {

        String createSubsTable = "CREATE TABLE IF NOT EXISTS subs(" +
                "subreddit_id VARCHAR(255) PRIMARY KEY," +
                "subreddit VARCHAR(255) NOT NULL" +
                ");";

        String createUsersTable = "CREATE TABLE IF NOT EXISTS users(" +
                "id VARCHAR(255) PRIMARY KEY," +
                "author VARCHAR(255) NOT NULL" +
                ");";

        // SQL statement for creating a new table
        String createPostsTable = "CREATE TABLE IF NOT EXISTS posts(" +
                "parent_id VARCHAR(255) PRIMARY KEY, " +
                "score INTEGER NOT NULL, " +
                "created_utc INTEGER NOT NULL," +
                "link_id VARCHAR(255), " +
                "body VARCHAR(8000), " +
                "name VARCHAR(255), " +
                "author VARCHAR(255), " +
                "subreddit VARCHAR(255), " +
                "FOREIGN KEY (author) REFERENCES users(author), " +
                "FOREIGN KEY (subreddit) REFERENCES subs(subreddit)" +
                ");";

        try (Connection conn = DriverManager.getConnection(dbLocation);
             Statement stmt = conn.createStatement()) {

            // todo: creates a new table
//            stmt.execute(createSubsTable);
//            stmt.execute(createUsersTable);
            stmt.execute(createPostsTable);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public static void parseJsonToDB(String dbLocation, InputStream inputStream) throws SQLException {

        final Connection conn = DriverManager.getConnection(dbLocation);

        Objects.requireNonNull(inputStream, "InputStream cannot be null");
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        long currentTime = System.currentTimeMillis();

        bufferedReader
                .lines()
                .filter(str -> !str.isEmpty())
                .map(JSONObject::new)
                .forEach(jsonObject -> {
                    try {
                        String subsSql = "INSERT OR IGNORE INTO subs(subreddit_id, subreddit) VALUES(?,?)";
                        String usersSql = "INSERT OR IGNORE INTO users(id, author) VALUES(?,?)";
                        String postsSql = "INSERT OR IGNORE INTO posts(parent_id, score, created_utc, link_id, body, name, author, subreddit) VALUES(?,?,?,?,?,?,?,?)";

                        PreparedStatement ppstmtSubs = conn.prepareStatement(subsSql);
                        PreparedStatement ppstmtUsers = conn.prepareStatement(usersSql);
                        PreparedStatement ppstmtPosts = conn.prepareStatement(postsSql);

                        ppstmtSubs.setString(1, jsonObject.getString("subreddit_id"));
                        ppstmtSubs.setString(2, jsonObject.getString("subreddit"));

                        ppstmtUsers.setString(1, jsonObject.getString("id"));
                        ppstmtUsers.setString(2, jsonObject.getString("author"));

                        ppstmtPosts.setString(1, jsonObject.getString("parent_id"));
                        ppstmtPosts.setInt(2, jsonObject.getInt("score"));
                        ppstmtPosts.setInt(3, jsonObject.getInt("created_utc"));
                        ppstmtPosts.setString(4, jsonObject.getString("link_id"));
                        ppstmtPosts.setString(5, jsonObject.getString("body"));
                        ppstmtPosts.setString(6, jsonObject.getString("name"));
                        ppstmtPosts.setString(7, jsonObject.getString("author"));
                        ppstmtPosts.setString(8, jsonObject.getString("subreddit"));

                        ppstmtSubs.execute();
                        ppstmtUsers.execute();
                        ppstmtPosts.execute();
                    } catch (SQLException e) {
                        System.out.println(e.getMessage());
                    }
                });

        System.out.println("Total time in seconds: " + (System.currentTimeMillis() - currentTime) / 1000);
    }

    public static void main(String[] args) {
        String tableName = "redditcomments.db";

        String dbLocation = "jdbc:sqlite:/Users/JorianWielink/" + tableName;
//        String dbLocation = "jdbc:sqlite:C:\Users\Void\ + tableName;

        // todo: UNCOMMENT THIS to create a new database:
//        createNewDatabase(dbLocation);
        createTable(dbLocation);

        try {
            parseJsonToDB(dbLocation, new FileInputStream(new File("/Users/JorianWielink/Downloads/RC_2007-10")));
        } catch (SQLException | FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
