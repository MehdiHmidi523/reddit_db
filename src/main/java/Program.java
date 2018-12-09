import com.oracle.javafx.jmx.json.JSONReader;
import jdk.nashorn.internal.parser.JSONParser;
import org.json.JSONObject;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class Program {
    private static void createNewDatabase(String dbLocation) {
        try (Connection conn = DriverManager.getConnection(dbLocation)) {
            if (conn != null) {
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("The driver name is " + meta.getDriverName());
                System.out.println("A new database has been created.");
            }
        } catch (SQLException e) { System.out.println(e.getMessage()); }
    }

    public static void createTable(String dbLocation) {

        String createSubsTable = "CREATE TABLE IF NOT EXISTS subs(" +
                "subreddit_id VARCHAR(255) NOT NULL," +
                "subreddit VARCHAR(255) NOT NULL," +
                "PRIMARY KEY(subreddit_id)" +
                ");";

        String createUsersTable = "CREATE TABLE IF NOT EXISTS users(" +
                "id VARCHAR(255) NOT NULL PRIMARY KEY," +
                "author VARCHAR(255) NOT NULL" +
                ");";

        String createPostsTable = "CREATE TABLE IF NOT EXISTS posts(" +
                "parent_id VARCHAR(255) NOT NULL PRIMARY KEY, " +
                "score INTEGER NOT NULL, " +
                "created_utc INTEGER NOT NULL," +
                "link_id VARCHAR(255) NOT NULL, " +
                "body VARCHAR(8000)," +
                "name VARCHAR(255) NOT NULL, " +
                "author VARCHAR(255) NOT NULL, " +
                "subreddit VARCHAR(255) NOT NULL, " +
                "FOREIGN KEY (author) REFERENCES users(author), " +
                "FOREIGN KEY (subreddit) REFERENCES subs(subreddit)" +
                ");";

        try (Connection conn = DriverManager.getConnection(dbLocation);
             Statement stmt = conn.createStatement()) {

            // todo: creates a new table
            stmt.execute(createSubsTable);
            stmt.execute(createUsersTable);
            stmt.execute(createPostsTable);
        } catch (SQLException e) { System.out.println(e.getMessage()); }
    }

    private static void parseJsonToDB(String dbLocation, InputStream inputStream) throws SQLException, IOException {
        final Connection conn = DriverManager.getConnection(dbLocation);
        Objects.requireNonNull(inputStream, "InputStream cannot be null");
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));

        long currentTime = System.currentTimeMillis();

//        the following GREATLY speeds up importing the data: 23 seconds against 120 minutes
//        conn.prepareStatement("PRAGMA synchronous = OFF").execute();
        conn.prepareStatement("PRAGMA journal_mode = WAL").execute();
        conn.setAutoCommit(false);

        String line = null;
        JSONObject jsonObject = null;
        int i = 0;

        String subsSql = "INSERT OR IGNORE INTO subs(subreddit_id, subreddit) VALUES(?,?)";
        String usersSql = "INSERT OR IGNORE INTO users(id, author) VALUES(?,?)";
        String postsSql = "INSERT OR IGNORE INTO posts(parent_id, score, created_utc, link_id, body, name, author, subreddit) VALUES(?,?,?,?,?,?,?,?)";

        PreparedStatement ppstmtSubs = conn.prepareStatement(subsSql);
        PreparedStatement ppstmtUsers = conn.prepareStatement(usersSql);
        PreparedStatement ppstmtPosts = conn.prepareStatement(postsSql);

        while ((line = bufferedReader.readLine()) != null) {
            jsonObject = new JSONObject(line);

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

            ppstmtSubs .addBatch();
            ppstmtUsers.addBatch();
            ppstmtPosts.addBatch();

            if (i++ % 10000 == 0) {
                ppstmtSubs.executeBatch();
                ppstmtUsers.executeBatch();
                ppstmtPosts.executeBatch();
                System.out.printf("batch %d executed.", (i / 10000));
            }
        }

        if (i % 10000 != 0) {
            ppstmtSubs.executeBatch();
            ppstmtUsers.executeBatch();
            ppstmtPosts.executeBatch();
        }

        System.out.println("\nTotal time in seconds: " + (System.currentTimeMillis() - currentTime) / 1000);
    }

    public static void main(String[] args) {
        String tableName = "redditcomments-not-nullablev10.db";

        String dbLocation = "jdbc:sqlite:/home/n41r0j/" + tableName;
        // String dbLocation = "jdbc:sqlite:/Users/JorianWielink/" + tableName;
        // String dbLocation = "jdbc:sqlite:C:\Users\Void\ + tableName;

        // TODO: UNCOMMENT THIS to create a new database:
        createNewDatabase(dbLocation);
        createTable(dbLocation);
        try {
            parseJsonToDB(dbLocation, new FileInputStream(new File("/home/n41r0j/Downloads/RC_2012-12")));
//            parseJsonToDB(dbLocation, new FileInputStream(new File("/home/n41r0j/RC_2007-10")));
        } catch (SQLException | IOException e) { e.printStackTrace(); }
    }
}