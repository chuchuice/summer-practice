package miit.chuice.practice;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;

public class MusicJDBC {

    private static final String PROTOCOL = "jdbc:postgresql://";
    private static final String DRIVER = "org.postgresql.Driver";
    private static final String URL_LOCALE_NAME = "localhost/";
    private static final String DATABASE_NAME = "music_app";
    public static final String DATABASE_URL = PROTOCOL + URL_LOCALE_NAME + DATABASE_NAME;
    public static final String USER_NAME = "postgres";
    public static final String DATABASE_PASS = "postgres";

    public static void main(String[] args) {
        checkDriver();
        checkDB();

        try(Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS)) {

            //showing all tables
            getGenres(connection); System.out.println();
            getAlbums(connection); System.out.println();
            getAllAlbumsSongs(connection); System.out.println();
            getAuthors(connection); System.out.println();
            getSingles(connection); System.out.println();

            // showing with param (search a track and search album's tracks)
            getAlbumsSong(connection, "Nevermind", true); System.out.println();
            getAlbumsSong(connection, "OK Computer", false); System.out.println();
            getSongNamed(connection, "Climbing Up the Walls"); System.out.println();

            addAuthor(connection, "Sonny", "John", "Skrillex", Date.valueOf("1988-01-15"));
            addSingle(connection, 15, 14, "505", Date.valueOf("2020-04-17"), Time.valueOf("00:02:00"));
            updateSingle(connection, "new title", "505", "Arctic Monkeys");
            removeSingle(connection, "new title", "Arctic Monkeys");

        } catch (SQLException e) {
            if (e.getSQLState().startsWith("23")){
                System.err.println("Произошло дублирование данных");
            } else throw new RuntimeException(e);
        }

    }

    public static void checkDriver() {
        try {
            Class.forName(DRIVER);
        } catch (ClassNotFoundException e) {
            System.err.println("Нет JDBC-драйвера!");
            throw new RuntimeException(e);
        }
    }

    public static void checkDB() {
        try{
            Connection connection = DriverManager.getConnection(DATABASE_URL, USER_NAME, DATABASE_PASS);
        } catch (SQLException e) {
            System.err.println("Нет подключения к базе данных");
            throw new RuntimeException(e);
        }
    }


    private static void getSingles(Connection connection) throws SQLException {
        String[] columnNames = {"id", "author_id", "genre_id", "title", "date_of_release", "length_of_the_song"};

        int id;
        int authorId;
        int genreId;
        String title;
        String dateOfRelease;
        String lengthOfTheSong;

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM music_app.public.singles");

        System.out.printf("%-3s | %-10s | %-10s | %-20s | %-15s | %-50s%n", "id", "author_id", "genre_id", "title", "date_of_release", "length_of_the_song");
        System.out.println("-".repeat(100));

        while (resultSet.next()) {
            id = resultSet.getInt(columnNames[0]);
            authorId = resultSet.getInt(columnNames[1]);
            genreId = resultSet.getInt(columnNames[2]);
            title = resultSet.getString(columnNames[3]);
            dateOfRelease = resultSet.getString(columnNames[4]);
            lengthOfTheSong = resultSet.getString(columnNames[5]);
            System.out.printf("%-3s | %-10s | %-10s | %-20s | %-15s | %-50s%n", id, authorId, genreId, title, dateOfRelease, lengthOfTheSong);
        }
    }

    private static void getAllAlbumsSongs(Connection connection) throws SQLException {
        String[] columnNames = {"id", "genre_id", "author_id", "title", "length_of_the_song", "album_id"};
        int id;
        int genreId;
        int authorId;
        int albumId;
        String title;
        Time lengthOfTheSong;

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM music_app.public.album_songs");

        System.out.printf("%-3s | %-10s | %-10s | %-8s | %-30s | %-50s%n", "id", "genre_id", "author_id", "album_id", "title", "length_of_the_song");
        System.out.println("-".repeat(100));

        while (resultSet.next()) {
            id = resultSet.getInt(columnNames[0]);
            genreId = resultSet.getInt(columnNames[1]);
            authorId = resultSet.getInt(columnNames[2]);
            title = resultSet.getString(columnNames[3]);
            lengthOfTheSong = resultSet.getTime(columnNames[4]);;
            albumId = resultSet.getInt(columnNames[5]);
            System.out.printf("%-3s | %-10s | %-10s | %-8s | %-30s | %-50s%n", id, genreId, authorId, albumId, title, lengthOfTheSong);
        }
    }

    private static void getAlbums(Connection connection) throws SQLException {
        String[] columnNames = {"id", "author_id", "title", "date_of_release", "number_of_tracks"};
        int id;
        int authorId;
        String title;
        Date dateOfRelease;
        int numberOfTracks;

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM music_app.public.album");

        System.out.printf("%-3s | %-10s | %-25s | %-15s | %-30s%n", "id", "author_id", "title", "date_of_release", "number_of_tracks");
        System.out.println("-".repeat(85));

        while (resultSet.next()) {
            id = resultSet.getInt(columnNames[0]);
            authorId = resultSet.getInt(columnNames[1]);
            title = resultSet.getString(columnNames[2]);
            dateOfRelease = resultSet.getDate(columnNames[3]);;
            numberOfTracks = resultSet.getInt(columnNames[4]);
            System.out.printf("%-3s | %-10s | %-25s | %-15s | %-30s%n", id, authorId, title, dateOfRelease, numberOfTracks);
        }
    }

    private static void getAuthors(Connection connection) throws SQLException {
        String[] columnNames = {"id", "name", "surname", "nickname", "date_of_birth"};
        int id;
        String name;
        String surname;
        String nickname;
        Date dateOfBirth;

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM music_app.public.author ORDER BY id");

        System.out.printf("%-3s | %-10s | %-10s | %-20s | %-30s%n", "id", "name", "surname", "nickname", "date_of_birth");
        System.out.println("-".repeat(70));

        while (resultSet.next()) {
            id = resultSet.getInt(columnNames[0]);
            name = resultSet.getString(columnNames[1]);
            surname = resultSet.getString(columnNames[2]);
            nickname = resultSet.getString(columnNames[3]);
            dateOfBirth = resultSet.getDate(columnNames[4]);

            System.out.printf("%-3s | %-10s | %-10s | %-20s | %-30s%n", id, name, surname, nickname, dateOfBirth);
        }
    }

    private static void getGenres(Connection connection) throws SQLException {
        String[] columnNames = {"id", "genre"};
        int id;
        String genre;

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM music_app.public.genres");

        while (resultSet.next()) {
            id = resultSet.getInt(columnNames[0]);
            genre = resultSet.getString(columnNames[1]);

            System.out.printf("%d\t |\t %s\n", id, genre);
        }
    }


    private static void getAlbumsSong(Connection connection, String title, boolean fromSQL) throws SQLException {
        if (title == null || title.isBlank()) return;

        if (fromSQL) getAlbumsSong(connection, title);
        else {
            long time = System.currentTimeMillis();
            Statement statement = connection.createStatement();

            ResultSet resultSet = statement.executeQuery(
                    "SELECT album_songs.id, album_songs.title, author.nickname," +
                            " album.title, genres.genre, album.date_of_release, album_songs.length_of_the_song\n" +
                            " FROM album_songs" +
                            " JOIN author ON album_songs.author_id = author.id" +
                            " JOIN genres ON album_songs.genre_id = genres.id" +
                            " JOIN album ON album_songs.album_id = album.id;"
            );

            System.out.printf("%-3s | %-30s | %-10s | %-20s | %-10s | %-15s | %-50s%n", "id", "title", "nickname", "album_title", "genre", "date_of_release", "length");
            System.out.println("-".repeat(125));

            while (resultSet.next()) {  // пока есть данные перебираем их
                if (resultSet.getString(4).equalsIgnoreCase(title)) { // и выводим только определенный параметр
                    System.out.printf("%-3s | %-30s | %-10s | %-20s | %-10s | %-15s | %-50s%n", resultSet.getInt(1),
                            resultSet.getString(2), resultSet.getString(3), resultSet.getString(4),
                            resultSet.getString(5), resultSet.getDate(6), resultSet.getTime(7)
                    );
                }
            }
            System.out.println("SELECT ALL and FIND (" + (System.currentTimeMillis() - time) + " мс.)");
        }
    }

    private static void getAlbumsSong(Connection connection, String title) throws SQLException {
        if (title == null || title.isBlank()) return;
        title = '%' + title + '%';

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement(
                "SELECT album_songs.id, album_songs.title, author.nickname," +
                        " album.title, genres.genre, album.date_of_release, album_songs.length_of_the_song\n" +
                        " FROM album_songs" +
                        " JOIN author ON album_songs.author_id = author.id" +
                        " JOIN genres ON album_songs.genre_id = genres.id" +
                        " JOIN album ON album_songs.album_id = album.id" +
                        " WHERE album.title LIKE ?"
        );

        statement.setString(1, title);
        ResultSet resultSet = statement.executeQuery();

        System.out.printf("%-3s | %-30s | %-10s | %-20s | %-10s | %-15s | %-50s%n", "id", "title", "nickname", "album_title", "genre", "date_of_release", "length");
        System.out.println("-".repeat(125));

        while (resultSet.next()) {
            System.out.printf("%-3s | %-30s | %-10s | %-20s | %-10s | %-15s | %-50s%n", resultSet.getInt(1),
                    resultSet.getString(2), resultSet.getString(3), resultSet.getString(4),
                    resultSet.getString(5), resultSet.getDate(6), resultSet.getTime(7)
            );
        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }

    private static void getSongNamed(Connection connection, String title) throws SQLException {
        if (title == null || title.isBlank()) return;
        title = '%' + title + '%';

        long time = System.currentTimeMillis();
        PreparedStatement statement = connection.prepareStatement (
                "SELECT album_songs.id, title, author.nickname, genres.genre, album_songs.length_of_the_song" +
                        " FROM album_songs" +
                        " JOIN author ON album_songs.author_id = author.id" +
                        " JOIN genres ON album_songs.genre_id = genres.id" +
                        " UNION ALL" +
                        " SELECT singles.id, title, author.nickname, genres.genre, length_of_the_song" +
                        " FROM singles" +
                        " JOIN author ON singles.author_id = author.id" +
                        " JOIN genres ON singles.genre_id = genres.id" +
                        " WHERE title LIKE ?;"
        );

        statement.setString(1, title);
        ResultSet resultSet = statement.executeQuery();

        System.out.printf("%-3s | %-30s | %-10s | %-10s | %-10s%n", "id", "title", "nickname", "genre", "length");
        System.out.println("-".repeat(73));

        while (resultSet.next()) {
            System.out.printf("%-3s | %-30s | %-10s | %-10s | %-10s%n", resultSet.getInt(1),
                    resultSet.getString(2), resultSet.getString(3),
                    resultSet.getString(4), resultSet.getTime(5));

        }
        System.out.println("SELECT with WHERE (" + (System.currentTimeMillis() - time) + " мс.)");
    }


    private static void removeSingle(Connection connection, String title, String nickname) throws SQLException {
        if (title == null || title.isBlank() || nickname == null || nickname.isBlank()) return;

        PreparedStatement statement = connection.prepareStatement(
                "DELETE FROM singles WHERE title LIKE ? " +
                        "AND author_id IN (SELECT id FROM author WHERE nickname = ?)"
        );

        statement.setString(1, title);
        statement.setString(2, nickname);

        int count = statement.executeUpdate();
        System.out.println("DELETEd " + count + " single");
        getSingles(connection);
    }

    private static void updateSingle(Connection connection, String newTitle, String oldTitle, String nickname) throws SQLException {
        if (newTitle == null || newTitle.isBlank() || oldTitle == null || oldTitle.isBlank()) return;

        PreparedStatement statement = connection.prepareStatement(
                "UPDATE singles SET title = ? WHERE title LIKE ? " +
                        "AND author_id IN (SELECT id FROM author WHERE nickname LIKE ?)"
        );

        statement.setString(1, newTitle);
        statement.setString(2, oldTitle);
        statement.setString(3, nickname);

        int count = statement.executeUpdate();

        System.out.println("UPDATEd " + count + " singles");
        getSingles(connection);
    }

    private static void addAuthor(Connection connection, String name, String surname, String nickname, Date dateOfBirth) throws SQLException {
        if (name == null || name.isBlank() || surname == null || surname.isBlank() ||
                nickname == null || nickname.isBlank()) return;

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO author (name, surname, nickname, date_of_birth) " +
                        "VALUES (?, ?, ?, ?)"
        );

        statement.setString(1, name);
        statement.setString(2, surname);
        statement.setString(3, nickname);
        statement.setDate(4, dateOfBirth);

        int count = statement.executeUpdate();

        ResultSet resultSet = statement.getGeneratedKeys();
        if (resultSet.next()) {
            System.out.println("Идентификатор автора " + resultSet.getInt(1));
        }

        System.out.println("INSERTed " + count + " author");
        getAuthors(connection);

    }

    private static void addSingle(Connection connection, int authorId, int genreid, String title,
                                  Date dateOfRelease, Time lengthOfTheSong) throws SQLException {

        if (authorId < 0 || genreid < 0 || dateOfRelease == null || title == null
                || title.isBlank() || lengthOfTheSong == null) return;

        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO singles (author_id, genre_id, title, date_of_release, length_of_the_song) " +
                        "VALUES (?, ?, ?, ?, ?)"
        );

        statement.setInt(1, authorId);
        statement.setInt(2, genreid);
        statement.setString(3, title);
        statement.setDate(4, dateOfRelease);
        statement.setTime(5, lengthOfTheSong);

        int count = statement.executeUpdate();  // выполняем запрос на коррекцию и возвращаем количество измененных строк

        ResultSet resultSet = statement.getGeneratedKeys();
        if (resultSet.next()) {
            System.out.println("Идентификатор сингла " + resultSet.getInt(1));
        }

        System.out.println("INSERTed " + count + " single");
        getSingles(connection);

    }

}
