package com.company;

import java.sql.*;
import java.util.Random;

public class Main {
    static Connection srcConnection;
    static Connection dstConnection;
    static private Random idGenerator = new Random();

    public static void main(String[] args) throws SQLException {


        srcConnection = DriverManager.getConnection("jdbc:mysql://mysqlsrv55.spb.labs.intellij.net/kotlin-testdb?" +
                "user=kotlin-test-user&password=paWJVNS5fYbb");
        dstConnection = DriverManager.getConnection("jdbc:mysql://mysqlsrv55.spb.labs.intellij.net/kotlin-testv2db?" +
                "user=kotlin-test-user&password=paWJVNS5fYbb");

        migrateUsers();

        checkUsers();
        checkPrograms();
        srcConnection.close();
        dstConnection.close();
    }

    private static void migrateUsers() {
        try (PreparedStatement st1 = srcConnection.prepareStatement("SELECT * FROM users ");
             ResultSet rs = st1.executeQuery()) {
            while (rs.next()) {
                addUser(rs.getString("USER_ID"), rs.getString("USER_NAME"), rs.getString("USER_TYPE"));
                int userId = getUserId(rs.getString("USER_ID"), rs.getString("USER_TYPE"));
                migrateUserPrograms(userId, rs.getString("USER_ID"), rs.getString("USER_TYPE"));
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
    }

    private static void addUser(String id, String name, String type) {
        try (PreparedStatement st2 = dstConnection.prepareStatement("INSERT INTO users (client_id, provider, username) VALUES (?, ?, ?)")) {
            st2.setString(1, id);
            st2.setString(2, type);
            st2.setString(3, name);
            st2.execute();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
    }

    private static void migrateUserPrograms(int userId, String clientId, String userType) {
        try (PreparedStatement st1 = srcConnection.prepareStatement("SELECT * FROM userprogramid WHERE USER_ID = ? AND USER_TYPE = ?")) {
            st1.setString(1, clientId);
            st1.setString(2, userType);
            ResultSet rs = st1.executeQuery();
            while (rs.next()) {

                try (PreparedStatement st2 = srcConnection.prepareStatement("SELECT * FROM programs WHERE PROGRAM_ID = ? ")) {
                    st2.setString(1, rs.getString("PROGRAM_ID"));
                    ResultSet rs2 = st2.executeQuery();
                    rs2.next();
                    int projectId = addProject(userId, rs2.getString("PROGRAM_NAME"), rs2.getString("PROGRAM_ARGS"), rs2.getString("RUN_CONF"));

                    if (projectId != -1) {
                        String publicLink = rs2.getString("PROGRAM_LINK").equals("") ? "" : rs2.getString("PROGRAM_LINK").substring("http://kotlin-demo.jetbrains.com/?publicLink=".length());
                        addFile(userId, projectId, rs2.getString("PROGRAM_TEXT"), rs2.getString("PROGRAM_NAME"), publicLink);
                    }
                }
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
    }

    private static int addProject(int userId, String name, String args, String runConf) {
        try (PreparedStatement st = dstConnection.prepareStatement("INSERT INTO projects (owner_id, name, args, run_configuration, public_id) VALUES (?,?,?,?,?)")) {
            st.setString(1, userId + "");
            st.setString(2, name);

            if (args != null) {
                st.setString(3, args);
            } else {
                st.setString(3, "");
            }

            if (runConf != null && !runConf.equals("")) {
                st.setString(4, runConf);
            } else {
                st.setString(4, "java");
            }

            String publicId = userId + idGenerator.nextInt() + "";
            st.setString(5, publicId);

            st.execute();
            return getProjectId(userId, name);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return -1;
        }
    }

    private static void addFile(int userId, int projectId, String content, String name, String publicId) {
        name = name.endsWith(".kt") ? name : name + ".kt";
        try (PreparedStatement st = dstConnection.prepareStatement("INSERT INTO files (content, name, project_id, public_id) VALUES (?,?,?,?)")) {
            st.setString(1, content);
            st.setString(2, name);
            st.setString(3, projectId + "");
            st.setString(4, publicId.equals("") ? userId + idGenerator.nextInt() + "" : publicId);
            st.execute();

            st.close();
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
    }


    private static int getUserId(String clientId, String provider) {
        try (PreparedStatement ps = dstConnection.prepareStatement("SELECT users.id FROM users WHERE users.client_id = ? AND users.provider = ?")) {
            ps.setString(1, clientId);
            ps.setString(2, provider);
            ResultSet rs = ps.executeQuery();
            rs.next();
            return rs.getInt("id");
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return -1;
        }
    }

    private static int getProjectId(int userId, String projectName) {
        try (PreparedStatement st = dstConnection.prepareStatement(
                "SELECT projects.id FROM projects WHERE ( projects.owner_id = ? AND projects.name = ?)")) {

            st.setString(1, userId + "");
            st.setString(2, projectName);
            try (ResultSet rs = st.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("id");
                } else {
                    System.err.println("Can't find project.");
                    return -1;
                }
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            return -1;
        }
    }

    private static void checkUsers() {
        try (PreparedStatement st1 = srcConnection.prepareStatement("SELECT * FROM users ");
             ResultSet rs = st1.executeQuery()) {
            while (rs.next()) {
                try (PreparedStatement st2 = dstConnection.prepareStatement("SELECT * FROM users WHERE " +
                        "users.client_id = ? AND users.provider = ? AND users.username = ?")) {
                    st2.setString(1, rs.getString("USER_ID"));
                    st2.setString(2, rs.getString("USER_TYPE"));
                    st2.setString(3, rs.getString("USER_NAME"));
                    ResultSet rs2 = st2.executeQuery();
                    if (!rs2.next()) {
                        System.err.println("User " + rs.getString("USER_NAME") + " not found after migration.");
                    }
                }
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
    }

    private static void checkPrograms() {
        try (PreparedStatement st1 = srcConnection.prepareStatement("SELECT * FROM userprogramid JOIN " +
                "programs ON  programs.PROGRAM_ID = userprogramid.PROGRAM_ID");
             ResultSet rs = st1.executeQuery()) {
            while (rs.next()) {
                PreparedStatement st2;
                if (rs.getString("PROGRAM_LINK") == null || rs.getString("PROGRAM_LINK").equals("")) {
                    st2 = dstConnection.prepareStatement("SELECT * FROM users JOIN " +
                            "projects ON projects.owner_id = users.id JOIN " +
                            "files ON files.project_id = projects.id WHERE " +
                            "users.client_id = ? AND users.provider = ? AND projects.name = ? AND files.name = ? AND files.content = ?");
                } else {
                    st2 = dstConnection.prepareStatement("SELECT * FROM users JOIN " +
                            "projects ON projects.owner_id = users.id JOIN " +
                            "files ON files.project_id = projects.id WHERE " +
                            "users.client_id = ? AND users.provider = ? AND projects.name = ? AND files.name = ? AND files.content = ? AND files.public_id = ?");
                }
                st2.setString(1, rs.getString("USER_ID"));
                st2.setString(2, rs.getString("USER_TYPE"));
                st2.setString(3, rs.getString("PROGRAM_NAME"));
                String filename = rs.getString("PROGRAM_NAME").endsWith(".kt") ? rs.getString("PROGRAM_NAME") : rs.getString("PROGRAM_NAME") + ".kt";
                st2.setString(4, filename);
                st2.setString(5, rs.getString("PROGRAM_TEXT"));
                if (rs.getString("PROGRAM_LINK") != null && !rs.getString("PROGRAM_LINK").equals("")) {
                    String publicId = rs.getString("PROGRAM_LINK").substring("http://kotlin-demo.jetbrains.com/?publicLink=".length());
                    st2.setString(6, publicId);
                }

                ResultSet rs2 = st2.executeQuery();
                if (!rs2.next()) {
                    System.err.println("Program " + rs.getString("PROGRAM_NAME") + " not found after migration.");
                }

                st2.close();
                rs2.close();
            }
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        }
    }
}
