package io.sustc.service.impl;

import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Arrays;
import java.util.List;

/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {

    /**
     * Getting a {@link DataSource} instance from the framework, whose connections are managed by HikariCP.
     * <p>
     * Marking a field with {@link Autowired} annotation enables our framework to automatically
     * provide you a well-configured instance of {@link DataSource}.
     * Learn more: <a href="https://www.baeldung.com/spring-dependency-injection">Dependency Injection</a>
     */
    @Autowired
    private DataSource dataSource;

    @Override
    public List<Integer> getGroupMembers() {
        //TODO: replace this with your own student IDs in your group
        return Arrays.asList(12210260, 12210255);
    }

    @Override
    public void importData(//TODO:还得把导入别的表的写了
            List<DanmuRecord> danmuRecords,
            List<UserRecord> userRecords,
            List<VideoRecord> videoRecords
    ) throws IOException, ClassNotFoundException {
        String Danmu =
                "D:\\course\\database\\proj2\\sustc-main\\sustc-main\\sustc-runner\\data\\import\\DanmuRecord.ser";
        String User = "D:\\course\\database\\proj2\\sustc-main\\sustc-main\\sustc-runner\\data\\import\\UserRecord.ser";
        String Video = "D:\\course\\database\\proj2\\sustc-main\\sustc-main\\sustc-runner\\data\\import\\VideoRecord.ser";
        ObjectInputStream inD = new ObjectInputStream(new FileInputStream(Danmu));
        ObjectInputStream inU = new ObjectInputStream(new FileInputStream(User));
        ObjectInputStream inV = new ObjectInputStream(new FileInputStream(Video));
        danmuRecords = (List<DanmuRecord>) inD.readObject();
        userRecords = (List<UserRecord>) inU.readObject();
        videoRecords = (List<VideoRecord>) inV.readObject();
        inD.close();
        inU.close();
        inV.close();
        String sqlD = "insert into danmurecord (id,bv,mid,content,posttime,time) values (?,?,?,?,?,?)";
        String sqlU = "insert into userrecord (mid,name,sex,birthday,level,identity,sign,password,qq,wechat,coins) " +
                "values (?,?,?,?,?,?,?,?,?,?,?)";
        String sqlV = "insert into videorecord (bv,title,ownermid,ownername,committime,reviewtime," +
                "publictime,duration,description,reviewer) " +
                "values (?,?,?,?,?,?,?,?,?,?)";
        String sqlPost = "insert into post (bv,mid) values (?,?)";
        String sqlLikeBy = "insert into likeby (id,mid) values (?,?)";
        String sqlView = "insert into viewrecord (mid,timestamp,bv,duration) values (?,?,?,?)";
        String sqlFollow = "insert into follow (mid,follower) values (?,?)";
        try(Connection connection = dataSource.getConnection();
            PreparedStatement D = connection.prepareStatement(sqlD);
            PreparedStatement U = connection.prepareStatement(sqlU);
            PreparedStatement V = connection.prepareStatement(sqlV);
            PreparedStatement LikeBy = connection.prepareStatement(sqlLikeBy);
            PreparedStatement Post = connection.prepareStatement(sqlPost);
            PreparedStatement view = connection.prepareStatement(sqlView);
            PreparedStatement follow = connection.prepareStatement(sqlFollow);
        ) {
            for (int i = 1; i <= danmuRecords.size(); i++) {
                String bv = danmuRecords.get(i).getBv();
                long mid = danmuRecords.get(i).getMid();
                String content = danmuRecords.get(i).getContent();
                Timestamp post = danmuRecords.get(i).getPostTime();
                D.setInt(1,i);
                D.setString(2,bv);
                D.setLong(3,mid);
                D.setString(4,content);
                D.setTimestamp(5,post);
                D.setFloat(6,danmuRecords.get(i).getTime());
                D.addBatch();
            }
            int[] DanmuResult = D.executeBatch();
            //import user records
            for (int i = 0; i < userRecords.size(); i++) {
                U.setLong(1,userRecords.get(i).getMid());
                U.setString(2,userRecords.get(i).getName());
                U.setString(3,userRecords.get(i).getSex());
                U.setString(4,userRecords.get(i).getBirthday());
                U.setShort(5,userRecords.get(i).getLevel());
                U.setString(6,userRecords.get(i).getName().toLowerCase());
                U.setString(7,userRecords.get(i).getSign());
                U.setString(8,userRecords.get(i).getPassword());
                U.setString(9,userRecords.get(i).getQq());
                U.setString(10,userRecords.get(i).getWechat());
                U.setInt(11,userRecords.get(i).getCoin());
                U.addBatch();
            }
            int[] UserResult = U.executeBatch();
            //import video records
            for (int i = 0; i < videoRecords.size(); i++) {
                V.setString(1,videoRecords.get(i).getBv());
                V.setString(2,videoRecords.get(i).getTitle());
                V.setLong(3,videoRecords.get(i).getOwnerMid());
                V.setString(4,videoRecords.get(i).getOwnerName());
                V.setTimestamp(5,videoRecords.get(i).getCommitTime());
                V.setTimestamp(6,videoRecords.get(i).getReviewTime());
                V.setTimestamp(7,videoRecords.get(i).getPublicTime());
                V.setFloat(8,videoRecords.get(i).getDuration());
                V.setString(9,videoRecords.get(i).getDescription());
                V.setLong(10,videoRecords.get(i).getReviewer());
                V.addBatch();
                Post.setLong(2,videoRecords.get(i).getOwnerMid());
                Post.setString(1,videoRecords.get(i).getBv());
                Post.addBatch();
            }
            int[] VideoResult = V.executeBatch();
            int[] postResult = Post.executeBatch();//TODO:可能不要这个表？？？
            //import view records
            for (int i = 0; i < videoRecords.size(); i++) {
                long[] viwerMids = videoRecords.get(i).getViewerMids();
                float[] viewTime = videoRecords.get(i).getViewTime();
                String bv = videoRecords.get(i).getBv();
                for (int j = 0; j < viwerMids.length; j++) {
                    view.setLong(1,viwerMids[j]);
                    view.setFloat(4,viewTime[j]);
                    view.setString(3,bv);
                    //TODO:timestamp的问题，怎么转换，还是和duration重合了？？？
                    view.addBatch();
                }
                int[] viewResult = view.executeBatch();
            }
            //import danmu like by records
            for (int i = 1; i <= danmuRecords.size(); i++) {
                LikeBy.setLong(1,i);
                long[] by = danmuRecords.get(i).getLikedBy();
                for (int j = 0; j < by.length; j++) {
                    LikeBy.setLong(2,by[j]);
                    LikeBy.addBatch();
                }
                int[] likeByResult = LikeBy.executeBatch();
            }
            //import following information
            for (int i = 0; i < userRecords.size() ; i++) {
                long mid = userRecords.get(i).getMid();
                long[] follows = userRecords.get(i).getFollowing();
                for (int j = 0; j < follows.length; j++) {
                    follow.setLong(1,follows[j]);//user mid
                    follow.setLong(2,mid);//user follow the mid
                    follow.addBatch();
                }
                int[] followResult = follow.executeBatch();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        System.out.println(danmuRecords.size());
        System.out.println(userRecords.size());
        System.out.println(videoRecords.size());
    }
//反序列化之后找个地方存着，再都加入表里？？？
    //还是不用存，用prepared statement批量inser进表里
    // TODO: implement your import logic
    /*
     * The following code is just a quick example of using jdbc datasource.
     * Practically, the code interacts with database is usually written in a DAO layer.
     *
     * Reference: [Data Access Object pattern](https://www.baeldung.com/java-dao-pattern)
     */

    @Override
    public void truncate() {
        // You can use the default truncate script provided by us in most cases,
        // but if it doesn't work properly, you may need to modify it.

        String sql = "DO $$\n" +
                "DECLARE\n" +
                "    tables CURSOR FOR\n" +
                "        SELECT tablename\n" +
                "        FROM pg_tables\n" +
                "        WHERE schemaname = 'public';\n" +
                "BEGIN\n" +
                "    FOR t IN tables\n" +
                "    LOOP\n" +
                "        EXECUTE 'TRUNCATE TABLE ' || QUOTE_IDENT(t.tablename) || ' CASCADE;';\n" +
                "    END LOOP;\n" +
                "END $$;\n";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer sum(int a, int b) {
        String sql = "SELECT ?+?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, a);
            stmt.setInt(2, b);
            log.info("SQL: {}", stmt);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
