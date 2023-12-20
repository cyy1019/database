package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.RegisterUserReq;
import io.sustc.dto.UserInfoResp;
import io.sustc.service.UserService;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class UserServiceImpl implements UserService {//TODO：给整个类写一个check auth valid的方法？？？
    private DataSource dataSource;
    @Override
    public long register(RegisterUserReq req) {
        return 0;
    }

    @Override
    public boolean deleteAccount(AuthInfo auth, long mid) {
        return false;
    }

    @Override
    //TODO:
    public boolean follow(AuthInfo auth, long followeeMid) {
        return false;
    }

    @Override
    public UserInfoResp getUserInfo(long mid) {
        String getMid = "select mid,coins from userrecord where mid = " + mid + ";";
        String getFollowing = "select mid from follow where follower =  " + mid + ";";
        String getFollowed = "select follower from follow where mid = " + mid + ";";
        String getWatch = "select bv from viewrecord where mid =" + mid + ";";
        String getLike = "select bv from uservideo where mid = "+ mid + " and likes = true;";
        String getCollect = "select bv from uservideo where mid = "+ mid +" and collected = true";
        String getPost = "select bv from post where mid = " + mid + ";";
        try (Connection connection = dataSource.getConnection();
        PreparedStatement get = connection.prepareStatement(getMid);
        PreparedStatement following = connection.prepareStatement(getFollowing);
        PreparedStatement followed = connection.prepareStatement(getFollowed);
        PreparedStatement watch = connection.prepareStatement(getWatch);
        PreparedStatement like = connection.prepareStatement(getLike);
        PreparedStatement collect = connection.prepareStatement(getCollect);
        PreparedStatement post = connection.prepareStatement(getPost);
        ) {
            ResultSet getRe = get.executeQuery();
            if(!getRe.next()){
                return null;
            }else{
                UserInfoResp user = new UserInfoResp();
                user.setMid(getRe.getLong("mid"));
                user.setCoin(getRe.getInt("coins"));
                ResultSet followingRe = following.executeQuery();
                ResultSet followedRe = followed.executeQuery();
                ResultSet watchRe = watch.executeQuery();
                ResultSet likeRe = like.executeQuery();
                ResultSet collectRe = collect.executeQuery();
                ResultSet postRe = post.executeQuery();
                List<Long> followedMid = new ArrayList<>();
                while(followedRe.next()){
                    followedMid.add(followedRe.getLong("follower"));
                }
                long[] foed = new long[followedMid.size()];
                for (int i = 0; i < followedMid.size(); i++) {
                    foed[i] = followedMid.get(i);
                }
                user.setFollower(foed);
                List<Long> followingMid = new ArrayList<>();
                while (followingRe.next()){
                    followingMid.add(followingRe.getLong("mid"));
                }
                long[] foing = new long[followingMid.size()];
                user.setFollowing(foing);
                return null;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
