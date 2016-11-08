package org.wordpress.android.datasets;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteStatement;
import android.text.TextUtils;

import org.wordpress.android.models.ReaderPost;
import org.wordpress.android.models.ReaderPostList;
import org.wordpress.android.models.ReaderTag;
import org.wordpress.android.models.ReaderTagList;
import org.wordpress.android.models.ReaderTagType;
import org.wordpress.android.ui.reader.ReaderConstants;
import org.wordpress.android.ui.reader.actions.ReaderActions;
import org.wordpress.android.ui.reader.models.ReaderBlogIdPostId;
import org.wordpress.android.ui.reader.models.ReaderBlogIdPostIdList;
import org.wordpress.android.util.AppLog;
import org.wordpress.android.util.CrashlyticsUtils;
import org.wordpress.android.util.SqlUtils;
import org.wordpress.android.util.StringUtils;

/**
 * tbl_posts contains all reader posts - the primary key is pseudo_id + tag_name + tag_type,
 * which allows the same post to appear in multiple streams (ex: it can exist in followed
 * sites, liked posts, and tag streams). note that posts in a specific blog or feed are
 * stored here with an empty tag_name.
 */
public class ReaderPostTable {
    private static final String COLUMN_NAMES =
            "post_id,"              // 1
          + "blog_id,"              // 2
          + "feed_id,"              // 3
          + "feed_item_id,"         // 4
          + "pseudo_id,"            // 5
          + "author_name,"          // 6
          + "author_first_name,"    // 7
          + "author_id,"            // 8
          + "title,"                // 9
          + "excerpt,"              // 10
          + "format,"               // 11
          + "url,"                  // 12
          + "short_url,"            // 13
          + "blog_url,"             // 14
          + "blog_name,"            // 15
          + "featured_image,"       // 16
          + "featured_video,"       // 17
          + "post_avatar,"          // 18
          + "score,"                // 19
          + "date_published,"       // 20
          + "date_liked,"           // 21
          + "date_tagged,"          // 22
          + "num_replies,"          // 23
          + "num_likes,"            // 24
          + "is_liked,"             // 25
          + "is_followed,"          // 26
          + "is_comments_open,"     // 27
          + "is_external,"          // 28
          + "is_private,"           // 29
          + "is_videopress,"        // 30
          + "is_jetpack,"           // 31
          + "primary_tag,"          // 32
          + "secondary_tag,"        // 33
          + "attachments_json,"     // 34
          + "discover_json,"        // 35
          + "xpost_post_id,"        // 36
          + "xpost_blog_id,"        // 37
          + "railcar_json,"         // 38
          + "tag_name,"             // 39
          + "tag_type,"             // 40
          + "has_gap_marker";       // 41

    protected static void createTables(SQLiteDatabase db) {
        db.execSQL("CREATE TABLE tbl_posts ("
                + "	post_id		        INTEGER DEFAULT 0,"
                + " blog_id             INTEGER DEFAULT 0,"
                + " feed_id             INTEGER DEFAULT 0,"
                + " feed_item_id        INTEGER DEFAULT 0,"
                + " pseudo_id           TEXT NOT NULL,"
                + "	author_name	        TEXT,"
                + "	author_first_name	TEXT,"
                + " author_id           INTEGER DEFAULT 0,"
                + "	title	            TEXT,"
                + "	excerpt             TEXT,"
                + "	format              TEXT,"
                + " url                 TEXT,"
                + " short_url           TEXT,"
                + " blog_url            TEXT,"
                + " blog_name           TEXT,"
                + " featured_image      TEXT,"
                + " featured_video      TEXT,"
                + " post_avatar         TEXT,"
                + " score               REAL DEFAULT 0,"
                + " date_published      TEXT,"
                + " date_liked          TEXT,"
                + " date_tagged         TEXT,"
                + " num_replies         INTEGER DEFAULT 0,"
                + " num_likes           INTEGER DEFAULT 0,"
                + " is_liked            INTEGER DEFAULT 0,"
                + " is_followed         INTEGER DEFAULT 0,"
                + " is_comments_open    INTEGER DEFAULT 0,"
                + " is_external         INTEGER DEFAULT 0,"
                + " is_private          INTEGER DEFAULT 0,"
                + " is_videopress       INTEGER DEFAULT 0,"
                + " is_jetpack          INTEGER DEFAULT 0,"
                + " primary_tag         TEXT,"
                + " secondary_tag       TEXT,"
                + " attachments_json    TEXT,"
                + " discover_json       TEXT,"
                + "	xpost_post_id		INTEGER DEFAULT 0,"
                + " xpost_blog_id       INTEGER DEFAULT 0,"
                + " railcar_json        TEXT,"
                + " tag_name            TEXT NOT NULL COLLATE NOCASE,"
                + " tag_type            INTEGER DEFAULT 0,"
                + " has_gap_marker      INTEGER DEFAULT 0,"
                + " PRIMARY KEY (pseudo_id, tag_name, tag_type)"
                + ")");

        db.execSQL("CREATE INDEX idx_posts_post_id_blog_id ON tbl_posts(post_id, blog_id)");
        db.execSQL("CREATE INDEX idx_posts_date_published ON tbl_posts(date_published)");
        db.execSQL("CREATE INDEX idx_posts_date_tagged ON tbl_posts(date_tagged)");
        db.execSQL("CREATE INDEX idx_posts_tag_name ON tbl_posts(tag_name)");

        db.execSQL("CREATE TABLE tbl_post_content ("
                + " post_id     INTEGER DEFAULT 0,"
                + " blog_id     INTEGER DEFAULT 0,"
                + " pseudo_id   TEXT NOT NULL,"
                + " content     TEXT NOT NULL,"
                + " PRIMARY KEY (pseudo_id)"
                + ")");
        db.execSQL("CREATE UNIQUE INDEX idx_post_content_ids ON tbl_post_content(post_id, blog_id)");
    }

    protected static void dropTables(SQLiteDatabase db) {
        db.execSQL("DROP TABLE IF EXISTS tbl_posts");
        db.execSQL("DROP TABLE IF EXISTS tbl_post_content");
    }

    protected static void reset(SQLiteDatabase db) {
        dropTables(db);
        createTables(db);
    }

    /*
     * purge table of unattached/older posts - no need to wrap this in a transaction since it's
     * only called from ReaderDatabase.purge() which already creates a transaction
     */
    protected static int purge(SQLiteDatabase db) {
        // delete posts attached to tags that no longer exist
        int numDeleted = db.delete("tbl_posts", "tag_name NOT IN (SELECT DISTINCT tag_name FROM tbl_tags)", null);

        // delete excess posts on a per-tag basis
        ReaderTagList tags = ReaderTagTable.getAllTags();
        for (ReaderTag tag: tags) {
            numDeleted += purgePostsForTag(db, tag);
        }

        // delete search results
        numDeleted += purgeSearchResults(db);

        // delete content attached to posts that no longer exist
        if (numDeleted > 0) {
            db.delete("tbl_post_content", "pseudo_id NOT IN (SELECT DISTINCT pseudo_id FROM tbl_posts)", null);
        }

        return numDeleted;
    }

    /*
     * purge excess posts in the passed tag
     */
    private static final int MAX_POSTS_PER_TAG = ReaderConstants.READER_MAX_POSTS_TO_DISPLAY;
    private static int purgePostsForTag(SQLiteDatabase db, ReaderTag tag) {
        int numPosts = getNumPostsWithTag(tag);
        if (numPosts <= MAX_POSTS_PER_TAG) {
            return 0;
        }

        String[] args = {tag.getTagSlug(), Integer.toString(tag.tagType.toInt()), Integer.toString(MAX_POSTS_PER_TAG)};
        String where = "pseudo_id NOT IN (SELECT DISTINCT pseudo_id FROM tbl_posts WHERE tag_name=? AND "
                       + "tag_type=? ORDER BY " + getSortColumnForTag(tag) + " DESC LIMIT ?)";
        int numDeleted = db.delete("tbl_posts", where, args);
        AppLog.d(AppLog.T.READER, String.format("reader post table > purged %d posts in tag %s", numDeleted, tag.getTagNameForLog()));
        return numDeleted;
    }

    /*
     * purge all posts that were retained from previous searches
     */
    private static int purgeSearchResults(SQLiteDatabase db) {
        String[] args = {Integer.toString(ReaderTagType.SEARCH.toInt())};
        return db.delete("tbl_posts", "tag_type=?", args);
    }

    public static int getNumPostsInBlog(long blogId) {
        if (blogId == 0) {
            return 0;
        }
        return SqlUtils.intForQuery(ReaderDatabase.getReadableDb(),
                "SELECT count(*) FROM tbl_posts WHERE blog_id=? AND tag_name=''",
                new String[]{Long.toString(blogId)});
    }

    public static int getNumPostsInFeed(long feedId) {
        if (feedId == 0) {
            return 0;
        }
        return SqlUtils.intForQuery(ReaderDatabase.getReadableDb(),
                "SELECT count(*) FROM tbl_posts WHERE feed_id=? AND tag_name=''",
                new String[]{Long.toString(feedId)});
    }

    public static int getNumPostsWithTag(ReaderTag tag) {
        if (tag == null) {
            return 0;
        }
        String[] args = {tag.getTagSlug(), Integer.toString(tag.tagType.toInt())};
        return SqlUtils.intForQuery(ReaderDatabase.getReadableDb(),
                    "SELECT count(*) FROM tbl_posts WHERE tag_name=? AND tag_type=?",
                    args);
    }

    public static void addOrUpdatePost(ReaderPost post) {
        if (post == null) {
            return;
        }
        ReaderPostList posts = new ReaderPostList();
        posts.add(post);
        addOrUpdatePosts(null, posts);
    }

    public static ReaderPost getBlogPost(long blogId, long postId, boolean excludeContent) {
        return getPost(false, blogId, postId, excludeContent);
    }

    public static ReaderPost getFeedPost(long feedId, long feedItemId, boolean excludeContent) {
        return getPost(true, feedId, feedItemId, excludeContent);
    }

    private static ReaderPost getPost(boolean isFeed, long blogOrFeedId, long postOrItemId, boolean excludeContent) {
        String sql = "SELECT * FROM tbl_posts WHERE "
                + (isFeed ? "feed_id" : "blog_id") + "=? AND " + (isFeed ? "feed_item_id" : "post_id") + "=? LIMIT 1";

        String[] args = new String[] {Long.toString(blogOrFeedId), Long.toString(postOrItemId)};
        Cursor c = ReaderDatabase.getReadableDb().rawQuery(sql, args);
        try {
            if (!c.moveToFirst()) {
                return null;
            }
            ReaderPost post = getPostFromCursor(c);
            if (!excludeContent) {
                post.setContent(getPostContent(post.getPseudoId()));
            }
            return post;
        } finally {
            SqlUtils.closeCursor(c);
        }
    }

    public static String getPostTitle(long blogId, long postId) {
        String[] args = {Long.toString(blogId), Long.toString(postId)};
        return SqlUtils.stringForQuery(ReaderDatabase.getReadableDb(),
                "SELECT title FROM tbl_posts WHERE blog_id=? AND post_id=?",
                args);
    }

    public static String getPostContent(long blogId, long postId) {
        String[] args = {Long.toString(blogId), Long.toString(postId)};
        return SqlUtils.stringForQuery(ReaderDatabase.getReadableDb(),
                "SELECT content FROM tbl_post_content WHERE blog_id=? AND post_id=?",
                args);
    }

    public static String getPostContent(String pseudoId) {
        String[] args = {StringUtils.notNullStr(pseudoId)};
        return SqlUtils.stringForQuery(ReaderDatabase.getReadableDb(),
                "SELECT content FROM tbl_post_content WHERE pseudo_id=?",
                args);
    }

    public static boolean postExists(long blogId, long postId) {
        String[] args = {Long.toString(blogId), Long.toString(postId)};
        return SqlUtils.boolForQuery(ReaderDatabase.getReadableDb(),
                "SELECT 1 FROM tbl_posts WHERE blog_id=? AND post_id=?",
                args);
    }

    /*
     * returns whether any of the passed posts are new or changed - used after posts are retrieved
     */
    public static ReaderActions.UpdateResult comparePosts(ReaderPostList posts) {
        if (posts == null || posts.size() == 0) {
            return ReaderActions.UpdateResult.UNCHANGED;
        }

        boolean hasChanges = false;
        for (ReaderPost post: posts) {
            ReaderPost existingPost = getBlogPost(post.blogId, post.postId, true);
            if (existingPost == null) {
                return ReaderActions.UpdateResult.HAS_NEW;
            } else if (!hasChanges && !post.isSamePost(existingPost)) {
                hasChanges = true;
            }
        }

        return (hasChanges ? ReaderActions.UpdateResult.CHANGED : ReaderActions.UpdateResult.UNCHANGED);
    }

    /*
     * returns true if any posts in the passed list exist in this list
     */
    public static boolean hasOverlap(ReaderPostList posts) {
        for (ReaderPost post: posts) {
            if (postExists(post.blogId, post.postId)) {
                return true;
            }
        }
        return false;
    }

    /*
     * returns the #comments known to exist for this post (ie: #comments the server says this post has), which
     * may differ from ReaderCommentTable.getNumCommentsForPost (which returns # local comments for this post)
     */
    public static int getNumCommentsForPost(ReaderPost post) {
        if (post == null) {
            return 0;
        }
        String[] args = new String[] {Long.toString(post.blogId), Long.toString(post.postId)};
        return SqlUtils.intForQuery(ReaderDatabase.getReadableDb(),
                "SELECT num_replies FROM tbl_posts WHERE blog_id=? AND post_id=?",
                args);
    }

    /*
     * returns the #likes known to exist for this post (ie: #likes the server says this post has), which
     * may differ from ReaderPostTable.getNumLikesForPost (which returns # local likes for this post)
     */
    public static int getNumLikesForPost(long blogId, long postId) {
        String[] args = {Long.toString(blogId), Long.toString(postId)};
        return SqlUtils.intForQuery(ReaderDatabase.getReadableDb(),
                "SELECT num_likes FROM tbl_posts WHERE blog_id=? AND post_id=?",
                args);
    }

    public static boolean isPostLikedByCurrentUser(ReaderPost post) {
        return post != null && isPostLikedByCurrentUser(post.blogId, post.postId);
    }
    public static boolean isPostLikedByCurrentUser(long blogId, long postId) {
        String[] args = new String[] {Long.toString(blogId), Long.toString(postId)};
        return SqlUtils.boolForQuery(ReaderDatabase.getReadableDb(),
                "SELECT is_liked FROM tbl_posts WHERE blog_id=? AND post_id=?",
                args);
    }

    /*
     * updates both the like count for a post and whether it's liked by the current user
     */
    public static void setLikesForPost(ReaderPost post, int numLikes, boolean isLikedByCurrentUser) {
        if (post == null) {
            return;
        }

        String[] args = {Long.toString(post.blogId), Long.toString(post.postId)};

        ContentValues values = new ContentValues();
        values.put("num_likes", numLikes);
        values.put("is_liked", SqlUtils.boolToSql(isLikedByCurrentUser));

        ReaderDatabase.getWritableDb().update(
                "tbl_posts",
                values,
                "blog_id=? AND post_id=?",
                args);
    }


    public static boolean isPostFollowed(ReaderPost post) {
        if (post == null) {
            return false;
        }
        String[] args = new String[] {Long.toString(post.blogId), Long.toString(post.postId)};
        return SqlUtils.boolForQuery(ReaderDatabase.getReadableDb(),
                "SELECT is_followed FROM tbl_posts WHERE blog_id=? AND post_id=?",
                args);
    }

    public static int deletePostsWithTag(final ReaderTag tag) {
        if (tag == null) {
            return 0;
        }

        String[] args = {tag.getTagSlug(), Integer.toString(tag.tagType.toInt())};
        return ReaderDatabase.getWritableDb().delete(
                "tbl_posts",
                "tag_name=? AND tag_type=?",
                args);
    }

    public static int deletePostsInBlog(long blogId) {
        String[] args = {Long.toString(blogId)};
        return ReaderDatabase.getWritableDb().delete("tbl_posts", "blog_id = ?", args);
    }

    /*
     * ensure that posts in blogs that are no longer followed don't have their followed status
     * set to true
     */
    public static void updateFollowedStatus() {
        SQLiteStatement statement = ReaderDatabase.getWritableDb().compileStatement(
                  "UPDATE tbl_posts SET is_followed = 0"
                + " WHERE is_followed != 0"
                + " AND blog_id NOT IN (SELECT DISTINCT blog_id FROM tbl_blog_info WHERE is_followed != 0)");
        try {
            int count = statement.executeUpdateDelete();
            if (count > 0) {
                AppLog.d(AppLog.T.READER, String.format("reader post table > marked %d posts unfollowed", count));
            }
        } finally {
            statement.close();
        }
    }

    /*
     * returns the iso8601 date of the oldest post with the passed tag
     */
    public static String getOldestDateWithTag(final ReaderTag tag) {
        if (tag == null) {
            return "";
        }

        // date field depends on the tag
        String dateColumn = getSortColumnForTag(tag);
        String sql = "SELECT " + dateColumn + " FROM tbl_posts"
                   + " WHERE tag_name=? AND tag_type=?"
                   + " ORDER BY " + dateColumn + " LIMIT 1";
        String[] args = {tag.getTagSlug(), Integer.toString(tag.tagType.toInt())};
        return SqlUtils.stringForQuery(ReaderDatabase.getReadableDb(), sql, args);
    }

    /*
     * returns the iso8601 pub date of the oldest post in the passed blog
     */
    public static String getOldestPubDateInBlog(long blogId) {
        String sql = "SELECT date_published FROM tbl_posts"
                  + " WHERE blog_id=? AND tag_name=''"
                  + " ORDER BY date_published LIMIT 1";
        return SqlUtils.stringForQuery(ReaderDatabase.getReadableDb(), sql, new String[]{Long.toString(blogId)});
    }

    public static String getOldestPubDateInFeed(long feedId) {
        String sql = "SELECT date_published FROM tbl_posts"
                  + " WHERE feed_id=? AND tag_name=''"
                  + " ORDER BY date_published LIMIT 1";
        return SqlUtils.stringForQuery(ReaderDatabase.getReadableDb(), sql, new String[]{Long.toString(feedId)});
    }

    public static void removeGapMarkerForTag(final ReaderTag tag) {
        if (tag == null) return;

        String[] args = {tag.getTagSlug(), Integer.toString(tag.tagType.toInt())};
        String sql = "UPDATE tbl_posts SET has_gap_marker=0 WHERE has_gap_marker!=0 AND tag_name=? AND tag_type=?";
        ReaderDatabase.getWritableDb().execSQL(sql, args);
    }

    /*
     * returns the blogId/postId of the post with the passed tag that has a gap marker, or null if none exists
     */
    public static ReaderBlogIdPostId getGapMarkerIdsForTag(final ReaderTag tag) {
        if (tag == null) {
            return null;
        }

        String[] args = {tag.getTagSlug(), Integer.toString(tag.tagType.toInt())};
        String sql = "SELECT blog_id, post_id FROM tbl_posts WHERE has_gap_marker!=0 AND tag_name=? AND tag_type=?";
        Cursor cursor = ReaderDatabase.getReadableDb().rawQuery(sql, args);
        try {
            if (cursor.moveToFirst()) {
                long blogId = cursor.getLong(0);
                long postId = cursor.getLong(1);
                return new ReaderBlogIdPostId(blogId, postId);
            } else {
                return null;
            }
        } finally {
            SqlUtils.closeCursor(cursor);
        }
    }

    public static void setGapMarkerForTag(long blogId, long postId, ReaderTag tag) {
        if (tag == null) return;

        String[] args = {
                Long.toString(blogId),
                Long.toString(postId),
                tag.getTagSlug(),
                Integer.toString(tag.tagType.toInt())
        };
        String sql = "UPDATE tbl_posts SET has_gap_marker=1 WHERE blog_id=? AND post_id=? AND tag_name=? AND tag_type=?";
        ReaderDatabase.getWritableDb().execSQL(sql, args);
    }

    public static String getGapMarkerDateForTag(ReaderTag tag) {
        ReaderBlogIdPostId ids = getGapMarkerIdsForTag(tag);
        if (ids == null) {
            return null;
        }

        String dateColumn = getSortColumnForTag(tag);
        String[] args = {Long.toString(ids.getBlogId()), Long.toString(ids.getPostId())};
        String sql = "SELECT " + dateColumn + " FROM tbl_posts WHERE blog_id=? AND post_id=?";
        return SqlUtils.stringForQuery(ReaderDatabase.getReadableDb(), sql, args);
    }

    /*
     * the column posts are sorted by depends on the type of tag stream being displayed:
     *
     *      liked posts      sort by the date the post was liked
     *      followed posts   sort by the date the post was published
     *      search results   sort by score
     *      tagged posts     sort by the date the post was tagged
     */
    private static String getSortColumnForTag(ReaderTag tag) {
        if (tag.isPostsILike()) {
            return "date_liked";
        } else if (tag.isFollowedSites()) {
            return "date_published";
        } else if (tag.tagType == ReaderTagType.SEARCH) {
            return "score";
        } else if (tag.isTagTopic()) {
            return "date_tagged";
        } else {
            return "date_published";
        }
    }

    /*
     * delete posts with the passed tag that come before the one with the gap marker for
     * this tag - note this may leave some stray posts in tbl_posts, but these will
     * be cleaned up by the next purge
     */
    public static void deletePostsBeforeGapMarkerForTag(ReaderTag tag) {
        String gapMarkerDate = getGapMarkerDateForTag(tag);
        if (TextUtils.isEmpty(gapMarkerDate)) return;

        String dateColumn = getSortColumnForTag(tag);
        String[] args = {gapMarkerDate, tag.getTagSlug(), Integer.toString(tag.tagType.toInt())};
        String where = "tag_name=? AND tag_type=? AND " + dateColumn + " < ?";
        int numDeleted = ReaderDatabase.getWritableDb().delete("tbl_posts", where, args);
        if (numDeleted > 0) {
            AppLog.d(AppLog.T.READER, "removed " + numDeleted + " posts older than gap marker");
        }
    }

    public static void setFollowStatusForPostsInBlog(long blogId, boolean isFollowed) {
        setFollowStatusForPosts(blogId, 0, isFollowed);
    }
    public static void setFollowStatusForPostsInFeed(long feedId, boolean isFollowed) {
        setFollowStatusForPosts(0, feedId, isFollowed);
    }
    private static void setFollowStatusForPosts(long blogId, long feedId, boolean isFollowed) {
        if (blogId == 0 && feedId == 0) {
            return;
        }

        SQLiteDatabase db = ReaderDatabase.getWritableDb();
        db.beginTransaction();
        try {
            if (blogId != 0) {
                String sql = "UPDATE tbl_posts SET is_followed=" + SqlUtils.boolToSql(isFollowed)
                          + " WHERE blog_id=?";
                db.execSQL(sql, new String[]{Long.toString(blogId)});
            } else {
                String sql = "UPDATE tbl_posts SET is_followed=" + SqlUtils.boolToSql(isFollowed)
                          + " WHERE feed_id=?";
                db.execSQL(sql, new String[]{Long.toString(feedId)});
            }


            // if blog/feed is no longer followed, remove its posts tagged with "Followed Sites"
            if (!isFollowed) {
                if (blogId != 0) {
                    db.delete("tbl_posts", "blog_id=? AND tag_name=?",
                            new String[]{Long.toString(blogId), ReaderTag.TAG_TITLE_FOLLOWED_SITES});
                } else {
                    db.delete("tbl_posts", "feed_id=? AND tag_name=?",
                            new String[]{Long.toString(feedId), ReaderTag.TAG_TITLE_FOLLOWED_SITES});
                }
            }

            db.setTransactionSuccessful();
        } finally {
            db.endTransaction();
        }
    }

    public static void addOrUpdatePosts(final ReaderTag tag, ReaderPostList posts) {
        if (posts == null || posts.size() == 0) {
            return;
        }

        SQLiteDatabase db = ReaderDatabase.getWritableDb();
        SQLiteStatement stmtPosts = db.compileStatement(
                "INSERT OR REPLACE INTO tbl_posts ("
                        + COLUMN_NAMES
                        + ") VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27,?28,?29,?30,?31,?32,?33,?34,?35,?36,?37,?38,?39,?40,?41)");

        SQLiteStatement stmtContent = db.compileStatement(
                "INSERT OR REPLACE INTO tbl_post_content (blog_id, post_id, pseudo_id, content) VALUES (?1,?2,?3,?4)");

        db.beginTransaction();
        try {
            String tagName = (tag != null ? tag.getTagSlug() : "");
            int tagType = (tag != null ? tag.tagType.toInt() : 0);

            // we can safely assume there's no gap marker because any existing gap marker is
            // already removed before posts are updated
            boolean hasGapMarker = false;

            for (ReaderPost post: posts) {
                stmtPosts.bindLong  (1,  post.postId);
                stmtPosts.bindLong  (2,  post.blogId);
                stmtPosts.bindLong  (3,  post.feedId);
                stmtPosts.bindLong  (4,  post.feedItemId);
                stmtPosts.bindString(5,  post.getPseudoId());
                stmtPosts.bindString(6,  post.getAuthorName());
                stmtPosts.bindString(7,  post.getAuthorFirstName());
                stmtPosts.bindLong  (8,  post.authorId);
                stmtPosts.bindString(9,  post.getTitle());
                stmtPosts.bindString(10, post.getExcerpt());
                stmtPosts.bindString(11, post.getFormat());
                stmtPosts.bindString(12, post.getUrl());
                stmtPosts.bindString(13, post.getShortUrl());
                stmtPosts.bindString(14, post.getBlogUrl());
                stmtPosts.bindString(15, post.getBlogName());
                stmtPosts.bindString(16, post.getFeaturedImage());
                stmtPosts.bindString(17, post.getFeaturedVideo());
                stmtPosts.bindString(18, post.getPostAvatar());
                stmtPosts.bindDouble(19, post.score);
                stmtPosts.bindString(20, post.getDatePublished());
                stmtPosts.bindString(21, post.getDateLiked());
                stmtPosts.bindString(22, post.getDateTagged());
                stmtPosts.bindLong  (23, post.numReplies);
                stmtPosts.bindLong  (24, post.numLikes);
                stmtPosts.bindLong  (25, SqlUtils.boolToSql(post.isLikedByCurrentUser));
                stmtPosts.bindLong  (26, SqlUtils.boolToSql(post.isFollowedByCurrentUser));
                stmtPosts.bindLong  (27, SqlUtils.boolToSql(post.isCommentsOpen));
                stmtPosts.bindLong  (28, SqlUtils.boolToSql(post.isExternal));
                stmtPosts.bindLong  (29, SqlUtils.boolToSql(post.isPrivate));
                stmtPosts.bindLong  (30, SqlUtils.boolToSql(post.isVideoPress));
                stmtPosts.bindLong  (31, SqlUtils.boolToSql(post.isJetpack));
                stmtPosts.bindString(32, post.getPrimaryTag());
                stmtPosts.bindString(33, post.getSecondaryTag());
                stmtPosts.bindString(34, post.getAttachmentsJson());
                stmtPosts.bindString(35, post.getDiscoverJson());
                stmtPosts.bindLong  (36, post.xpostPostId);
                stmtPosts.bindLong  (37, post.xpostBlogId);
                stmtPosts.bindString(38, post.getRailcarJson());
                stmtPosts.bindString(39, tagName);
                stmtPosts.bindLong  (40, tagType);
                stmtPosts.bindLong  (41, SqlUtils.boolToSql(hasGapMarker));
                stmtPosts.execute();

                if (post.hasContent()) {
                    stmtContent.bindLong  (1,  post.postId);
                    stmtContent.bindLong  (2,  post.blogId);
                    stmtContent.bindString(3,  post.getPseudoId());
                    stmtContent.bindString(4,  post.getContent());
                    stmtContent.execute();
                }
            }

            db.setTransactionSuccessful();

        } finally {
            db.endTransaction();
            SqlUtils.closeStatement(stmtPosts);
            SqlUtils.closeStatement(stmtContent);
        }
    }

    public static ReaderPostList getPostsWithTag(ReaderTag tag, int maxPosts) {
        if (tag == null) {
            return new ReaderPostList();
        }

        String sql = "SELECT * FROM tbl_posts WHERE tag_name=? AND tag_type=?";

        if (tag.tagType == ReaderTagType.DEFAULT) {
            // skip posts that are no longer liked if this is "Posts I Like", skip posts that are no
            // longer followed if this is "Followed Sites"
            if (tag.isPostsILike()) {
                sql += " AND is_liked != 0";
            } else if (tag.isFollowedSites()) {
                sql += " AND is_followed != 0";
            }
        }

        sql += " ORDER BY " + getSortColumnForTag(tag) + " DESC";

        if (maxPosts > 0) {
            sql += " LIMIT " + Integer.toString(maxPosts);
        }

        String[] args = {tag.getTagSlug(), Integer.toString(tag.tagType.toInt())};
        Cursor cursor = ReaderDatabase.getReadableDb().rawQuery(sql, args);
        try {
            return getPostListFromCursor(cursor);
        } finally {
            SqlUtils.closeCursor(cursor);
        }
    }

    public static ReaderPostList getPostsInBlog(long blogId, int maxPosts) {
        String sql = "SELECT * FROM tbl_posts WHERE blog_id=? AND tag_name='' ORDER BY date_published DESC";

        if (maxPosts > 0) {
            sql += " LIMIT " + Integer.toString(maxPosts);
        }

        Cursor cursor = ReaderDatabase.getReadableDb().rawQuery(sql, new String[]{Long.toString(blogId)});
        try {
            return getPostListFromCursor(cursor);
        } finally {
            SqlUtils.closeCursor(cursor);
        }
    }

    public static ReaderPostList getPostsInFeed(long feedId, int maxPosts) {
        String sql = "SELECT * FROM tbl_posts WHERE feed_id=? AND tag_name='' ORDER BY date_published DESC";

        if (maxPosts > 0) {
            sql += " LIMIT " + Integer.toString(maxPosts);
        }

        Cursor cursor = ReaderDatabase.getReadableDb().rawQuery(sql, new String[]{Long.toString(feedId)});
        try {
            return getPostListFromCursor(cursor);
        } finally {
            SqlUtils.closeCursor(cursor);
        }
    }

    /*
     * same as getPostsWithTag() but only returns the blogId/postId pairs
     */
    public static ReaderBlogIdPostIdList getBlogIdPostIdsWithTag(ReaderTag tag, int maxPosts) {
        ReaderBlogIdPostIdList idList = new ReaderBlogIdPostIdList();
        if (tag == null) {
            return idList;
        }

        String sql = "SELECT blog_id, post_id FROM tbl_posts WHERE tag_name=? AND tag_type=?";

        if (tag.tagType == ReaderTagType.DEFAULT) {
            if (tag.isPostsILike()) {
                sql += " AND is_liked != 0";
            } else if (tag.isFollowedSites()) {
                sql += " AND is_followed != 0";
            }
        }

        sql += " ORDER BY " + getSortColumnForTag(tag) + " DESC";

        if (maxPosts > 0) {
            sql += " LIMIT " + Integer.toString(maxPosts);
        }

        String[] args = {tag.getTagSlug(), Integer.toString(tag.tagType.toInt())};
        Cursor cursor = ReaderDatabase.getReadableDb().rawQuery(sql, args);
        try {
            if (cursor != null && cursor.moveToFirst()) {
                do {
                    idList.add(new ReaderBlogIdPostId(cursor.getLong(0), cursor.getLong(1)));
                } while (cursor.moveToNext());
            }
            return idList;
        } finally {
            SqlUtils.closeCursor(cursor);
        }
    }

    /*
     * same as getPostsInBlog() but only returns the blogId/postId pairs
     */
    public static ReaderBlogIdPostIdList getBlogIdPostIdsInBlog(long blogId, int maxPosts) {
        String sql = "SELECT post_id FROM tbl_posts WHERE blog_id=? AND tag_name='' ORDER BY date_published DESC";

        if (maxPosts > 0) {
            sql += " LIMIT " + Integer.toString(maxPosts);
        }

        Cursor cursor = ReaderDatabase.getReadableDb().rawQuery(sql, new String[]{Long.toString(blogId)});
        try {
            ReaderBlogIdPostIdList idList = new ReaderBlogIdPostIdList();
            if (cursor != null && cursor.moveToFirst()) {
                do {
                    idList.add(new ReaderBlogIdPostId(blogId, cursor.getLong(0)));
                } while (cursor.moveToNext());
            }

            return idList;
        } finally {
            SqlUtils.closeCursor(cursor);
        }
    }

    private static ReaderPost getPostFromCursor(Cursor c) {
        if (c == null) {
            throw new IllegalArgumentException("getPostFromCursor > null cursor");
        }

        ReaderPost post = new ReaderPost();

        post.postId = c.getLong(c.getColumnIndex("post_id"));
        post.blogId = c.getLong(c.getColumnIndex("blog_id"));
        post.feedId = c.getLong(c.getColumnIndex("feed_id"));
        post.feedItemId = c.getLong(c.getColumnIndex("feed_item_id"));
        post.authorId = c.getLong(c.getColumnIndex("author_id"));
        post.setPseudoId(c.getString(c.getColumnIndex("pseudo_id")));

        post.setAuthorName(c.getString(c.getColumnIndex("author_name")));
        post.setAuthorFirstName(c.getString(c.getColumnIndex("author_first_name")));
        post.setBlogName(c.getString(c.getColumnIndex("blog_name")));
        post.setBlogUrl(c.getString(c.getColumnIndex("blog_url")));
        post.setExcerpt(c.getString(c.getColumnIndex("excerpt")));
        post.setFormat(c.getString(c.getColumnIndex("format")));
        post.setFeaturedImage(c.getString(c.getColumnIndex("featured_image")));
        post.setFeaturedVideo(c.getString(c.getColumnIndex("featured_video")));

        post.setTitle(c.getString(c.getColumnIndex("title")));
        post.setUrl(c.getString(c.getColumnIndex("url")));
        post.setShortUrl(c.getString(c.getColumnIndex("short_url")));
        post.setPostAvatar(c.getString(c.getColumnIndex("post_avatar")));

        post.setDatePublished(c.getString(c.getColumnIndex("date_published")));
        post.setDateLiked(c.getString(c.getColumnIndex("date_liked")));
        post.setDateTagged(c.getString(c.getColumnIndex("date_tagged")));

        post.score = c.getDouble(c.getColumnIndex("score"));
        post.numReplies = c.getInt(c.getColumnIndex("num_replies"));
        post.numLikes = c.getInt(c.getColumnIndex("num_likes"));

        post.isLikedByCurrentUser = SqlUtils.sqlToBool(c.getInt(c.getColumnIndex("is_liked")));
        post.isFollowedByCurrentUser = SqlUtils.sqlToBool(c.getInt( c.getColumnIndex("is_followed")));
        post.isCommentsOpen = SqlUtils.sqlToBool(c.getInt(c.getColumnIndex("is_comments_open")));
        post.isExternal = SqlUtils.sqlToBool(c.getInt(c.getColumnIndex("is_external")));
        post.isPrivate = SqlUtils.sqlToBool(c.getInt(c.getColumnIndex("is_private")));
        post.isVideoPress = SqlUtils.sqlToBool(c.getInt(c.getColumnIndex("is_videopress")));
        post.isJetpack = SqlUtils.sqlToBool(c.getInt(c.getColumnIndex("is_jetpack")));

        post.setPrimaryTag(c.getString(c.getColumnIndex("primary_tag")));
        post.setSecondaryTag(c.getString(c.getColumnIndex("secondary_tag")));

        post.setAttachmentsJson(c.getString(c.getColumnIndex("attachments_json")));
        post.setDiscoverJson(c.getString(c.getColumnIndex("discover_json")));

        post.xpostPostId = c.getLong(c.getColumnIndex("xpost_post_id"));
        post.xpostBlogId = c.getLong(c.getColumnIndex("xpost_blog_id"));

        post.setRailcarJson(c.getString(c.getColumnIndex("railcar_json")));

        return post;
    }

    private static ReaderPostList getPostListFromCursor(Cursor cursor) {
        ReaderPostList posts = new ReaderPostList();
        try {
            if (cursor != null && cursor.moveToFirst()) {
                do {
                    posts.add(getPostFromCursor(cursor));
                } while (cursor.moveToNext());
            }
        } catch (IllegalStateException e) {
            CrashlyticsUtils.logException(e, CrashlyticsUtils.ExceptionType.SPECIFIC);
            AppLog.e(AppLog.T.READER, e);
        }
        return posts;
    }
}
