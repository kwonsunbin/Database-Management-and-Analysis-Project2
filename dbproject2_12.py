import csv 
import pymysql 

#data경로는사용자의경로와비밀번호등으로바꾸어야함
def requirement2(host, user, password):
    conn = pymysql.connect(host=host, user=user, password=password, charset='utf8') 

    curs=conn.cursor()
    #데이터베이스생성
    sql = 'CREATE DATABASE IF NOT EXISTS db2017_12' 
    curs.execute(sql)


requirement2('localhost', 'root', '017330') 
print("Database db2017_12 has been made successfully\n")

#스키마 만들기
def requirement3(host, user, password):
    conn = pymysql.connect(host=host, user=user, password=password,db='db2017_12', charset='utf8')


    try:
        with conn.cursor() as curs:
            sql1 = (
                    'CREATE TABLE IF NOT EXISTS userInfo ('
                    'Id int(11) NOT NULL,'  
                    'Reputation int(11),'
                    'DisplayName varchar(255),'
                    'Age int(11),'
                    'CreationDate TIMESTAMP,'
                    'LastAccessDate TIMESTAMP,'   
                    'WebsiteUrl varchar(255),'
                    'Location varchar(255),'
                    'AboutMe LONGTEXT,'
                    'Primary Key (Id))'
                )
            curs.execute(sql1)

            sql2 = (
               'CREATE TABLE IF NOT EXISTS posts ('  
                'Id int(11) NOT NULL,'           
                'CreationDate TIMESTAMP,'
                'Body longtext,'
                'OwnerUserId int(11),'
                 'LasActivityDate TIMESTAMP,'
                'Primary Key (Id),'                                          
                'Foreign Key (OwnerUserId) references userInfo(Id))'
                 )
            curs.execute(sql2)

            sql3 = (
                'CREATE TABLE IF NOT EXISTS answerPosts ('   
                'Id int(11) NOT NULL,'
                'PostId int(11),'
                'Accepted int(11),'
                'ParentId int(11),'
                'Primary Key (Id),'
                'Foreign Key (PostId) references posts(Id))'
                    )
            curs.execute(sql3)

            sql4 = (
                'CREATE TABLE IF NOT EXISTS questionPosts ('  
                'Id int(11) NOT NULL,'
                'PostId int(11),'
                'AcceptedAnswerId int(11),'
                'ViewCount int(11),'
                'Title varchar(255),'
                'Primary Key (Id),'
                'Foreign Key (PostId) references posts(Id))'

                    )
            curs.execute(sql4)

            sql5 = (
                'CREATE TABLE IF NOT EXISTS questionTags ('    
                'Id int(11) NOT NULL,'
                'TagName varchar(255), '
                'Primary Key (Id, TagName))'

                    )
            curs.execute(sql5)



            sql8 = (
                'CREATE TABLE IF NOT EXISTS postHistory ('   
                'Id int(11) NOT NULL,'
                'PostHistoryTypeId int(11),'
                'PostId int(11),'
                'CreationDate TIMESTAMP,'
                'UserInfoId int(11),'
                'Text longtext,'
                'Comment longtext,'
                'Primary Key (Id),'
                'Foreign Key (PostId) references posts(Id),'
                'Foreign Key (UserInfoId) references userInfo(Id))'
                     )
            curs.execute(sql8)


            sql9 = (
                    'CREATE TABLE IF NOT EXISTS postLinks (' 
                    'Id int(11) NOT NULL,'
                    'CreationDate TIMESTAMP,'
                    'PostId int(11),'
                    'RelatedPostId int(11),'
                    'LinkTypeId int(11),'
                    'Primary Key (Id),'
                    'Foreign Key (PostId) references posts(Id),'
                    'Foreign Key (RelatedPostId) references posts(Id))'
                   )
            curs.execute(sql9)


            sql10 = (
                    'CREATE TABLE IF NOT EXISTS badges ('    
                    'Id int(11) NOT NULL,'
                    'UserInfoId int(11),'
                    'Name varchar(255),'
                    'Date TIMESTAMP,'
                    'Primary Key (Id),'
                    'Foreign Key (UserInfoId) references userInfo(Id))'
                )
            curs.execute(sql10)


            sql11 = (
                    'CREATE TABLE IF NOT EXISTS comments ('  
                    'Id int(11) NOT NULL,'
                    'PostId int(11),'
                    'Score int(11),'
                    'CreationDate TIMESTAMP,'
                    'UserInfoId int(11),'
                    'Primary Key (Id),'
                    'Foreign Key (PostId) references posts(Id),'
                    'Foreign Key (UserInfoId) references userInfo(Id))'
                )
            curs.execute(sql11)


            sql12 = (
                    'CREATE TABLE IF NOT EXISTS tags ('   
                    'Id int(11) NOT NULL,'
                    'TagName varchar(255),'
                    'Primary Key (Id))'
                )
            curs.execute(sql12)


            sql13 = (
                    'CREATE TABLE IF NOT EXISTS tagsPosts ('   
                    'Id int(11) NOT NULL,'
                    'ExcerptPostId int(11),'
                    'WikiPostId int(11),'
                    'Primary Key (Id),'
                    'Foreign Key (WikiPostId) references posts(Id),'
                    'Foreign Key (ExcerptPostId) references posts(Id)'
                    ')'
                )
            curs.execute(sql13)

            sql14 = (
                    'CREATE TABLE IF NOT EXISTS votes ('   
                    'Id int(11) NOT NULL,'
                    'PostId int(11), '
                    'VoteTypeId int(11), '
                    'CreationDate TIMESTAMP,'
                    'PRIMARY KEY (Id),'
                    'Foreign Key (PostId) references posts(Id))'
                )
            curs.execute(sql14)

            sql15 = (
                    'CREATE TABLE IF NOT EXISTS votesBookMark ('   
                    'Id int(11) NOT NULL,'
                    'UserInfoId int(11),'
                    'PRIMARY KEY (Id),'
                    'Foreign Key (Id) references votes(Id),'
                    'Foreign Key (UserInfoId) references'
                    ' userInfo(Id))'
        )
            curs.execute(sql15)

            sql16 = (
                    'CREATE TABLE IF NOT EXISTS votesBountyAmount ('   
                    'Id int(11) NOT NULL,'
                    'PRIMARY KEY (Id),'
                    'BountyAmount int(11),'
                    'Foreign Key (Id) references votes(Id))'
        )
            curs.execute(sql16)
            conn.commit()
    finally:
            conn.close()


requirement3('localhost', 'root', '017330' )
print("Schema Completed\n")

#데이터 전처리 후 SQL에 올리기
print("Data Preprocessing & Loading...  It takes minutes\n")

def requirement4(host, user, password):
    conn = pymysql.connect(host=host, user=user, password=password, db='db2017_12', charset='utf8')   

    curs=conn.cursor()

    f = open('./dataset_revised/userInfo.csv', 'r', encoding='utf-8',  
             errors='replace')
    rdr = csv.reader(f)
    next(rdr, None)
    userInfo = []

    for line in rdr:
        for i in (0, 1, 3):              
            if line[i] != "":
                line[i] = int(line[i])
            else:
                line[i] = None
        for j in (2, 4, 5, 6, 7, 8):        
            if line[j] == "":
                line[j] = None
        userInfo.append(line)

    f.close()

    f = open('./dataset_revised/posts.csv', 'r', encoding='utf-8',
             errors='replace')
    rdr = csv.reader(f)
    next(rdr, None)
    posts = []

    for line in rdr:
        for i in (0, 3):
            if line[i] != "":
                line[i] = int(line[i])
            else:
                line[i] = None
        for j in (1, 2, 4):
            if line[j] == "":
                line[j] = None
        posts.append(line)

    f.close()

    f = open('./dataset_revised/answerPosts.csv', 'r', encoding='utf-8',
             errors='replace')
    rdr = csv.reader(f)
    next(rdr, None)
    answerPosts = []

    for line in rdr:
        for i in (0, 1, 3):
            if line[i] != "":
                line[i] = int(line[i])
            else:
                line[i] = None
        if line[2] == "":
            line[2] = None
        answerPosts.append(line)

    f.close()

    f = open('./dataset_revised/questionPosts.csv', 'r', encoding='utf-8',
             errors='replace')
    rdr = csv.reader(f)
    next(rdr, None)
    questionPosts = []
    questionTags = []

    for line in rdr:
        for i in (0, 1, 2, 3):
            if line[i] != "":
                line[i] = int(line[i])
            else:
                line[i] = None
        for j in (4, 5):
            if line[j] == "":
                line[j] = None

        for tagname in line[5].split('<')[1:]:
            line2 = []
            line2.append(line[0])
            line2.append(tagname.replace('>', ''))

            questionTags.append(line2)

        del line[5]
        questionPosts.append(line)

    f.close()

    f = open('./dataset_revised/postHistory.csv', 'r', encoding='utf-8',
             errors='replace')
    rdr = csv.reader(f)
    next(rdr, None)
    postHistory = []

    for line in rdr:
        for i in (0, 1, 2, 4):
            if line[i] != "":
                line[i] = int(line[i])
            else:
                line[i] = None
        for j in (3, 5, 6):
            if line[j] == "":
                line[j] = None
        postHistory.append(line)

    f.close()

    f = open('./dataset_revised/postLinks.csv', 'r', encoding='utf-8',
             errors='replace')
    rdr = csv.reader(f)
    next(rdr, None)
    postLinks = []

    for line in rdr:
        for i in (0, 2, 3, 4):
            if line[i] != "":
                line[i] = int(line[i])
            else:
                line[i] = None
        if line[1] == "":
            line[1] = None
        postLinks.append(line)

    f.close()

    f = open('./dataset_revised/badges.csv', 'r', encoding='utf-8',
             errors='replace')
    rdr = csv.reader(f)
    next(rdr, None)
    badges = []

    for line in rdr:
        for i in (0, 1):
            if line[i] != "":
                line[i] = int(line[i])
            else:
                line[i] = None
        for j in (2, 3):
            if line[j] == "":
                line[j] = None
        badges.append(line)

    f.close()

    f = open('./dataset_revised/comments.csv', 'r', encoding='utf-8',
             errors='replace')
    rdr = csv.reader(f)
    next(rdr, None)
    comments = []

    for line in rdr:
        for i in (0, 1):
            if line[i] != "":
                line[i] = int(line[i])
            else:
                line[i] = None
        for j in (2, 3):
            if line[j] == "":
                line[j] = None
        comments.append(line)

    f.close()

    f = open('./dataset_revised/tags.csv', 'r', encoding='utf-8',
             errors='replace')
    rdr = csv.reader(f)
    next(rdr, None)
    tags = []
    tagsPosts = []

    for line in rdr:
        line2 = []
        for i in (0, 2, 3):
            if line[i] != "":
                line[i] = int(line[i])
            else:
                line[i] = None
        if line[1] == "":
            line[1] = None
        line2.append(line[0])
        line2.append(line[2])
        line2.append(line[3])
        del line[2]
        del line[2]
        tags.append(line)
        if line2[1] != None:
            tagsPosts.append(line2)
    f.close()

    f = open('./dataset_revised/votes.csv', 'r', encoding='utf-8',
             errors='replace')
    rdr = csv.reader(f)
    next(rdr, None)
    votes = []
    votesBookMark = []
    votesBountyAmount = []

    for line in rdr:
        for i in (0, 1, 2, 4, 5):
            if line[i] != "":
                line[i] = int(line[i])
            else:
                line[i] = None
        if line[3] == "":
            line[3] = None

        line2 = []
        line3 = []

        line2.append(line[0])
        line2.append(line[4])

        line3.append(line[0])
        line3.append(line[5])

        del line[4]
        del line[4]

        votes.append(line)
        if line[2] == 5:
            votesBookMark.append(line2)
        if line[2] == 9:
            votesBountyAmount.append(line3)

    f.close()



    try:
        with conn.cursor() as cursor: 
            cursor.executemany("insert into userInfo(Id, Reputation, DisplayName, Age, CreationDate, LastAccessDate, WebsiteUrl, Location, AboutMe) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)", userInfo)
            cursor.executemany("insert into posts(Id, CreationDate, Body, OwnerUserId, LasActivityDate) values(%s,%s,%s,%s,%s)", posts)
            cursor.executemany("insert into answerPosts(Id, PostId, Accepted, ParentId) values(%s,%s,%s,%s)", answerPosts)
            cursor.executemany("insert into questionPosts(Id, PostId, AcceptedAnswerId, ViewCount, Title) values(%s,%s,%s,%s,%s)", questionPosts)
            cursor.executemany("insert into questionTags(Id, TagName) values(%s,%s)", questionTags)
            cursor.executemany("insert into postHistory(Id, PostHistoryTypeId, PostId, CreationDate, UserInfoId, Text, Comment) values(%s,%s,%s,%s,%s,%s,%s)",postHistory)
            cursor.executemany("insert into postLinks(Id, CreationDate, PostId, RelatedPostId, LinkTypeId) values(%s,%s,%s,%s,%s)", postLinks)
            cursor.executemany("insert into badges(Id, UserInfoId, Name, Date) values(%s,%s,%s,%s)", badges)
            cursor.executemany("insert into comments(Id, PostId, Score, CreationDate, UserInfoId) values(%s,%s,%s,%s,%s)", comments)
            cursor.executemany("insert into tags values(%s,%s)", tags)
            cursor.executemany("insert into tagsPosts values(%s ,%s,%s)", tagsPosts)
            cursor.executemany("insert into votes values(%s,%s,%s,%s)", votes)
            cursor.executemany("insert into votesBookMark values(%s,%s)", votesBookMark)
            cursor.executemany("insert into votesBountyAmount values(%s,%s)", votesBountyAmount)

        conn.commit()
    finally:
        pass

    sql6 = (
        'ALTER TABLE questionPosts ADD Foreign Key (AcceptedAnswerId) references answerPosts(Id)'  
    )
    sql7 = (
        'ALTER TABLE answerPosts ADD Foreign Key (ParentId) references questionPosts(Id)'
    )
    curs.execute(sql6)
    curs.execute(sql7)

    conn.commit()


    conn.close()


requirement4('localhost', 'root', '017330' )
print("Data Preporcessing & Loading done\n")

def requirement6(host, user, password):
    conn = pymysql.connect(host=host, user=user, password=password, db='db2017_12', charset='utf8')


    curs=conn.cursor()

    sql =(
        """SELECT *
        FROM userInfo AS U INNER JOIN ((SELECT U.Id, SUM(Q.ViewCount) AS Totalhits
                                        FROM userInfo AS U, posts AS P, questionposts AS Q
                                        WHERE U.Id=P.OwnerUserId AND P.Id=Q.PostId AND (U.Age BETWEEN 10 AND 19) GROUP BY U.Id ORDER BY Totalhits DESC LIMIT 1)
                                        UNION ALL
                                        (SELECT U.Id, SUM(Q.ViewCount) AS Totalhits
                                        FROM userInfo AS U, posts AS P, questionposts AS Q
                                        WHERE U.Id=P.OwnerUserId AND P.Id=Q.PostId AND (U.Age BETWEEN 20 AND 29) GROUP BY U.Id ORDER BY Totalhits DESC LIMIT 1)
                                        UNION ALL
                                        (SELECT U.Id, SUM(Q.ViewCount) AS Totalhits
                                        FROM userInfo AS U, posts AS P, questionposts AS Q
                                        WHERE U.Id=P.OwnerUserId AND P.Id=Q.PostId AND (U.Age BETWEEN 30 AND 39) GROUP BY U.Id ORDER BY Totalhits DESC LIMIT 1)
                                        UNION ALL
                                        (SELECT U.Id, SUM(Q.ViewCount) AS Totalhits
                                        FROM userInfo AS U, posts AS P, questionposts AS Q
                                        WHERE U.Id=P.OwnerUserId AND P.Id=Q.PostId AND (U.Age BETWEEN 40 AND 49) GROUP BY U.Id ORDER BY Totalhits DESC LIMIT 1)
                                        UNION ALL
                                        (SELECT U.Id, SUM(Q.ViewCount) AS Totalhits
                                        FROM userInfo AS U, posts AS P, questionposts AS Q
                                        WHERE U.Id=P.OwnerUserId AND P.Id=Q.PostId AND (U.Age BETWEEN 50 AND 999) GROUP BY U.Id ORDER BY Totalhits DESC LIMIT 1))
                                        AS Agebest ON U.Id=Agebest.Id
        ORDER BY U.Reputation DESC"""

        )

    curs.execute(sql)

    rows = curs.fetchall()

    for row in rows:
      print(row)

    conn.close()

print("R6 Processing...\n")
requirement6('localhost', 'root', '017330' )
print("\n")
print("R6 done\n")

def requirement7(host, user, password):
    conn = pymysql.connect(host=host, user=user, password=password, db='db2017_12', charset='utf8')

    curs=conn.cursor()

    sql = (
        '''SELECT *
        FROM
            (SELECT COUNT(*) AS "2010"
            FROM userInfo AS U
            WHERE Year(U.CreationDate) = 2010
            GROUP BY Year(U.CreationDate) ) AS y2010
               CROSS JOIN
            (SELECT COUNT(*) AS "2011"
            FROM userInfo AS U
            WHERE Year(U.CreationDate) = 2011
            GROUP BY Year(U.CreationDate) ) AS y2011
               CROSS JOIN
            (SELECT COUNT(*) AS "2012"
            FROM userInfo AS U
            WHERE Year(U.CreationDate) = 2012
            GROUP BY Year(U.CreationDate) ) AS y2012
               CROSS JOIN
            (SELECT COUNT(*) AS "2013"
            FROM userInfo AS U
            WHERE Year(U.CreationDate) = 2013
            GROUP BY Year(U.CreationDate) ) AS y2013
               CROSS JOIN
            (SELECT COUNT(*) AS "2014"
            FROM userInfo AS U
            WHERE Year(U.CreationDate) = 2014
            GROUP BY
            Year(U.CreationDate) ) AS y2014'''

    )

    curs.execute(sql)

    rows = curs.fetchall()

    for row in rows:
        print(row)

    conn.close()

print("R7 Processing...\n")
requirement7('localhost', 'root', '017330')
print("\n")
print("R7 done\n")

def requirement8(host, user, password):
    conn = pymysql.connect(host=host, user=user, password=password, db='db2017_12', charset='utf8')

    curs=conn.cursor()

    sql=('''SELECT LIKEDISLIKE.Id, likeoverone, dislikecount, likeoverone-dislikecount AS score
            FROM
           (SELECT P.Id, COUNT(*) AS commentcount
          FROM posts AS P
          INNER JOIN comments AS C
          ON P.Id = C.PostId
          GROUP BY P.Id
          HAVING commentcount >= 10) AS COMMENTCOUNT

           INNER JOIN

           (SELECT LIKEPOST.Id, likeoverone, dislikecount
           FROM
               (SELECT P.Id, COUNT(*) AS likeoverone
                FROM posts AS P
                INNER JOIN votes AS V
                ON P.Id = V.PostId
                WHERE V.VoteTypeId = 2
                GROUP BY P.Id
                HAVING likeoverone >= 1) AS LIKEPOST

                  INNER JOIN

              (SELECT P.Id, COUNT(*) AS dislikecount
               FROM posts AS P
               INNER JOIN votes AS V
               ON P.Id = V.PostId
               WHERE V.VoteTypeId = 3
               GROUP BY P.Id)  AS DISLIKEPOST

              ON LiKEPOST.Id = DISLIKEPOST.Id
          ) AS LIKEDISLIKE

          ON COMMENTCOUNT.Id = LIKEDISLIKE.Id
        ORDER BY score DESC'''
        )

    curs.execute(sql)

    rows = curs.fetchall()

    for row in rows:
        print(row)

    conn.close()

print("R8 Processing...\n")
requirement8('localhost', 'root', '017330')
print("\n")
print("R8 done\n")

def requirement9(host, user, password):
    conn = pymysql.connect(host=host, user=user, password=password, db='db2017_12', charset='utf8')

    curs=conn.cursor()

    sql=(
        '''SELECT getbadge.Id, cbadge, avPost
        FROM
            (SELECT U.Id, COUNT(*) AS cbadge
             FROM userInfo AS U
             LEFT OUTER JOIN badges AS B
             ON U.Id = B.UserInfoId
             UNION
             SELECT U.Id, COUNT(*) AS cbadge
             FROM userInfo AS U
             RIGHT OUTER JOIN badges AS B
            ON U.Id = B.UserInfoId
            GROUP BY U.Id) AS getbadge
        INNER JOIN
            (SELECT postcount.Id, AVG(cPost) AS AVPOST
            FROM(
                SELECT U.Id, COUNT(*) AS cPost
                FROM userInfo AS U
                LEFT OUTER JOIN posts AS P
                ON U.Id = P.OwnerUserId
                UNION
                SELECT U.Id, COUNT(*) AS cPost
                FROM userInfo AS U
                RIGHT OUTER JOIN posts AS P
                ON U.Id = P.OwnerUserId
                WHERE YEAR(P.CreationDate) = 2010
                GROUP BY U.Id
                UNION ALL
                SELECT U.Id, COUNT(*) AS cPost
                FROM userInfo AS U
                LEFT OUTER JOIN posts AS P
                ON U.Id = P.OwnerUserId
                UNION
                SELECT U.Id, COUNT(*) AS cPost
                FROM userInfo AS U
                RIGHT OUTER JOIN posts AS P
                ON U.Id = P.OwnerUserId
                WHERE YEAR(P.CreationDate) = 2011
                GROUP BY U.Id
                UNION ALL
                SELECT U.Id, COUNT(*) AS cPost
                FROM userInfo AS U
                LEFT OUTER JOIN posts AS P
                ON U.Id = P.OwnerUserId
                UNION
                SELECT U.Id, COUNT(*) AS cPost
                FROM userInfo AS U
                RIGHT OUTER JOIN posts AS P
                ON U.Id = P.OwnerUserId
                WHERE YEAR(P.CreationDate) = 2012
                GROUP BY U.Id
                UNION ALL
                SELECT U.Id, COUNT(*) AS cPost
                FROM userInfo AS U
                LEFT OUTER JOIN posts AS P
                ON U.Id = P.OwnerUserId
                UNION
                SELECT U.Id, COUNT(*) AS cPost
                FROM userInfo AS U
                RIGHT OUTER JOIN posts AS P
                ON U.Id = P.OwnerUserId
                WHERE YEAR(P.CreationDate) = 2013
                GROUP BY U.Id
                UNION ALL
                SELECT U.Id, COUNT(*) AS cPost
                FROM userInfo AS U
                LEFT OUTER JOIN posts AS P
                ON U.Id = P.OwnerUserId
                UNION
                SELECT U.Id, COUNT(*) AS cPost
                FROM userInfo AS U
                RIGHT OUTER JOIN posts AS P
                ON U.Id = P.OwnerUserId
                WHERE YEAR(P.CreationDate) = 2014
                GROUP BY U.Id
                ) AS postcount
            GROUP BY postcount.Id
            ) AS postaverage
        ON getbadge.Id = postaverage.Id
        WHERE(cbadge >= 50) AND NOT(getbadge.Id = 0)
        ORDER BY cbadge DESC'''
        )

    curs.execute(sql)

    rows = curs.fetchall()

    for row in rows:
     print(row)

    conn.close()

print("R9 Processing...\n")
requirement9('localhost', 'root', '017330' )
print("\n")
print("R9 done\n")

def requirement10(host, user, password):
    conn = pymysql.connect(host=host, user=user, password=password, db='db2017_12', charset='utf8')

    curs=conn.cursor()

    sql =(
         '''SELECT DATE_FORMAT(P.CreationDate, '%Y-%m') AS M, P.Id, P.Body, maxcomment
             FROM posts AS P
            INNER JOIN
             (SELECT P.Id, COUNT(*) AS maxcomment
            FROM posts AS P
            LEFT OUTER JOIN comments AS C
            ON P.Id = C.PostId

            UNION
            SELECT P.Id, COUNT(*) AS maxcomment
            FROM posts AS P
            RIGHT OUTER JOIN comments AS C
            ON P.Id = C.PostId

            GROUP BY P.Id
            ORDER BY maxcomment DESC ) AS commentcount

        ON P.Id=commentcount.Id
        WHERE P.Id != 0
        GROUP BY M'''
        )
    curs.execute(sql)

    rows = curs.fetchall()

    for row in rows:
        print(row)

    conn.close()
print("R10 Processing...\n")
requirement10('localhost', 'root', '017330' )
print("\n")
print("R10 done\n")