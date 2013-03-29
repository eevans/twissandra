import uuid
import time
import threading
import cql

_local = threading.local()
try:
    cursor = _local.cursor
except AttributeError:
    conn = cql.connect("localhost", cql_version="3.0.0")
    cursor = _local.cursor = conn.cursor()
    cursor.execute("USE twissandra")
    
__all__ = [
    'get_user_by_username', 'get_friend_usernames', 'get_follower_usernames', 'get_timeline',
    'get_userline', 'get_tweet', 'save_user', 'save_tweet', 'add_friends', 'remove_friend',
    'DatabaseError', 'NotFound', 'InvalidDictionary', 'PUBLIC_TIMELINE_KEY'
]

# NOTE: Having a single userline key to store all of the public tweets is not
#       scalable.  Currently, Cassandra requires that an entire row (meaning
#       every column under a given key) to be able to fit in memory.  You can
#       imagine that after a while, the entire public timeline would exceed
#       available memory.
#
#       The fix for this is to partition the timeline by time, so we could use
#       a key like !PUBLIC!2010-04-01 to partition it per day.  We could drill
#       down even further into hourly keys, etc.  Since this is a demonstration
#       and that would add quite a bit of extra code, this excercise is left to
#       the reader.
PUBLIC_TIMELINE_KEY = '!PUBLIC!'


# EXCEPTIONS

class DatabaseError(Exception):
    """
    The base error that functions in this module will raise when things go
    wrong.
    """
    pass

class NotFound(DatabaseError): pass
class InvalidDictionary(DatabaseError): pass


# QUERYING APIs

def get_user_by_username(username):
    """
    Given a username, this gets the user record.
    """
    cursor.execute("SELECT password FROM users WHERE username = :user", dict(user=username))
    if not (cursor.rowcount > 0):
        raise NotFound('User %s not found' % (username,))
    return dict(password=cursor.fetchone()[0])

def get_friend_usernames(username, count=5000):
    """
    Given a username, gets the usernames of the people that the user is
    following.
    """
    cursor.execute("SELECT followed FROM following WHERE username = :user", dict(user=username))
    return [row[0] for row in cursor if cursor.rowcount > 0]

def get_follower_usernames(username, count=5000):
    """
    Given a username, gets the usernames of the people following that user.
    """
    cursor.execute("SELECT following FROM followers WHERE username = :user", dict(user=username))
    return [row[0] for row in cursor if cursor.rowcount > 0]

def get_timeline(username, start=None, limit=40):
    """
    Given a username, get their tweet timeline (tweets from people they follow).
    """
    if start: posted_at_start = str(start)
    else: posted_at_start = "now()"

    query = """
        SELECT tweetid, posted_by, body FROM timeline
        WHERE username = :username AND tweetid < %s ORDER BY tweetid DESC LIMIT %d
    """
    cursor.execute(query % (posted_at_start, limit+1), dict(username=username))

    nextid = None
    tweets = []

    for row in cursor:
        tweets.append({"id": row[0], "username": row[1], "body": row[2]})

    if len(tweets) > limit:
        nextid = tweets.pop()["id"]

    return (tweets, nextid)

def get_userline(username, start=None, limit=40):
    """
    Given a username, get their userline (their tweets).
    """
    if start: posted_at_start = str(start)
    else: posted_at_start = "now()"

    query = """
        SELECT tweetid, body FROM userline
        WHERE username = :username AND tweetid < %s ORDER BY tweetid DESC LIMIT %d
    """
    cursor.execute(query % (posted_at_start, limit+1), dict(username=username))

    nextid = None
    tweets = []

    for row in cursor:
        tweets.append({"id": row[0], "username": username, "body": row[1]})

    if len(tweets) > limit:
        nextid = tweets.pop()["id"]

    return (tweets, nextid)

def get_tweet(tweet_id):
    """
    Given a tweet id, this gets the entire tweet record.
    """
    cursor.execute("SELECT username, body FROM tweets WHERE tweetid = :uuid", dict(uuid=tweet_id))
    if not (cursor.rowcount > 0):
        raise NotFound('Tweet %s not found' % (tweet_id,))
    row = cursor.fetchone()
    return {'username': row[0], 'body': row[1].decode('utf-8')}


# INSERTING APIs

def save_user(username, password):
    """
    Saves the user record.
    """
    cursor.execute(
        "UPDATE users SET password = :password WHERE username = :user_id",
        dict(password=password, user_id=username))

def save_tweet(username, body):
    """
    Saves the tweet record.
    """
    # Create a type 1 UUID based on the current time.
    tweet_id = uuid.uuid1()

    # Make sure the tweet body is utf-8 encoded.
    body = body.encode('utf-8')

    # Insert the tweet, then into the user's userline, then into the public userline.
    cursor.execute(
        "INSERT INTO tweets (tweetid, username, body) VALUES (:tweet_id, :username, :body)",
        dict(tweet_id=tweet_id, username=username, body=body))
    cursor.execute(
        "INSERT INTO userline (username, tweetid, body) VALUES (:username, :posted_at, :body)",
        dict(username=username, posted_at=tweet_id, body=body))
    cursor.execute(
        """INSERT INTO timeline (username, tweetid, posted_by, body)
           VALUES (:username, :posted_at, :posted_by, :body)""",
        dict(username=PUBLIC_TIMELINE_KEY, posted_at=tweet_id, posted_by=username, body=body))

    # Get the user's followers, and insert the tweet into all of their streams
    follower_usernames = [username] + get_follower_usernames(username)
    for follower_username in follower_usernames:
        cursor.execute(
            """INSERT INTO timeline (username, tweetid, posted_by, body)
               VALUES (:username, :posted_at, :posted_by, :body)""",
            dict(username=follower_username, posted_at=tweet_id, posted_by=username, body=body))

def add_friends(from_username, to_usernames):
    """
    Adds a friendship relationship from one user to some others.
    """
    # FIXME: use a BATCH here
    for to_username in to_usernames:
        cursor.execute(
            "INSERT INTO following (username, followed) VALUES (:from_username, :to_username)",
            dict(from_username=from_username, to_username=to_username))
        cursor.execute(
            "INSERT INTO followers (username, following) VALUES (:to_username, :from_username)",
            dict(from_username=from_username, to_username=to_username))

def remove_friend(from_username, to_username):
    """
    Removes a friendship relationship from one user to some others.
    """
    # FIXME: use a BATCH here
    cursor.execute(
        "DELETE FROM following WHERE username = :from_username AND followed = :to_username",
        dict(from_username=from_username, to_username=to_username))
    cursor.execute(
        "DELETE FROM followers WHERE username = :to_username AND following = :from_username",
        dict(from_username=from_username, to_username=to_username))


# vi:se ts=4 sw=4 ai et nu:
