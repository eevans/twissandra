import uuid
import time
import threading
import cql

import pycassa
from pycassa.cassandra.ttypes import NotFoundException

_local = threading.local()
try:
    cursor = _local.cursor
except AttributeError:
    conn = cql.connect("localhost", cql_version="3.0.0")
    cursor = _local.cursor = conn.cursor()
    cursor.execute("USE twissandra")
    
def _dictify_one_row(rows):
    assert len(rows) == 1, rows
    return dict((c.name, c.value) for c in rows[0].columns)

# TODO use uuid bytes in row key rather than string


__all__ = ['get_user_by_username', 'get_friend_usernames',
    'get_follower_usernames',
    'get_timeline', 'get_userline', 'get_tweet', 'save_user',
    'save_tweet', 'add_friends', 'remove_friend', 'DatabaseError',
    'NotFound', 'InvalidDictionary', 'PUBLIC_USERLINE_KEY']

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
PUBLIC_USERLINE_KEY = '!PUBLIC!'


class DatabaseError(Exception):
    """
    The base error that functions in this module will raise when things go
    wrong.
    """
    pass

class NotFound(DatabaseError): pass
class InvalidDictionary(DatabaseError): pass


def _get_line(cf, username, start, limit):
    """
    Gets a timeline or a userline given a username, a start, and a limit.
    """
    # First we need to get the raw timeline (in the form of tweet ids)

    # We get one more tweet than asked for, and if we exceed the limit by doing
    # so, that tweet's key (timestamp) is returned as the 'next' key for
    # pagination.
    next = None

    query = "SELECT tweet FROM %s WHERE username = :username" % cf
    parms = dict(username=username)

    if start:
        query += " AND tweet > :start"
        parms["start"] = start

    query += " ORDER BY tweet DESC LIMIT %d" % (limit + 1)
    cursor.execute(query, parms)

    rows = [row for row in cursor]

    if not rows:
        return [], next

    columns = [row[0] for row in rows]
    if len(columns) > limit:
        next = columns.pop()

    tweets = []
    # Now we do a manual join to get the tweets themselves
    for tweetid in columns:
        cursor.execute("SELECT user_id, body FROM tweets WHERE id = :tweetid",
            dict(tweetid=tweetid))
        row = cursor.fetchone()
	if row:
            tweets.append({'id': tweetid, 'body': row[1].decode('utf-8'), 'username': row[0]})

    return (tweets, next)


# QUERYING APIs

def get_user_by_username(username):
    """
    Given a username, this gets the user record.
    """
    cursor.execute("SELECT password FROM users WHERE id = :user", dict(user=username))
    if not (cursor.rowcount > 0):
        raise NotFound('User %s not found' % (username,))
    return dict(password=cursor.fetchone()[0])

def get_friend_usernames(username, count=5000):
    """
    Given a username, gets the usernames of the people that the user is
    following.
    """
    cursor.execute(
        "SELECT followed FROM following WHERE followed_by = :user",
        dict(user=username))
    return [row[0] for row in cursor if cursor.rowcount > 0]

def get_follower_usernames(username, count=5000):
    """
    Given a username, gets the usernames of the people following that user.
    """
    cursor.execute(
        "SELECT followed_by FROM following WHERE followed = :user",
        dict(user=username))
    return [row[0] for row in cursor if cursor.rowcount > 0]

def get_timeline(username, start=None, limit=40):
    """
    Given a username, get their tweet timeline (tweets from people they follow).
    """
    return _get_line('timeline', username, start, limit)

def get_userline(username, start=None, limit=40):
    """
    Given a username, get their userline (their tweets).
    """
    return _get_line('userline', username, start, limit)

def get_tweet(tweet_id):
    """
    Given a tweet id, this gets the entire tweet record.
    """
    cursor.execute("SELECT username, body FROM tweets WHERE key = :uuid", dict(uuid=tweet_id))
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
        "UPDATE users SET password = :password WHERE id = :user_id",
        dict(password=password, user_id=username))

def save_tweet(username, body):
    """
    Saves the tweet record.
    """
    tweet_id = uuid.uuid1()

    # Make sure the tweet body is utf-8 encoded
    body = body.encode('utf-8')

    # Insert the tweet, then into the user's timeline, then into the public one
    cursor.execute(
        "UPDATE tweets SET user_id = :user_id, body = :body WHERE id = :tweet_id",
        dict(user_id=username, body=body, tweet_id=tweet_id))
    cursor.execute(
        "INSERT INTO userline (username, tweet) VALUES (:username, :tweet)",
        dict(username=username, tweet=tweet_id))
    cursor.execute(
        "INSERT INTO userline (username, tweet) VALUES (:username, :tweet)",
        dict(username=PUBLIC_USERLINE_KEY, tweet=tweet_id))

    # Get the user's followers, and insert the tweet into all of their streams
    follower_usernames = [username] + get_follower_usernames(username)
    for follower_username in follower_usernames:
        cursor.execute(
            "INSERT INTO timeline (username, tweet) VALUES (:username, :tweet_id)",
            dict(username=follower_username, tweet_id=tweet_id))

def add_friends(from_username, to_usernames):
    """
    Adds a friendship relationship from one user to some others.
    """
    for to_username in to_usernames:
        row_id = uuid.uuid1()
        cursor.execute(
            "INSERT INTO following (followed, followed_by) VALUES (:to_user, :from_user)",
            dict(to_user=to_username, from_user=from_username, uuid=row_id))

def remove_friend(from_username, to_username):
    """
    Removes a friendship relationship from one user to some others.
    """
    cursor.execute(
        "SELECT id FROM following WHERE followed = :to_user AND followed_by = :from_user ALLOW FILTERING",
        dict(to_user=to_username, from_user=from_username))
    assert cursor.rowcount == 1
    cursor.execute("DELETE FROM following WHERE id = :rowid", dict(rowid=cursor.fetchone()[0]))


# vi:se ts=4 sw=4 ai et:
