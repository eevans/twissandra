import uuid
import time
import threading

import pycassa
from pycassa.cassandra.ttypes import NotFoundException

from cql import Connection
_local = threading.local()
try:
    conn = _local.conn
except AttributeError:
    conn = _local.conn = Connection('localhost')
    conn.execute("USE twissandra")
    
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


class NotFound(DatabaseError):
    pass


class InvalidDictionary(DatabaseError):
    pass

def _get_line(cf, username, start, limit):
    """
    Gets a timeline or a userline given a username, a start, and a limit.
    """
    # First we need to get the raw timeline (in the form of tweet ids)

    # We get one more tweet than asked for, and if we exceed the limit by doing
    # so, that tweet's key (timestamp) is returned as the 'next' key for
    # pagination.
    next = None
    rows = conn.execute("SELECT FIRST %s REVERSED ?..'' FROM %s WHERE key = ?" % (limit + 1, cf), start or '', username) #V1 "or ''" will be automatic
    if not rows:
        return [], next
    columns = rows[0].columns
    if len(columns) > limit:
        next = columns.pop().name

    tweets = []
    # Now we do a manual join to get the tweets themselves
    for tweet_id in (c.name for c in columns):
        rows = conn.execute("SELECT username, body FROM tweets WHERE key = ?", tweet_id)
        d = _dictify_one_row(rows)
        tweets.append({'id': tweet_id, 'body': d['body'].decode('utf-8'), 'username': d['username']})

    return (tweets, next)


# QUERYING APIs

def get_user_by_username(username):
    """
    Given a username, this gets the user record.
    """
    rows = conn.execute("SELECT password FROM users WHERE key = ?", username)
    if not rows:
        raise NotFound('User %s not found' % (username,))
    return _dictify_one_row(rows)

def get_friend_usernames(username, count=5000):
    """
    Given a username, gets the usernames of the people that the user is
    following.
    """
    rows = conn.execute("SELECT followed FROM following WHERE followed_by = ?", username)
    return [row.columns[0].value for row in rows]

def get_follower_usernames(username, count=5000):
    """
    Given a username, gets the usernames of the people following that user.
    """
    rows = conn.execute("SELECT followed_by FROM following WHERE followed = ?", username)
    return [row.columns[0].value for row in rows]

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
    rows = conn.execute("SELECT username, body FROM tweets WHERE key = ?", tweet_id)
    if not rows:
        raise NotFound('Tweet %s not found' % (tweet_id,))
    d = _dictify_one_row(rows)
    return {'username': d['username'], 'body': d['body'].decode('utf-8')}


# INSERTING APIs

def save_user(username, password):
    """
    Saves the user record.
    """
    conn.execute("UPDATE users SET password = ? WHERE key = ?", password, username)

def save_tweet(username, body):
    """
    Saves the tweet record.
    """
    tweet_id = str(uuid.uuid1())

    # Make sure the tweet body is utf-8 encoded
    body = body.encode('utf-8')

    # Insert the tweet, then into the user's timeline, then into the public one
    conn.execute("UPDATE tweets SET username = ?, body = ? WHERE key = ?", username, body, tweet_id)
    conn.execute("UPDATE userline SET ? = '' WHERE key = ?", tweet_id, username)
    conn.execute("UPDATE userline SET ? = '' WHERE key = ?", tweet_id, PUBLIC_USERLINE_KEY)
    # Get the user's followers, and insert the tweet into all of their streams
    follower_usernames = [username] + get_follower_usernames(username)
    for follower_username in follower_usernames:
        conn.execute("UPDATE timeline SET ? = '' WHERE key = ?", tweet_id, follower_username)

def add_friends(from_username, to_usernames):
    """
    Adds a friendship relationship from one user to some others.
    """
    for to_username in to_usernames:
        row_id = str(uuid.uuid1())
        conn.execute("UPDATE following SET followed = ?, followed_by = ? WHERE key = ?", to_username, from_username, row_id)

def remove_friend(from_username, to_username):
    """
    Removes a friendship relationship from one user to some others.
    """
    rows = conn.execute("SELECT * FROM following WHERE followed = ? AND followed_by = ?", to_username, from_username)
    assert len(rows) == 1
    conn.execute("DELETE FROM following WHERE key = ?", rows[0].key)
