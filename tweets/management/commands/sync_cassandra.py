
import pycassa, cql
from pycassa.system_manager import *

from django.core.management.base import NoArgsCommand

class Command(NoArgsCommand):
    def handle_noargs(self, **options):
        sys = SystemManager()

        # If there is already a Twissandra keyspace, we have to ask the user
        # what they want to do with it.
        if 'twissandra' in sys.list_keyspaces():
            msg = 'Looks like you already have a Twissandra keyspace.\nDo you '
            msg += 'want to delete it and recreate it? All current data will '
            msg += 'be deleted! (y/n): '
            resp = raw_input(msg)
            if not resp or resp[0] != 'y':
                print "Ok, then we're done here."
                return
            sys.drop_keyspace('twissandra')

        conn = cql.connect("localhost", cql_version="3.0.0")
        cursor = conn.cursor()

        print "Creating keyspace..."
        cursor.execute("""
            CREATE KEYSPACE twissandra
                WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """)
        cursor.execute("USE twissandra")

        print "Creating users columnfamily..."
        cursor.execute("CREATE TABLE users (id text PRIMARY KEY, password text)")

        print "Creating following columnfamily..."
        cursor.execute("""
            CREATE TABLE following (id uuid PRIMARY KEY, followed text, followed_by text)
        """)

        print "Creating indexes..."
        cursor.execute("CREATE INDEX following_followed ON following(followed)")
        cursor.execute("CREATE INDEX following_followed_by ON following(followed_by)")

        print "Creating tweets columnfamily..."
        cursor.execute("CREATE TABLE tweets (id uuid PRIMARY KEY, user_id text, body text)")

        print "Creating userline columnfamily..."
        cursor.execute("""
            CREATE TABLE userline (tweet timeuuid, username text, PRIMARY KEY(username, tweet))
        """)

        print "Creating timeline columnfamily..."
        cursor.execute("""
            CREATE TABLE timeline (tweet timeuuid, username text, PRIMARY KEY(username, tweet))
        """)

        print 'All done!'

# vi:se ai ts=4 sw=4 et:
