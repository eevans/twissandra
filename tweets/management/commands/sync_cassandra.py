
import cql

from django.core.management.base import NoArgsCommand

class Command(NoArgsCommand):
    def handle_noargs(self, **options):
        # Create Cassandra connection and obtain a cursor.
        conn = cql.connect("localhost", cql_version="3.0.0")
        cursor = conn.cursor()

        # This can result in data loss, so prompt the user first.
        print
        print "Warning:  This will drop any existing keyspace named \"twissandra\","
        print "and delete any data contained within."
        print

        if not raw_input("Are you sure? (y/n) ").lower() in ('y', "yes"):
            print "Ok, then we're done here."
            return

        print "Dropping existing keyspace..."
        cursor.execute("DROP KEYSPACE twissandra")


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
