
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
        cursor.execute("""
            CREATE TABLE users (
                username text PRIMARY KEY,
                password text
            )
        """)

        print "Creating following columnfamily..."
        cursor.execute("""
            CREATE TABLE following (
                username text,
                followed text,
                PRIMARY KEY(username, followed)
            )
        """)

        print "Creating followers columnfamily..."
        cursor.execute("""
            CREATE TABLE followers (
                username text,
                following text,
                PRIMARY KEY(username, following)
            )
        """)


        print "Creating tweets columnfamily..."
        cursor.execute("""
            CREATE TABLE tweets (
                id uuid PRIMARY KEY,
                username text,
                body text
            )
        """)

        print "Creating userline columnfamily..."
        cursor.execute("""
            CREATE TABLE userline (
                username text,
                posted_at timestamp,
                tweetid uuid,
                body text,
                PRIMARY KEY(username, posted_at)
            )
        """)

        print "Creating timeline columnfamily..."
        cursor.execute("""
            CREATE TABLE timeline (
                username text,
                posted_at timestamp,
                tweetid uuid,
                posted_by text,
                body text,
                PRIMARY KEY(username, posted_at)
            )
        """)

        print 'All done!'

# vi:se ai ts=4 sw=4 et:
