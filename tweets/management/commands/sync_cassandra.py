
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
        try: cursor.execute("DROP KEYSPACE twissandra")
        except: pass

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
                tweetid uuid PRIMARY KEY,
                username text,
                body text
            )
        """)

        print "Creating userline columnfamily..."
        cursor.execute("""
            CREATE TABLE userline (
                tweetid timeuuid,
                username text,
                body text,
                PRIMARY KEY(username, tweetid)
            )
        """)

        print "Creating timeline columnfamily..."
        cursor.execute("""
            CREATE TABLE timeline (
                tweetid timeuuid,
                username text,
                posted_by text,
                body text,
                PRIMARY KEY(username, tweetid)
            )
        """)

        print 'All done!'

# vi:se ai ts=4 sw=4 et:
