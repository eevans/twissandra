import pycassa
from pycassa.system_manager import *

from cql import Connection

from django.core.management.base import NoArgsCommand

class Command(NoArgsCommand):

    def handle_noargs(self, **options):
        sys = SystemManager()
        conn = Connection('localhost')

        # If there is already a Twissandra keyspace, we have to ask the user
        # what they want to do with it.
        if 'Twissandra' in sys.list_keyspaces():
            msg = 'Looks like you already have a Twissandra keyspace.\nDo you '
            msg += 'want to delete it and recreate it? All current data will '
            msg += 'be deleted! (y/n): '
            resp = raw_input(msg)
            if not resp or resp[0] != 'y':
                print "Ok, then we're done here."
                return
            sys.drop_keyspace('Twissandra')

        conn = Connection("localhost")
        conn.execute("CREATE KEYSPACE twissandra WITH strategy_class='SimpleStrategy' and replication_factor=1") #V1 strategy become optional
        conn.execute("USE twissandra")
        conn.execute("CREATE COLUMNFAMILY users (password utf8)")
        conn.execute("CREATE COLUMNFAMILY following (followed utf8, followed_by utf8)")
        conn.execute("CREATE INDEX following_followed ON following(followed)")
        conn.execute("CREATE INDEX following_followed_by ON following(followed_by)")
        conn.execute("CREATE COLUMNFAMILY tweets (user_id utf8, body utf8)")
        conn.execute("CREATE COLUMNFAMILY timeline WITH comparator=timeuuid")
        conn.execute("CREATE COLUMNFAMILY userline WITH comparator=timeuuid")

        print 'All done!'
