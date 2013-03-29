# Twissandra

Twissandra is an example project, created to learn and demonstrate how to use
Cassandra.  Running the project will present a website that has similar
functionality to Twitter.

You can see a running copy at [http://twissandra.com/](http://twissandra.com/)

Most of the magic happens in twissandra/cass.py, so check that out.

## Installation

Installing Twissandra is fairly straightforward.  Really it just involves
checking out Cassandra and Twissandra, doing a little configuration, and
then starting it up.  Here's a roadmap of the steps we're going to take to
install the project:

1. Check out the latest Cassandra source code
2. Check out the Twissandra source code
3. Install and configure Cassandra
4. Create a virtual Python environment with Twissandra's dependencies
5. Start up the webserver

### Check out the latest Cassandra source code

    git clone git://git.apache.org/cassandra.git

### Check out the Twissandra source code

    git clone git://github.com/eevans/twissandra.git

### Install and configure Cassandra

Now build Cassandra:

    cd cassandra
    ant

Then we need to create our database directories on disk:

    sudo mkdir -p /var/log/cassandra
    sudo chown -R `whoami` /var/log/cassandra
    sudo mkdir -p /var/lib/cassandra
    sudo chown -R `whoami` /var/lib/cassandra

Finally we can start Cassandra:

    ./bin/cassandra -f

### Create a virtual Python environment with Twissandra's dependencies

First, make sure to have virtualenv installed.  If it isn't installed already,
this should do the trick:

    sudo easy_install -U virtualenv

Now let's create a new virtual environment, and begin using it:

    virtualenv twiss
    source twiss/bin/activate

We should install pip, so that we can more easily install Twissandra's
dependencies into our new virtual environment:

    easy_install -U pip

Now let's install all of the dependencies:

    pip install -U -r twissandra/requirements.txt

Now that we've got all of our dependencies installed, we're ready to start up
the server.

### Create the schema

Make sure you're in the Twissandra checkout, and then run the sync_cassandra
command to create the proper keyspace in Cassandra:

    cd twissandra
    python manage.py sync_cassandra

### Start up the webserver

This is the fun part! We're done setting everything up, we just need to run it:

    python manage.py runserver

Now go to http://127.0.0.1:8000/ and you can play with Twissandra!

## Schema Layout

In Cassandra, the way that your data is structured is very closely tied to how
how it will be retrieved.  Let's start with the user table. The key is a
username, and the columns are the properties on the user:

    -- User storage
    CREATE TABLE users (username text PRIMARY KEY, password text);

Friends and followers are keyed by the username in the `following` and
`followers` tables respectively.  The use of a compound PRIMARY KEY like
this allows us to setup a one to many relationship between a user and the
people they are following, or the people following them.
    
    -- Users user is following
    CREATE TABLE following (
        username text,
        followed text,
        PRIMARY KEY(username, followed)
    );
    
    -- Users who follow user
    CREATE TABLE followers (
        username  text,
        following text,
        PRIMARY KEY(username, following)
    );

Tweets are stored with a UUID for the key.

    -- Tweet storage
    CREATE TABLE tweets (tweetid uuid PRIMARY KEY, username text, body text);

The `timeline` and `userline` tables keep track of which tweets should
appear, and in what order.  To that effect, the partition key is the
username, with columns for the time each was posted, and the text of the
tweet.

The `timeline` table has an additional column for storing the user who
authored the tweet.  This is because the timeline stores a materialized
view of the tweets a user is interested in; tweets created by others:

    -- Materialized view of tweets created by user
    CREATE TABLE userline (
        tweetid  timeuuid,
        username text,
        body     text,
        PRIMARY KEY(username, tweetid)
    );

    -- Materialized view of tweets created by user, and users she follows
    CREATE TABLE timeline (
        username  text,
        tweetid   timeuuid,
        posted_by text,
        body      text,
        PRIMARY KEY(username, tweetid)
    );
