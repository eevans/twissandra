#!/usr/bin/env python

from cass import add_friends, get_friend_usernames, remove_friend, save_user, save_tweet
from random import randint, choice

USERS = ("stewie", "brian", "meg", "lois", "peter", "chris")
WORDS = [
    ["implement", "utilize", "integrate", "streamline", "optimize", "evolve", "transform", "embrace", "enable", "orchestrate", "leverage", "reinvent", "aggregate", "architect", "enhance", "incentivize", "morph", "empower", "envisioneer", "monetize", "harness", "facilitate", "seize", "disintermediate", "synergize", "strategize", "deploy", "brand", "grow", "target", "syndicate", "synthesize", "deliver", "mesh", "incubate", "engage", "maximize", "benchmark", "expedite", "reintermediate", "whiteboard", "visualize", "repurpose", "innovate", "scale", "unleash", "drive", "extend", "engineer", "revolutionize", "generate", "exploit", "transition", "e-enable", "iterate", "cultivate", "recontextualize"],
    ["clicks-and-mortar", "value-added", "vertical", "proactive", "robust", "revolutionary", "scalable", "leading-edge", "innovative", "intuitive", "strategic", "e-business", "mission-critical", "sticky", "one-to-one", "24/7", "end-to-end", "global", "B2B", "B2C", "granular", "frictionless", "virtual", "viral", "dynamic", "24/365", "best-of-breed", "killer", "magnetic", "bleeding-edge", "web-enabled", "interactive", "dot-com", "sexy", "back-end", "real-time", "efficient", "front-end", "distributed", "seamless", "extensible", "turn-key", "world-class", "open-source", "cross-platform", "cross-media", "synergistic", "bricks-and-clicks", "out-of-the-box", "enterprise", "integrated", "impactful", "wireless", "transparent", "next-generation", "cutting-edge", "user-centric", "visionary", "customized", "ubiquitous", "plug-and-play", "collaborative", "compelling", "holistic"],
    ["synergies", "web-readiness", "paradigms", "markets", "partnerships", "infrastructures", "platforms", "initiatives", "channels", "eyeballs", "communities", "ROI", "solutions", "e-tailers", "e-services", "action-items", "portals", "niches", "technologies", "content", "vortals", "supply-chains", "convergence", "relationships", "architectures", "interfaces", "e-markets", "e-commerce", "systems", "bandwidth", "infomediaries", "models", "mindshare", "deliverables", "users", "schemas", "networks", "applications", "metrics", "e-business", "functionalities", "experiences", "methodologies"]
]


def create_users():
    for user in USERS:
        save_user(user, "qwerty")

def reset_friends():
    for user in USERS:
        # Remove all friends
        for friend in get_friend_usernames(user):
            remove_friend(user, friend)
        # Add some new friends
        for i in range(randint(1, len(USERS)-1)):
            add_friends(user, [choice(USERS)])

def create_tweets():
    for i in range(1, 10000):
        body = "%s %s %s" % (choice(WORDS[0]), choice(WORDS[1]), choice(WORDS[2]))
        save_tweet(choice(USERS), body)

def main():
    create_users()
    reset_friends()
    create_tweets()


if __name__ == '__main__':
    main()
